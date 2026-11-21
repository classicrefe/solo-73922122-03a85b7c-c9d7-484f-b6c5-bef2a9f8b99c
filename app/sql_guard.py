"""Static validation of user-supplied SQL.

Defence in depth:

1. Only a single statement is accepted, and it must start with SELECT/WITH.
2. A keyword/name blacklist rejects statements that could touch files, the
   network, extensions, catalog mutation or instance administration.
3. At execution time every DuckDB connection is additionally created with
   ``enable_external_access=false`` so even a bypass of the static checks
   cannot reach the file system or network.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

import sqlparse

from .config import MAX_SQL_LENGTH

# Statements must begin (after comments/whitespace) with one of these.
_ALLOWED_FIRST_TYPES = {"SELECT", "WITH"}

# Statement types reported by sqlparse that are never acceptable.
_FORBIDDEN_STATEMENT_TYPES = {
    "INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "DROP", "ALTER",
    "TRUNCATE", "REPLACE", "GRANT", "REVOKE", "ATTACH", "DETACH",
    "COPY", "PRAGMA", "TRANSACTION", "UNKNOWN",
}

# Token-level keyword blacklist. Matched on identifier/name tokens as well as
# keyword tokens, so quoted identifiers such as "pragma" are covered.
_FORBIDDEN_KEYWORDS = {
    # schema / catalog mutation
    "CREATE", "TABLE", "VIEW", "INDEX", "SCHEMA", "SEQUENCE", "FUNCTION",
    "MACRO", "TYPE", "DROP", "ALTER", "INSERT", "UPDATE", "DELETE", "MERGE",
    "TRUNCATE", "REPLACE", "ATTACH", "DETACH", "CALL", "PRAGMA",
    "SET", "RESET", "USE", "LOAD", "INSTALL", "COPY", "EXPORT", "IMPORT",
    "GRANT", "REVOKE", "VACUUM", "REINDEX", "CHECKPOINT", "BEGIN",
    "COMMIT", "ROLLBACK", "SAVEPOINT", "TRANSACTION",
    # file / network / process / extension related functions and settings
    "READ_BLOB", "READ_TEXT", "READ_CSV", "READ_CSV_AUTO", "READ_PARQUET",
    "READ_JSON", "READ_JSON_AUTO", "READ_ICEBERG", "READ_NEXUS",
    "WRITE_PARQUET", "GLOB", "LIST_FILES", "FILENAME",
    "HTTP_GET", "HTTP_PUT", "HTTP_POST", "S3_AMZ", "GCS_", "AZURE_",
    "LOAD_EXTENSION", "TRYGUNZIP", "SECRET", "CREATE_SECRET",
    "DBGEN", "SQLITE_MASTER", "SQLITE_SCHEMA", "DUCKDB_TABLES",
    "DUCKDB_VIEWS", "DUCKDB_DATABASES", "DUCKDB_SECRETS",
    "DUCKDB_SETTINGS", "CURRENT_SETTING",
    "TEMP", "TEMPORARY",
}

# Substrings that indicate forbidden function/setting names regardless of
# how sqlparse tokenises them (e.g. read_csv('...') inside an expression).
_FORBIDDEN_PATTERNS = (
    re.compile(r"\bread_(?:blob|text|csv|csv_auto|parquet|json|json_auto|iceberg|nexus)\s*\(", re.I),
    re.compile(r"\bwrite_parquet\s*\(", re.I),
    re.compile(r"\b(?:glob|list_files)\s*\(", re.I),
    re.compile(r"\bhttp_(?:get|put|post|head|delete)\s*\(", re.I),
    re.compile(r"\b(?:load|install)\b[^;]*?\bextension\b", re.I),
    re.compile(r"\bcopy\b[^;]*?\bto\b", re.I),
    re.compile(r"\b(?:attach|detach)\b", re.I),
    re.compile(r"\bpragma\b", re.I),
    re.compile(r"\bcreate\s+(?:or\s+replace\s+)?(?:temp(?:orary)?\s+)?(?:table|view|macro|function|secret|schema|index|sequence|type)\b", re.I),
    re.compile(r"\bdrop\s+(?:table|view|macro|function|secret|schema|index|sequence|type)\b", re.I),
    re.compile(r"\b(?:call|exec|execute)\s*\(", re.I),
    re.compile(r"\bduckdb_(?:tables|views|databases|secrets|settings|temporary_files)\b", re.I),
    re.compile(r"\bcurrent_setting\s*\(", re.I),
    re.compile(r"\bsqlite_(?:master|schema)\b", re.I),
    re.compile(r"information_schema\b", re.I),
    re.compile(r"\bpg_(?:catalog|tables)\b", re.I),
)


class SqlRejected(ValueError):
    """Raised when the submitted SQL fails static validation."""


@dataclass(frozen=True)
class ValidatedSql:
    text: str


def _strip_outer_comments(sql: str) -> str:
    # sqlparse keeps comments as tokens; remove them for structural checks.
    return sqlparse.format(
        sql, strip_comments=True, strip_whitespace=True
    )


def validate_sql(sql: str) -> ValidatedSql:
    """Validate user SQL. Raises :class:`SqlRejected` on any violation."""
    if not isinstance(sql, str):
        raise SqlRejected("SQL must be a string")
    if len(sql) > MAX_SQL_LENGTH:
        raise SqlRejected(f"SQL exceeds maximum length of {MAX_SQL_LENGTH} characters")
    if not sql.strip():
        raise SqlRejected("SQL is empty")

    statements = [s for s in sqlparse.parse(sql) if s.value.strip()]
    if len(statements) != 1:
        raise SqlRejected("exactly one statement is allowed; multiple statements are rejected")

    stmt = statements[0]
    stype = (stmt.get_type() or "").upper()
    if stype in _FORBIDDEN_STATEMENT_TYPES:
        raise SqlRejected(f"statement type {stype} is not allowed")
    if stype not in _ALLOWED_FIRST_TYPES:
        # sqlparse reports plain "SELECT" and "WITH"; anything else is denied.
        first_keyword = _first_keyword(stmt)
        raise SqlRejected(
            f"only SELECT / WITH queries are allowed (found: {first_keyword or stype or 'UNKNOWN'})"
        )

    compact = _strip_outer_comments(sql)
    if not compact:
        raise SqlRejected("SQL contains comments only")

    # Token-level keyword scan (covers quoted identifier forms too).
    upper_identifiers = set()
    for token in stmt.flatten():
        word = token.normalized.upper().strip('"[]`')
        ttype = str(token.ttype)
        if ttype.startswith("Name") or ttype.startswith("Keyword"):
            upper_identifiers.add(word)
    hit = upper_identifiers.intersection(_FORBIDDEN_KEYWORDS)
    if hit:
        raise SqlRejected(f"forbidden keyword/name used: {sorted(hit)[0]}")

    # Pattern scan over the comment-free text.
    for pattern in _FORBIDDEN_PATTERNS:
        if pattern.search(compact):
            raise SqlRejected("forbidden construct detected in SQL")

    # No semicolon-separated trailing content (sqlparse already enforces a
    # single statement, but an empty trailing statement must also fail).
    if compact.rstrip().endswith(";"):
        compact = compact.rstrip()[:-1].rstrip()
    if ";" in compact:
        raise SqlRejected("statement must not contain multiple commands")

    return ValidatedSql(text=sql.strip())


def _first_keyword(stmt) -> str:
    for token in stmt.flatten():
        if token.is_keyword or str(token.ttype).startswith("Name"):
            return token.normalized.upper()
    return ""
