"""Unit tests for the static SQL guard."""
import pytest

from app.sql_guard import SqlRejected, validate_sql

ALLOWED = [
    "SELECT 1 AS x",
    "select * from observations where temperature_c > 0 order by ts_utc",
    "WITH hot AS (SELECT * FROM observations WHERE temperature_c > 10) "
    "SELECT count(*) FROM hot",
    "SELECT station_id, AVG(temperature_c) AS avg_t FROM observations GROUP BY station_id",
    "WITH a AS (SELECT * FROM observations), b AS (SELECT * FROM observations) "
    "SELECT a.ts_utc FROM a JOIN b ON a.ts_utc = b.ts_utc",
    "SELECT /* inline comment */ * FROM observations -- trailing\n",
    "WITH ranked AS (SELECT ts_utc, ROW_NUMBER() OVER (ORDER BY temperature_c DESC) rn "
    "FROM observations) SELECT * FROM ranked WHERE rn <= 2;",
]

REJECTED = [
    "",
    "   ",
    "SELECT 1; SELECT 2",
    "SELECT 1;;",
    "INSERT INTO observations VALUES ('x','y',1)",
    "UPDATE observations SET temperature_c = 0",
    "DELETE FROM observations",
    "DROP TABLE observations",
    "CREATE TABLE t (x int)",
    "CREATE VIEW v AS SELECT 1",
    "ALTER TABLE observations ADD COLUMN x int",
    "ATTACH ':memory:' AS m",
    "DETACH DATABASE m",
    "PRAGMA version",
    "COPY (SELECT 1) TO '/tmp/x.csv' (FORMAT csv)",
    "LOAD sqlite",
    "INSTALL spatial",
    "CALL dbgen(sf => 1)",
    "SELECT * FROM read_csv_auto('/etc/passwd')",
    "SELECT read_blob('/etc/hostname')",
    "SELECT * FROM read_parquet('/tmp/x.parquet')",
    "SELECT http_get('http://127.0.0.1:9/')",
    "SELECT * FROM glob('/tmp/*')",
    "SELECT * FROM duckdb_tables()",
    "SELECT current_setting('memory_limit')",
    "SELECT * FROM information_schema.tables",
    "TRUNCATE observations",
    "BEGIN; SELECT 1; COMMIT",
    "SET memory_limit='1GB'",
    'SELECT * FROM "pragma"',
    "SELECT * FROM observations; DROP TABLE observations",
    "CREATE OR REPLACE TEMP VIEW v AS SELECT 1",
    "WITH x AS (SELECT 1) SELECT * FROM x; SELECT 2",
]


@pytest.mark.parametrize("sql", ALLOWED)
def test_allowed_queries_pass(sql):
    result = validate_sql(sql)
    assert result.text.strip()


@pytest.mark.parametrize("sql", REJECTED)
def test_rejected_queries_fail(sql):
    with pytest.raises(SqlRejected):
        validate_sql(sql)


def test_length_limit():
    with pytest.raises(SqlRejected):
        validate_sql("SELECT " + "1," * 50_000)
