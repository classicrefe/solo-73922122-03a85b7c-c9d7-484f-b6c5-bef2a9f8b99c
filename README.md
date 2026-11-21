# 观测数据 SQL 清洗沙箱

面向采样数据工程师的外协清洗 SQL 试跑平台：上传观测 CSV、按站点提交只读
SQL，成功后下载结果 CSV 交给质控。基于 **FastAPI + DuckDB**，全部数据驻留
内存，每轮独立隔离执行。

## 功能与约束（与需求逐条对应）

| 需求 | 实现 |
| --- | --- |
| 上传至多 1 MiB 观测 CSV（站号、UTC时刻、温度） | `POST /api/upload`，超限返回 413；表头支持任意列序，UTF-8 编码 |
| 站点多时上传校验/分站不卡住轮次查询与结果下载 | 上传后立即在事件循环外的**校验线程池**中解析、校验、按站切分；事件循环只负责读 body 与回包，所有 HTTP 接口全程可响应 |
| 按文件进入校验的先后判定新旧 | 上传进入流水线时分配单调递增批次号 `sequence`，与校验完成先后无关 |
| 仅校验通过且比当前批次更新的文件才能换入，站点与行数一并更新 | `DataStore.swap_if_newer` 做 compare-and-swap，单次原子替换整批站点/行数 |
| 较新文件校验失败时，较早的有效文件仍可换入 | 失败文件不发布、不占批次；较早的有效文件完成时仍新于当前批次即可换入 |
| 网页区分换批成功 / 已有更新批次 / 校验失败 | 响应 `outcome` 为 `swapped` / `stale` / `rejected`（400），页面分别绿色/蓝色/红色提示 |
| 已受理的 SQL 继续使用原输入 | 每轮在创建时复制该站点列数据，换批不触及任何在途/已完成轮次 |
| 选站后提交 SQL，每轮输入固定为所选站点数据 | 每轮仅把该站点行装入该轮**私有 DuckDB 连接**的 `observations` 表 |
| 仅允许 SELECT / WITH，支持筛选、连接、聚合 | 静态 SQL 守卫（sqlparse 单语句判定 + 关键字/模式黑名单） |
| 不得读写服务器文件、联网、加载扩展 | 守卫拦截 `read_csv`/`COPY`/`ATTACH`/`LOAD`/`INSTALL`/`http_get` 等；DuckDB 连接再以 `enable_external_access=false` 兜底关闭文件系统、网络与扩展 |
| 不得访问其他轮次数据 | 每轮独立线程 + 独立内存库 + 独立数据副本，其他轮次的连接不可达 |
| 最多两轮并行，满额拒绝 | 注册表实时统计活动轮次，第 3 个请求返回 503 |
| 执行满 5 秒终止 | 看门狗在 5 秒调用 DuckDB 原生 `interrupt()` 立即中止，状态置 `failed` |
| 结果超过 8 MiB 整轮失败 | 流式累计结果字节，超限即失败，已生成内容全部丢弃 |
| 可查询状态、可取消 | `GET /api/rounds/{id}`、`POST /api/rounds/{id}/cancel` |
| 取消受理不等执行结束；实际退出后才报取消并释放名额；迟到结果不开放 | 取消立即返回 `accepted`，监督线程等 worker 真正退出后才置 `cancelled`；取消优先于同时到达的结果 |
| 成功先发布则不可取消 | 已 `succeeded` 的轮次取消返回 409，结果仍可下载 |
| 失败/超时不供部分结果 | 失败轮次结果接口返回 409 |
| 慢查询不挡另一轮、状态查询、已有结果下载 | 每轮独立线程；DuckDB 执行 C++ 期间释放 GIL 真并行；HTTP 接口均为短操作 |

## 目录结构

```
app/
  config.py          限额常量（1 MiB / 5 s / 8 MiB / 2 并行 / 校验线程数）
  csv_io.py          CSV 解析与校验
  upload_pipeline.py 事件循环外的校验/分站流水线、批次号、换批结果（swapped/stale/rejected）
  sql_guard.py       SQL 静态安全校验
  worker.py          每轮查询执行器（独立 DuckDB 连接、interrupt 中止、8MiB 判定）
  registry.py        轮次注册表、名额控制、状态机、看门狗与监督线程
  store.py           内存数据集（按站点切分、sequence 与 swap_if_newer 原子换批）
  main.py            FastAPI 路由
  static/index.html  演示网页（上传/选站/提交/轮询/取消/下载）
sample_data/observations.csv  演示数据（3 个站点 9 行）
tests/             pytest 用例（SQL 守卫、CSV、上传流水线、端到端 API）
Dockerfile, docker-compose.yml
```

## 本地运行（无需 Docker）

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
# 浏览器打开 http://localhost:8000
```

## Docker Compose 运行

```bash
docker compose up --build
# 打开 http://localhost:8000
```

Compose 额外做了加固：只读根文件系统、`no-new-privileges`、丢弃所有 Linux
capability、512 MiB 内存上限、内置健康检查。服务本身不写任何磁盘文件。

## HTTP API 摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/` | 演示网页 |
| GET | `/api/health` | 健康状态、当前活动轮数、当前批次号/文件/站点列表与行数 |
| POST | `/api/upload` | multipart 上传 CSV（≤1 MiB）；校验在后台线程执行，接口本身不阻塞其他请求 |
| POST | `/api/rounds` | 提交 `{station_id, sql}` 创建一轮；满额 503、SQL 非法 400 |
| GET | `/api/rounds` / `/api/rounds/{id}` | 轮次列表 / 单轮状态 |
| POST | `/api/rounds/{id}/cancel` | 请求取消（成功轮返回 409） |
| GET | `/api/rounds/{id}/result` | 下载结果 CSV（仅 succeeded 可用） |

`POST /api/upload` 的判定与响应：

| `outcome` | HTTP | 含义 |
| --- | --- | --- |
| `swapped` | 200 | 本次换批成功：校验通过且批次号新于当前批次，站点与行数已整体更新 |
| `stale` | 200 | 已有更新批次：本文件虽校验通过，但完成时当前批次更新，未换入 |
| `rejected` | 400 | 校验失败：结构化 `detail` 带错误原因，当前批次保持不变 |

轮次状态：`pending` → `running` → `succeeded` / `failed` / `cancelled`。

## 设计说明：上传为什么不阻塞事件循环

旧实现把 `parse_observations_csv()` 与按站切分直接写在 `async def` 上传路由里。
观测站点一多，纯 Python 的解析与切分会长时间占用事件循环线程，期间轮次
轮询、状态查询和已有结果下载全部排队等待。

现在上传路由只在事件循环上读取 multipart 体，随后把任务交给
`UploadPipeline`（有界 `ThreadPoolExecutor`）：

1. 进入流水线即分配递增批次号，新旧只取决于**进入校验的先后**；
2. 解析、校验、按站切分都在 worker 线程完成，事件循环保持空闲；
3. 校验成功后用 `DataStore.swap_if_newer` 原子发布——仅当批次号新于当前批次；
4. 校验失败返回 400 且不触碰当前批次；完成较晚的旧批次得到 `stale` 并丢弃。

轮次在创建时就持有该站点列数据的独立副本，因此任何换批都不会改变已受理
SQL 的输入。

## 测试

```bash
pip install pytest httpx
python -m pytest tests/ -q
```

## 设计说明：为什么用线程 + interrupt 而非子进程

首选方案本为每轮独立子进程，以便从进程边界硬终止。当前构建/运行环境的
seccomp 策略禁止向子进程发送信号（`SIGTERM/SIGKILL` 均返回 EPERM），硬杀
进程不可行。因此改为：每轮一个守护线程持有**独立的内存 DuckDB 连接**，
超时/取消时调用 DuckDB 官方的 `connection.interrupt()`，查询在 C++ 层立即
抛 `InterruptException` 退出（已实测中断延迟 < 10 ms）。轮次间无共享连接、
无共享表，数据隔离不依赖进程边界；DuckDB 执行期间释放 GIL，两个慢查询可
真正并行。
