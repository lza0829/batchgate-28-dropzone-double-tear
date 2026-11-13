# 02-batchgate（闸序）

Batchgate — 离线作业依赖编排：作业定义 → 依赖就绪检查 → 执行/失败重试 → 产出水位与对账。

与 Feedstitch（多源 Feed 归并）主链路不同，禁止做成 Feed 解析/冲突/回看换皮。

## 启动

```bash
docker compose up --build
```

| 入口 | 地址 |
| --- | --- |
| 前端 | http://localhost:5200 |
| API | http://localhost:10200 |

## 主链

配置作业与依赖 → 触发一次编排 Run → 按依赖顺序执行夹具作业 → 写入尝试记录与产出水位 → 报告对账

## 落盘触发链

配置落盘目录（`dropzone_dir`，相对 DATA_DIR 或绝对路径）→ 扫描登记触发批次（文件名/发现时间/处置状态）→ 确认批次开一趟编排 → 触发侧与编排侧双重对账。

- 文件只是触发信号：只登记文件名/大小/时间，不读内容、不做业务键归并入库。
- 空文件、扩展名不在 `dropzone_extensions`（默认 `.json`）的文件登记为 `invalid`；可人工 `ignore`。
- 同一文件按 `(目录, 文件名)` 去重，已触发批次重复确认返回 409；确需重触发走显式 `retrigger`，次数 +1 且事件留痕（`trigger_events`）。
- 对账：`GET /api/dropzone/reconcile` 核对 发现数 = 各状态之和、触发次数合计 = dropzone 来源编排数；编排报告带 `reconcile_ok`（成功+失败+跳过 == 作业数 == 尝试数）。
- 页面：「落盘触发」配目录、扫描、看批次、确认/忽略/重触发、查看留痕与编排结果。

## 种子说明

- **ingest_orders**：无依赖，写出订单水位。
- **dim_customer**：无依赖，写出客户维表水位。
- **fact_order_daily**：依赖上两者成功且水位推进后才可执行（底座仅做“上游成功”检查，严格水位闸与重试预算留给 0-1 题）。

## 技术栈

Python 3.12 + FastAPI + SQLite；Vue 3 + Vite + Nginx。

## 0-1 切口（未实现）

`dep_gate` / `retry_budget` / `watermark_reconcile` / `sla_breach`
