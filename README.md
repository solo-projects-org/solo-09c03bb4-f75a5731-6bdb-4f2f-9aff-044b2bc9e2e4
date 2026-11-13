# Order Book Collector（盘口采集服务）

面向流动性分析的单交易对盘口采集服务。采集端通过 **Binance Spot 兼容** 的
HTTP 深度快照 + WebSocket 增量流拼接实时盘口；只有确认衔接成功后才对内提供
查询，断流期间绝不沿用旧盘口。

## 组成

| 服务 | 端口 | 说明 |
|---|---|---|
| `collector` | 8000 | 采集服务（FastAPI），Swagger: <http://localhost:8000/docs> |
| `mock-source` | 9000 | 本地模拟源（FastAPI），可注入快照延迟 / 序号缺口 / 断连 / 断流，Swagger: <http://localhost:9000/docs> |

## 快速开始

```bash
docker compose up --build
```

然后在 Swagger 上操作（<http://localhost:8000/docs>）：

1. `POST /collection/start`，body `{"symbol": "BTCUSDT"}` —— 启动采集；
2. `GET /status` —— 查看状态（`idle` / `syncing` / `live`）与诊断计数；
3. `GET /orderbook` —— 仅在 `live` 时返回买卖各前五档和末序号，否则 `409`。

盘口响应示例（价格、数量均为精确十进制字符串）：

```json
{
  "symbol": "BTCUSDT",
  "state": "live",
  "lastUpdateId": 1033,
  "bids": [["66999.90", "4.72250"], ...],
  "asks": [["67000.30", "3.86056"], ...]
}
```

## 同步规则（与 Binance Spot 一致）

- 快照带 `lastUpdateId`；增量带首末序号 `U`、`u`，价格档数量为**绝对数量**，
  零量删档。
- `u <= 当前序号`：忽略；否则必须满足 `U <= 当前序号 + 1 <= u` 才能接续，
  否则判定序号缺口。
- 取快照期间照常接收并缓存增量；丢弃 `u <= lastUpdateId` 的事件，接上覆盖
  `lastUpdateId + 1` 的首个增量后才进入 `live`、开放查询。
- **断连、序号缺口、流静默（默认 3s 无数据）→ 立即回到 `syncing` 并清空
  盘口**，停止提供查询（`GET /orderbook` 返回 409，`GET /status` 显示
  `syncing`），重新衔接后自动恢复。
- 每次连接有独立的代次（generation）；**旧连接的迟到快照会被丢弃**（见
  `status.counters.stale_snapshots`），不能恢复查询。
- 单次查询在同一把锁内读取状态、末序号与档位，保证一次查询只读同一状态。

## 故障演练（模拟源）

通过 <http://localhost:9000/docs> 或 curl 注入故障：

```bash
# 快照延迟 4 秒（之后的每个 /depth 请求都生效）
curl -X POST http://localhost:9000/control/snapshot-delay -H 'Content-Type: application/json' -d '{"ms": 4000}'

# 序号缺口：跳过 5 个更新序号
curl -X POST http://localhost:9000/control/gap -H 'Content-Type: application/json' -d '{"updates": 5}'

# 断开所有 WebSocket 连接
curl -X POST http://localhost:9000/control/disconnect

# 断流 6 秒（连接不断，但不发任何数据）
curl -X POST http://localhost:9000/control/pause -H 'Content-Type: application/json' -d '{"seconds": 6}'

# 查看模拟源内部状态
curl http://localhost:9000/market/state
```

注入后观察 `GET /status`：状态应立即变为 `syncing`，`counters` 中对应的
`gaps` / `reconnects` / `stale_timeouts` / `stale_snapshots` 递增，恢复后
回到 `live`。

典型组合场景——快照延迟 + 断连，可验证"旧连接的迟到快照不能恢复查询"：

```bash
curl -X POST http://localhost:9000/control/snapshot-delay -d '{"ms": 4000}' -H 'Content-Type: application/json'
curl -X POST http://localhost:8000/collection/start -d '{"symbol": "BTCUSDT"}' -H 'Content-Type: application/json'
sleep 1 && curl -X POST http://localhost:9000/control/disconnect
# 旧快照约 4 秒后迟到到达：状态保持 syncing，stale_snapshots +1；
# 新连接的快照到达并衔接后才恢复 live。
```

## 配置（collector 环境变量）

| 变量 | 默认 | 说明 |
|---|---|---|
| `SOURCE_HTTP_BASE` | `http://localhost:9000` | 快照源 HTTP 地址 |
| `SOURCE_WS_BASE` | `ws://localhost:9000` | 增量流 WS 地址 |
| `BOOK_LIMIT` | `10` | 快照档数（源每侧最多十档） |
| `SERVE_DEPTH` | `5` | 查询返回的档数 |
| `RECONNECT_DELAY_SECONDS` | `1.0` | 重连间隔 |
| `STALE_TIMEOUT_SECONDS` | `3.0` | 无任何数据的判死时长 |
| `SNAPSHOT_HTTP_TIMEOUT_SECONDS` | `60.0` | 快照请求超时 |

## 本地开发与测试

```bash
pip install -r collector/requirements.txt -r mock_source/requirements.txt pytest

# 单元测试（合并规则、迟到快照防护）
python3 -m pytest tests/test_orderbook.py tests/test_collector_unit.py -q

# 端到端测试（需先启动两个服务）
(cd mock_source && uvicorn app.main:app --port 9000 &)
(cd collector && uvicorn app.main:app --port 8000 &)
python3 tests/integration_test.py
```

集成测试覆盖：启动衔接、缺口重同步、断连重连、断流判死、快照延迟期间
不供数、旧连接迟到快照被丢弃、停止后盘口不可用。

## 代码结构

```
collector/app/
  book.py        # 订单簿：Decimal 精确十进制，快照加载 + 增量衔接规则
  collector.py   # 状态机 idle/syncing/live，快照-增量拼接，代次防护
  config.py      # 环境变量配置
  main.py        # FastAPI：/collection/start|stop, /status, /orderbook
mock_source/app/
  market.py      # 合成盘口引擎（每侧≤10 档），混沌注入
  main.py        # FastAPI：/depth, /ws/<symbol>@depth, /control/*
tests/
  test_orderbook.py      # 合并规则单元测试
  test_collector_unit.py # 迟到快照代次防护单元测试
  integration_test.py    # 端到端故障演练测试
```
