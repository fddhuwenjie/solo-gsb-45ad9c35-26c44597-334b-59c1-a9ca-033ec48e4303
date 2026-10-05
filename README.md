# 工业传感器二进制帧重同步 API

基于 **Python 3.11 + FastAPI + Pydantic v2 + SQLite** 的服务，用于在可能从任意半帧
开始的原始字节流中重新定位、校验并解码工业传感器二进制帧。

核心能力：

- **协议注册**：声明同步字节、长度字段（偏移 / 宽度 / 端序 / 调整值）、DLE 风格转义
  规则、CRC-16 参数（内置常用预设，也可自定义 poly/init/refin/refout/xorout），以及
  带符号与定点缩放（`scale` / `divisor`，支持位域与 min/max）的字段布局。
- **重同步解码** `POST /decode`：提交 base64 字节流，服务按同步字节逐个扫描候选帧；
  长度越界、转义非法、CRC 失败的候选被跳过并从下一个同步字节重试；选择**最早结束且
  校验通过**的完整帧；两个不同起点的合法帧若在同一位置结束则判定为协议歧义，返回
  结构化 422。
- **编码下发** `POST /encode`：按已注册协议把字段物理值（`Decimal` 精确反算原始整数）
  组装成可直接下发的二进制帧：写入位域、补 0 未写比特、派生长度值与 CRC、执行 DLE
  转义并添加同步字节；结果在返回前强制经现有解码器回读校验，字段不一致直接拒绝返回。
- **不完整尾部保留**：流末尾尚未凑齐的帧（含 dangling 转义符、只出现一半的同步头）
  原样保留在 `tail` 中返回，可直接与后续到达的字节拼接后再次提交。
- **采集会话（跨分片流式解码）**：`POST /sessions` 创建会话并**固定协议确切版本**，
  随后用 `POST /sessions/{id}/chunks` 分批追加 base64 分片；服务自动把上次未决的
  原始尾部接到新分片前再解码，每次只返回**本次新确定**的帧与丢弃区间（起止偏移统一为
  会话累计字节位置），并返回新的待决尾部与下一偏移。接收偏移、尾部与响应一起持久化到
  SQLite，服务重启后可继续；最近一次分片按同一偏移+内容重发返回原响应，偏移相同但内容
  不同或乱序提交返回 409 且不改状态。`GET /sessions/{id}` 查询进度。
- **丢弃区间审计**：每次跳过都返回 `[start, end)` 区间与各候选的失败原因
  （`length_out_of_bounds` / `crc_failed` / `malformed_escape`）。
- **结果复用**：同一协议版本 + 输入字节 SHA-256 摘要命中 SQLite 缓存，响应中
  `cached=true`。
- **结构化错误**：字段越界、协议歧义、非法协议声明均返回统一错误信封的 HTTP 422。

## 一条命令启动

```bash
docker compose up --build
```

容器把宿主机 **8000 端口**映射到容器内 8000（compose 中的 `ports: ["8000:8000"]`，
镜像 `EXPOSE 8000`，uvicorn 监听 `0.0.0.0:8000`）。服务监听
<http://localhost:8000>，交互式文档在 <http://localhost:8000/docs>。
若 8000 被占用，把 compose 映射改为 `"8001:8000"` 即可从 <http://localhost:8001> 访问。
SQLite 数据文件保存在命名卷 `resync-data`（容器内 `/data/resync.db`）。

本地（无 Docker）启动：

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

环境变量：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `RESYNC_DB_PATH` | `/data/resync.db` | SQLite 文件路径 |
| `RESYNC_MAX_DECODE_BYTES` | `1048576` | 单次解码原始字节上限（超出返回 413） |

## 快速示例

### 1. 注册协议

```bash
curl -s -X POST localhost:8000/protocols -H 'Content-Type: application/json' -d '{
  "protocol_id": "sensor-line",
  "version": 1,
  "sync": [170, 85],
  "endian": "big",
  "length_field": {"offset": 4, "size": 1},
  "crc": {"preset": "CRC-16/CCITT-FALSE", "offset": 6},
  "fields": [
    {"name": "seq",  "type": "u8",  "offset": 0},
    {"name": "addr", "type": "u8",  "offset": 1},
    {"name": "temp", "type": "s16", "offset": 2, "scale": "0.01"}
  ],
  "sequence_field": "seq",
  "max_frame_length": 64
}'
```

帧的逻辑视图（同步字节之后）：

```
offset  0    1    2    3    4      5    6    7    8
        seq  addr temp_hi temp_lo len  st   crc_hi crc_lo x
└──────────── CRC 覆盖（offset 6..7 之外的全部逻辑字节）────────┘
```

内置 CRC 预设：`CRC-16/CCITT-FALSE`、`CRC-16/XMODEM`、`CRC-16/X25`、
`CRC-16/MODBUS`、`CRC-16/IBM-SDLC`、`CRC-16/ARC`、`CRC-16/USB`、`CRC-16/DNP`；
也可以省略 `preset`，直接给 `poly/init/refin/refout/xorout`。

### 2. 解码（可从半帧开始）

```bash
curl -s -X POST localhost:8000/decode -H 'Content-Type: application/json' -d '{
  "protocol_id": "sensor-line",
  "payload_b64": "AZm...（base64，可包含噪声、坏帧、半帧尾部）"
}'
```

响应示例：

```json
{
  "protocol_id": "sensor-line",
  "version": 1,
  "input_digest": "b4f07670…",
  "frames": [
    {
      "start": 2,
      "end": 13,
      "logical_length": 9,
      "sequence": 10,
      "crc_ok": true,
      "fields": [
        {"name": "seq",  "offset": 0, "size": 1, "raw": 10,   "value": 10,    "signed": false},
        {"name": "addr", "offset": 1, "size": 1, "raw": 3,    "value": 3,     "signed": false},
        {"name": "temp", "offset": 2, "size": 2, "raw": 2345, "value": 23.45, "signed": true}
      ]
    }
  ],
  "discarded": [
    {"start": 0, "end": 2, "reason": "noise", "failed_attempts": []}
  ],
  "tail": {"start": 13, "end": 18, "payload_b64": "qlUKAAk="},
  "frame_count": 1,
  "consumed": 13,
  "cached": false
}
```

- `start` / `end`：帧在**原始字节流**中的半开区间 `[start, end)`（端到端，含同步字节）。
- `discarded`：被放弃的字节区间；`reason` 为 `noise`（其间没有可识别同步头）或
  `resync_failed_checks`，后者的 `failed_attempts` 给出每个候选同步点的失败细节。
- `tail`：流尾部无法确认成帧的字节，base64 原样回传，收到新数据后拼接重试即可。
- 再次提交完全相同的字节流时 `cached=true`。

### 3. 编码一帧（可直接下发）

`POST /encode` 按已注册协议反向组装一帧。请求给出 `protocol_id`、`version`（可省略，
取最新版本）、`logical_length`（逻辑体长度，即同步字节之后、转义之前的字节数）和
`fields`（字段名 → 物理值）。字段值建议用字符串给出精确小数（裸 JSON 数字也会按
`Decimal` 解析，不会经过二进制浮点）：

```bash
curl -s -X POST localhost:8000/encode -H 'Content-Type: application/json' -d '{
  "protocol_id": "sensor-line",
  "version": 1,
  "logical_length": 9,
  "fields": {
    "seq": 1,
    "addr": 3,
    "temp": "23.45",
    "state": 7,
    "alarm": 1
  }
}'
```

响应示例：

```json
{
  "protocol_id": "sensor-line",
  "version": 1,
  "wire_b64": "qlUBAwkJ…",
  "logical_b64": "AQMJCSVl…",
  "logical_length": 9,
  "wire_length": 11,
  "length_value": 9,
  "crc": "65A5",
  "fields": [
    {"name": "seq",  "offset": 0, "size": 1, "value": "1",     "raw": 1,    "wire": 1,    "bit_offset": null, "bit_width": null, "signed": false},
    {"name": "addr", "offset": 1, "size": 1, "value": "3",     "raw": 3,    "wire": 3,    "bit_offset": null, "bit_width": null, "signed": false},
    {"name": "temp", "offset": 2, "size": 2, "value": "23.45", "raw": 2345, "wire": 2345, "bit_offset": null, "bit_width": null, "signed": true},
    {"name": "state", "offset": 5, "size": 1, "value": "7",    "raw": 7,    "wire": 7,    "bit_offset": 0, "bit_width": 3, "signed": false},
    {"name": "alarm", "offset": 5, "size": 1, "value": "1",    "raw": 1,    "wire": 1,    "bit_offset": 3, "bit_width": 1, "signed": false}
  ]
}
```

- `wire_b64`：`同步字节 || 转义后的逻辑体`，base64 解码后即可直接下发。
- `logical_b64`：同步字节之后、未转义的逻辑体；未被任何字段写入的比特保证为 0。
- `length_value` / `crc`：实际写入长度字段的原始值（`logical_length - adjustment`）
  与实际计算并写入的 CRC-16（大写十六进制）。
- `logical_length` / `wire_length`：实际逻辑长度与线上长度（开启转义时后者更大）。
- `fields[].raw` 为按 `value × divisor ÷ scale` 反算的有符号原始整数；
  `fields[].wire` 为放到线上的无符号值（有符号字段为二补码，位域为位域内整数值）。

编码规则与校验：

- 服务用 `Decimal` 执行 `raw = value × divisor ÷ scale`，结果必须是**精确整数**且
  满足字段位宽（含位域宽度）与 `min`/`max`，否则 422。
- 多个字段覆盖同一比特时，只允许写入相同的值；冲突返回
  `OVERLAPPING_WRITE_CONFLICT` 422（details 给出字节/比特与冲突字段）。
- 字段不得占用长度字段或 CRC 的派生位置（`DERIVED_POSITION_CONFLICT`）。
- 字段全部写入后才写长度值与 CRC，随后执行转义、添加同步字节。
- 返回前强制把 `wire_b64` 送入现有 `/decode` 解码器回读；帧数不为 1、尾部非空或任一
  字段值与请求不一致时返回 `ENCODE_VERIFICATION_FAILED` 422，不下发该帧。

### 4. 采集会话：跨分片流式解码（分批到达的字节流）

现场字节流分批到达时，用会话把多次分片拼成一条连续流：服务为每个会话保存**累计接收
偏移**与**待决尾部**（上次没凑齐的原始字节），新分片到达时自动把尾部拼在新数据前，
再用同一套重同步解码器解码；每次响应只携带**本次新确定**的帧与丢弃区间，
`start`/`end` 全部为会话级累计偏移。调用方只需发送新到达的字节，无需自己拼接。

#### 4.1 创建会话（固定协议确切版本）

```bash
curl -s -X POST localhost:8000/sessions -H 'Content-Type: application/json' -d '{
  "protocol_id": "sensor-line",
  "version": 1
}'
```

`version` 必填：会话把该确切版本钉死，之后注册新版本不影响本会话。响应（201）：

```json
{
  "session_id": "fb63f6becc99488fb89eeccc8fe1581b",
  "protocol_id": "sensor-line",
  "version": 1,
  "next_offset": 0,
  "frames_total": 0,
  "tail": {"start": 0, "end": 0, "payload_b64": ""},
  "created_at": "2026-10-05 16:22:19",
  "updated_at": "2026-10-05 16:22:19"
}
```

#### 4.2 追加分片（自动拼接上次尾部）

`POST /sessions/{session_id}/chunks`，请求体提交**期望接收偏移** `offset`（首片为 0，
之后等于上一响应的 `next_offset`）和**非空** base64 分片 `payload_b64`。下面把两帧
（线上共 22 字节）切成 16 + 6 两片，第二帧横跨分片边界：

```bash
SID=fb63f6becc99488fb89eeccc8fe1581b

# 分片 1：完整帧 1 + 帧 2 的前 5 字节
curl -s -X POST localhost:8000/sessions/$SID/chunks -H 'Content-Type: application/json' -d '{
  "offset": 0,
  "payload_b64": "qlUBAwkpCQB1mwCqVQID/w=="
}'
```

响应：帧 1 完成（`"start": 0, "end": 11`），帧 2 的前 5 字节留在新尾部：

```json
{
  "session_id": "fb63f6becc99488fb89eeccc8fe1581b",
  "protocol_id": "sensor-line",
  "version": 1,
  "frames": [
    {"start": 0, "end": 11, "logical_length": 9, "sequence": 1, "crc_ok": true,
     "fields": [{"name": "temp", "offset": 2, "size": 2, "raw": 2345, "value": 23.45, "signed": true}]}
  ],
  "discarded": [],
  "tail": {"start": 11, "end": 16, "payload_b64": "qlUCA/8="},
  "frame_count": 1,
  "chunk_bytes": 16,
  "next_offset": 16
}
```

```bash
# 分片 2：帧 2 的剩余 6 字节（服务自动把上面的 tail 拼在前面再解码）
curl -s -X POST localhost:8000/sessions/$SID/chunks -H 'Content-Type: application/json' -d '{
  "offset": 16,
  "payload_b64": "agkAqooA"
}'
```

响应：帧 2 在会话绝对偏移 `[11, 22)` 处解出，尾部清空：

```json
{
  "frames": [
    {"start": 11, "end": 22, "logical_length": 9, "sequence": 2, "crc_ok": true,
     "fields": [{"name": "temp", "offset": 2, "size": 2, "raw": 65386, "value": -1.5, "signed": true}]}
  ],
  "discarded": [],
  "tail": {"start": 22, "end": 22, "payload_b64": ""},
  "frame_count": 1,
  "chunk_bytes": 6,
  "next_offset": 22
}
```

#### 4.3 重试与乱序（幂等）

- **重发最近一次分片**（同一 `offset` + 同一内容）：返回原响应，不重复产出、不推进
  状态——网络重试是安全的。上例中把分片 2 原样再发一次，返回与 4.2 完全相同的响应。
- **同一偏移、不同内容**：`409 SESSION_OFFSET_CONFLICT`，状态不变：

  ```bash
  curl -s -X POST localhost:8000/sessions/$SID/chunks -H 'Content-Type: application/json' -d '{
    "offset": 16,
    "payload_b64": "AAAAAAAA"
  }'
  # {"error": {"code": "SESSION_OFFSET_CONFLICT", "message": "offset 16 was already received ...", ...}}
  ```

- **乱序/并发提交**（`offset` 不等于会话当前 `next_offset`，或并发竞争落败）：
  同样 `409 SESSION_OFFSET_CONFLICT` 且不改状态；并发下同一偏移只有两个相同内容的
  请求会都拿到原响应，不同内容只有一个成功。
- **不推进偏移的失败**：拼接后（尾部 + 新分片）超过 `RESYNC_MAX_DECODE_BYTES` 返回
  413；解码报错（如 `AMBIGUOUS_FRAME`、`FIELD_OUT_OF_BOUNDS`）返回 422。两者都不改
  会话状态，修正后可按原偏移重发。

#### 4.4 进度查询与重启续传

```bash
curl -s localhost:8000/sessions/$SID
```

```json
{
  "session_id": "fb63f6becc99488fb89eeccc8fe1581b",
  "protocol_id": "sensor-line",
  "version": 1,
  "next_offset": 22,
  "frames_total": 2,
  "tail": {"start": 22, "end": 22, "payload_b64": ""},
  "created_at": "2026-10-05 16:22:19",
  "updated_at": "2026-10-05 16:22:19"
}
```

接收偏移、待决尾部与最近一次响应**一起持久化**在 SQLite（compose 的 `resync-data`
卷）中：服务重启后会话可继续追加，最近一次分片的重发在重启后仍返回原响应。

## 协议声明字段

| 字段 | 说明 |
| --- | --- |
| `sync` | 1–8 个同步字节（如 `[0xAA,0x55]`） |
| `endian` | 协议默认端序 `big` / `little`；长度字段、CRC、各字段可单独覆盖 |
| `length_field` | `offset`（相对逻辑体）、`size`（1/2/4）、`endian`、`adjustment`。线上长度值 + adjustment = 逻辑帧长；当长度字段把帧头也计数时用负数（如 `-4`） |
| `escaping` | DLE 转义：`escape_byte`、`xor_mask`（转义后字节 = 原字节 XOR mask）、`escapes_sync`、额外受保护字节 `escaped_bytes` |
| `crc` | CRC-16 参数与 `offset`（2 字节 CRC 在逻辑体中的偏移）、`endian`、`verify` |
| `fields[]` | `type` 为 `u8/u16/u24/u32/u64/s8/s16/s24/s32/s64`；定点值 = 整数 × `scale` ÷ `divisor`；可选 `bit_offset`+`bit_width` 取位域，`min`/`max` 做范围校验 |
| `sequence_field` | 指定作为帧序号的字段名（响应中的 `sequence`） |
| `min/max_frame_length` | 逻辑帧长上下界；越界候选触发重同步 |

### 转义语义

转义只作用于同步头之间。开启后，同步字节、转义字节（及 `escaped_bytes` 声明的字节）
在链路上以 `escape_byte || (byte XOR xor_mask)` 两个字节出现；解码时实时反转义，长度
字段与所有字段偏移都按**逻辑（反转义后）空间**解释。缓冲区恰好结束在转义引导字节处时
视为不完整帧，进入 `tail`。

## 错误信封

所有错误统一为：

```json
{"error": {"code": "FIELD_OUT_OF_BOUNDS", "message": "…", "details": { … }}}
```

| HTTP | code | 触发场景 |
| --- | --- | --- |
| 422 | `VALIDATION_ERROR` / `INVALID_PROTOCOL` | 请求体或协议声明不合法（附字段级 details） |
| 422 | `INVALID_BASE64` | `payload_b64` 不是合法 base64 |
| 422 | `FIELD_OUT_OF_BOUNDS` | 字段超出帧边界，或缩放后值超出 `min/max` |
| 422 | `AMBIGUOUS_FRAME` | 多个不同起点的合法帧结束于同一位置 |
| 422 | `VALUE_NOT_REPRESENTABLE` | `/encode`：反算结果不是精确整数（或值非有限值、scale 为 0） |
| 422 | `UNKNOWN_FIELD` | `/encode`：请求的字段名不在协议声明中 |
| 422 | `OVERLAPPING_WRITE_CONFLICT` | `/encode`：字段覆盖同一比特但写入不同值 |
| 422 | `DERIVED_POSITION_CONFLICT` | `/encode`：字段占用长度字段或 CRC 的字节位置 |
| 422 | `LENGTH_NOT_ENCODABLE` | `/encode`：长度值超出长度字段位宽 |
| 422 | `ENCODE_VERIFICATION_FAILED` | `/encode`：解码器回读结果与请求不一致（帧拒绝返回） |
| 404 | `PROTOCOL_NOT_FOUND` | 协议 ID / 版本不存在 |
| 404 | `SESSION_NOT_FOUND` | 会话 ID 不存在 |
| 409 | `PROTOCOL_VERSION_CONFLICT` | 同 ID+版本已存在但定义不同 |
| 409 | `SESSION_OFFSET_CONFLICT` | 分片偏移与内容冲突：同偏移不同内容、乱序或并发竞争落败（状态不变） |
| 413 | `PAYLOAD_TOO_LARGE` | 超 `RESYNC_MAX_DECODE_BYTES`（会话为尾部+新分片拼接后超限，不推进偏移） |

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/health` | 健康检查 |
| `POST` | `/protocols` | 注册（或幂等覆盖）协议版本 |
| `GET` | `/protocols` | 列出已注册协议及定义摘要 |
| `GET` | `/protocols/{id}?version=N` | 读取协议定义（省略版本取最新） |
| `POST` | `/decode` | 解码 / 重同步 |
| `POST` | `/encode` | 按已注册协议编码一帧（长度/CRC/转义/同步自动生成，回读校验） |
| `POST` | `/sessions` | 创建采集会话，固定协议确切版本 |
| `GET` | `/sessions/{id}` | 查询会话进度（累计偏移、待决尾部、累计帧数） |
| `POST` | `/sessions/{id}/chunks` | 追加非空 base64 分片（自动拼接上次尾部，幂等重发） |

同 ID + 版本重复提交**完全相同**的定义是幂等的（返回 `replaced=true`）；定义不同则 409。

## 项目结构

```
app/
  config.py    # 环境配置
  models.py    # Pydantic 协议/请求/响应模型与协议自检
  crc.py       # 通用 CRC-16（反射/非反射）
  decoder.py   # 候选扫描、反转义、长度/CRC 校验、重同步与歧义判定
  encoder.py   # 字段写入（位域/定点反算/冲突检测）、长度与 CRC 派生、转义、回读校验
  db.py        # SQLite：协议表 + (协议版本, 输入摘要) 结果缓存 + 采集会话（偏移/尾部/最近响应）
  errors.py    # 结构化错误类型
  main.py      # FastAPI 路由与错误处理
tests/         # pytest 用例（含构造 CRC 碰撞的歧义用例与编码回读校验）
Dockerfile
docker-compose.yml
```

## 测试

```bash
pip install -r requirements.txt pytest httpx
pytest
```

覆盖：半帧起始 + 噪声 + 多帧 + 不完整尾部、坏 CRC / 长度越界后重同步、DLE 转义
（含同步字节与转义字节被转义）、little-endian + CRC-16/MODBUS + 长度 adjustment、
位域与有符号定点缩放、字段越界 422、CRC 碰撞歧义 422、缓存复用、版本冲突与解析；
编码侧覆盖：定点精确反算、正负有符号值、位域共享字节与同值重叠、转义帧往返、
LE/adjustment 长度值、不可精确表示 / min/max / 位宽 / 派生位置 / 长度溢出 422，
以及解码器回读不一致拒绝返回；会话侧覆盖：跨分片拼帧与一次性解码结果一致、
绝对偏移、尾部结转、最近一次分片重发幂等、同偏移不同内容 / 乱序 / 并发竞争 409、
拼接超限 413 与解码 422 不推进偏移、进度查询、版本钉死与重启续传。
