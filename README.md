# 渡轮结算 · Iceberg v2 票号删除核对

用于核对湖仓源快照中，某航次日分区里的票号是否已被 **Iceberg v2 位置删除**
（positional delete）标记。网页接收有效文件清单和对应 Parquet 副本，输入整数
票号，逐条给出原文件、从 0 起算的文件内行号、是否已删除以及命中的删除文件。

## 快速启动

```bash
docker compose up --build
```

打开 <http://localhost:8000>，点 **“载入样例”** 即可运行内置演示。接口健康检查：

```bash
curl http://localhost:8000/healthz
```

容器通过 Docker Compose 的 `mem_limit` / `memswap_limit` 硬性限制在
**256 MiB**。位置删除文件按固定大小的批（默认 4096 行，可用环境变量
`DELETE_BATCH_ROWS` 调整）流式读取，峰值内存与文件总行数无关。

停止：

```bash
docker compose down
```

## 网页用法

1. 上传 **有效文件清单**（CSV 或 JSON）。
2. 上传清单中所有 `data` / `position_delete` 文件的 **Parquet 副本**，可多选。
   文件名必须与清单的 `uploaded_filename` 一致。
3. 填写整数票号（逗号、空格、分号或换行分隔），以及票号列名（默认
   `ticket_no`）。
4. 开始核对。

上传文件只保存在一次请求的临时目录中，请求结束后删除；不会按用户文件名
拼接服务器路径。

### 清单格式

CSV（推荐）：

```csv
original_path,uploaded_filename,file_type,data_sequence_number
s3://ferry-lake/warehouse/settlement/voyage_date=2026-11-05/data/data-0001.parquet,data-0001.parquet,data,7
s3://ferry-lake/warehouse/settlement/voyage_date=2026-11-05/delete-pos/delete-pos-0001.parquet,delete-pos-0001.parquet,position_delete,12
```

JSON 也支持，顶层可以是数组，或包含 `files` / `entries` 数组的对象：

```json
[
  {
    "original_path": "s3://.../data/data-0001.parquet",
    "uploaded_filename": "data-0001.parquet",
    "file_type": "data",
    "data_sequence_number": 7
  }
]
```

列名可使用以下别名：

| 含义 | 可用列名 |
| --- | --- |
| 原始路径 | `original_path`, `original_file_path`, `source_path`, `path` |
| 上传文件名 | `uploaded_filename`, `uploaded_file`, `filename` |
| 文件类型 | `file_type`, `content`, `type` |
| 数据序列号 | `data_sequence_number`, `sequence_number`, `seq` |

`file_type` 可选值：

| 值 | 行为 |
| --- | --- |
| `data` / `0` | 数据文件，参与扫描 |
| `position_delete` / `positional_delete` / `1` / `delete` | 位置删除文件，参与匹配 |
| `equality_delete` / `2` | 等值删除，**忽略**；不要求上传 Parquet 副本 |

只处理位置删除。位置删除 Parquet 必须包含 `file_path`（字符串）和 `pos`
（整数）两列，其他列（例如 Iceberg 可选的 `row`）会被忽略。

## 核对规则

- **行组跳读**：对每个数据文件的每个行组读取 `ticket_no` 列的 min/max 统计。
  票号落在统计范围外时跳过该行组；统计缺失（`statistics` 为 `None` 或没有
  min/max）时读取该行组。统计范围包含票号时也读取。
- **原始行号**：行号始终是数据文件中从 0 起算的原始行号 =
  前面所有行组的行数之和 + 行组内原始下标。筛选或只读取某个行组都不会重置、
  压缩或重新编号。
- **位置删除匹配**：删除记录必须同时满足
  1. 删除记录的 `file_path` 精确等于清单中的 `original_path`；
  2. `pos` 精确等于该命中行的从 0 起算的行号；
  3. 删除文件的数据序列号 **不小于** 数据文件的数据序列号。

  不满足序列号条件的候选删除文件会显示为“被规则忽略的删除记录”，但不会
  把票判成已删除。
- **流式删除扫描**：位置删除文件按行组、再按批（`DELETE_BATCH_ROWS`，
  默认 4096 行）读取。即使 200 万条删除记录全部位于同一个行组，也不会把
  整份文件或整个行组读入内存。扫描时逐条校验，只有 `(file_path, pos)`
  正好命中本次所查票号数据行的记录才会被保留；其余无关记录随即丢弃。
  删除记录的原始行号按“前面所有行组行数之和 + 批内下标”计算，流式读取
  不会重置或压缩行号。
- **全有或全无**：清单中的任何一个 `data` / `position_delete` 副本缺失，
  或任一文件打不开、行组读取失败，或任一位置删除记录无效（`file_path`
  为空/非字符串、`pos` 缺失/非整数/为负），整次核对都会中止并明确列出
  文件名和原始行号，不显示任何票号的部分结论——即使已经找到删票依据。
  未出现在清单中的上传文件会被忽略并给出警告。

## 内置样例

`sample/` 中的演示数据由 `tools/make_sample.py` 生成：

- `data-0001.parquet`：5 个行组，每个 20 行。
  - 行组 0–3 的票号范围分别是 1000–1019、2000–2019、3000–3019、4000–4019。
  - 票号 **4242** 在第 5 个行组（索引 4），全局行号 **99**。
  - 核对 4242 时，前 4 个行组全部被统计范围排除并跳过；读取第 5 个行组后
    仍按原始行号 99 命中 `delete-pos-0001.parquet`。
- `data-0002.parquet`：写入时关闭了列统计，票号 **8888** 在第 2 个行组
  （全局行号 **19**）。因为没有统计，两个行组都会被读取，随后命中
  `delete-pos-0002.parquet`。
- `delete-pos-stale-0001.parquet`：也指向 4242 / 行 99，但数据序列号是 6，
  小于数据文件的 7，因此被忽略。
- 清单中的 `equality_delete` 项没有上传副本，核对仍正常完成，因为它被忽略。

样例默认票号：`4242, 1003, 4218, 8888, 9999`。其中 1003 已删除，4218
存在但未删除，9999 不存在。

## 大行组样例

`tools/make_large_sample.py` 专门生成“单个行组内 200 万条位置删除记录”的
演示数据，用来验证流式扫描：

```bash
python3 tools/make_large_sample.py                 # 输出到 sample-large/
python3 tools/make_large_sample.py --rows 5000000  # 自定义行数
```

生成目录中的 `manifest.csv` 和三个 `.parquet` 文件可以直接从核查页上传；
默认核对票号 `4242, 1003` 即可看到：

- 两条票都命中位于 200 万行行组末尾的删除记录，并显示删除文件中的原始
  行号（分别为 1999997 和 1999996，具体随 `--rows` 变化）；
- `delete-pos-stale-0001.parquet` 中指向 4242 / 行 99 的记录因序列号 6
  小于数据文件的 7 而显示为“被规则忽略”；
- 其余 199 万多条无关记录只在批内校验后丢弃，不会进入结果或长期占用内存。

## 本地开发与测试

需要 Python 3.11+、Flask、PyArrow。

```bash
python3 -m pip install -r requirements.txt
python3 app.py                     # http://localhost:8000
python3 -m pytest -q               # 40 个测试
```

重新生成样例（会覆盖 `sample/` 下的 Parquet 和清单）：

```bash
python3 tools/make_sample.py
```

## 目录

```
app.py                     Flask 路由、上传处理、票号解析
checker/manifest.py        清单解析与校验
checker/engine.py          PyArrow 行组跳读、行号计算、位置删除匹配
checker/errors.py          CheckError（始终带文件名）
templates/index.html       网页
static/style.css           样式
sample/                    内置样例（清单 + Parquet）
tools/make_sample.py       内置样例生成脚本
tools/make_large_sample.py 单行组 200 万条位置删除样例生成脚本
tests/                     pytest 测试
Dockerfile                 容器镜像
docker-compose.yml         启动配置
```

## 已知边界

- 位置删除文件不会被整体读入内存；扫描仍会遍历全部行以校验无效记录，因此
  核对耗时随删除文件行数增长，但内存占用只随批大小和命中记录数增长。
- 每个票号独立扫描数据文件。一次输入大量票号时耗时随票号数线性增长。
- 路径按字符串精确比较，不解析 `s3://`、`file://` 等 URI 等价关系。
  请保证清单的 `original_path` 与删除记录中的 `file_path` 写法完全一致。
