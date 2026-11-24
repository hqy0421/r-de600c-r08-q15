# 仪表数据按时间排序的分批（keyset）导出

目标程序 `trdsql` 每次调用读取一段不可变的 JSONL 快照并执行一条 SQL。
本目录给出一套**基于稳定查询键的游标分页**方案：每批重新打开同一份快照，
按时间排序导出明细，即使多行时间戳完全相同也不丢行、不重复，
且各批拼接结果与一次性导出**逐字节同序**。

## 目录结构

```
batch_export/
├── README.md
├── snapshot/
│   ├── instrument.jsonl        # 不可变仪表快照（37 行，含大量同时间戳、文件内未按时间排序）
│   └── mini.jsonl              # 4 行最小示例（含同时间戳）
├── scripts/
│   ├── make_snapshot.py        # 生成快照（可指定行数）
│   ├── paged_export.py         # 分页导出驱动器（构造 SQL 与不透明游标）
│   └── verify_export.py        # 分批拼接 vs 一次导出的校验器
└── out/                        # 运行产物（批文件、manifest、对照文件）
```

所有 Python 脚本均以 `python3 -I` 运行（隔离模式，不加载当前目录下的任何
`*.py`，防止快照目录中的植入模块被导入），脚本与数据分目录存放。

## 为什么用 (ts, num) 复合键，而不是只用时间戳

仅 `ORDER BY ts` 在时间戳相同时顺序不确定，用 `WHERE ts > :last_ts` 做翻页
会在同一时间戳组内**跳行或重行**。本方案引入目标程序自带的稳定行号列：

- `trdsql -inum` 为每一行附加 `num` 列，取值是该行在 JSONL 中的**物理行号
  （从 1 开始）**。对一份固定快照，`num` 唯一且永不改变。
- 排序键定为 `(ts ASC, num ASC)`。`num` 作为决胜列（tie-breaker），
  使全序确定：相同时间戳的行按物理出现次序排列，一行都不会丢。

游标记录上一批最后一行的 `(ts, num)`，下一批重新打开快照后用**行值谓词**
取严格位于该键之后的行：

```sql
SELECT num, ts, meter_id, reading_id, kwh, quality
FROM `snapshot/instrument.jsonl`
WHERE `ts` > :last_ts
   OR (`ts` = :last_ts AND `num` > :last_num)
ORDER BY `ts` ASC, `num` ASC
LIMIT :batch_size_plus_one
```

`WHERE` 谓词与 `ORDER BY` 使用完全相同的键，因此谓词切分点与排序切分点一致，
批与批之间无缝、无重叠。

## 不透明游标

游标是对 `{"v":1,"ts":...,"num":...}` 做 base64url 编码得到的不透明 token，
由驱动器产出、回传，调用方只需原样保存并在下一批 `--cursor` 带回，无需理解
其内容。解码时校验结构、`num` 为正整数，非法 token 直接报错（退出码 2），
不会拼进 SQL。字符串字面量统一做单引号转义。

## has_more 与“多取一行”

每批实际请求 `LIMIT batch_size + 1`：

- 返回的前 `batch_size` 行作为本批输出；
- 若还存在第 `batch_size+1` 行，则 `has_more=true`，说明后面还有数据。

这样在总行数恰好是批大小整数倍时也**不会产生空探测批次**；到达末尾时
`has_more=false`、`next_cursor` 仍指向最后一行（可用于幂等重放，再查一次
得到 0 行）。

## 重新打开快照的一致性保证

每一批都是一次独立的 `trdsql` 进程，重新打开**同一份**快照文件。
`manifest.json` 记录快照的绝对路径与 SHA-256，校验器会重新计算磁盘文件
哈希并比对，确保所有批次读取的是同一份未被修改的数据。

## 复现步骤

```bash
# 1. 构建目标程序（在仓库根目录 /workspace）
go build -o /tmp/trdsql ./cmd/trdsql

cd batch_export

# 2. 单批演示（无游标 = 第一批）
python3 -I scripts/paged_export.py \
  --snapshot snapshot/instrument.jsonl --batch 5 --trdsql /tmp/trdsql
# stderr 给出 next_cursor 与 has_more

# 3. 带回游标读取下一批（再次独立打开同一快照）
python3 -I scripts/paged_export.py \
  --snapshot snapshot/instrument.jsonl --batch 5 --trdsql /tmp/trdsql \
  --cursor '<上一步的 next_cursor>'

# 4. 自动循环导出全部批次，写批文件与 manifest
python3 -I scripts/paged_export.py \
  --snapshot snapshot/instrument.jsonl --batch 7 --trdsql /tmp/trdsql \
  --all --out out/run_b7

# 5. 校验：分批拼接必须与一次导出完全一致
python3 -I scripts/verify_export.py \
  --snapshot snapshot/instrument.jsonl --batch 7 \
  --trdsql /tmp/trdsql --work out
```

## 已验证的场景

校验器对批大小 `1、2、5、10、37（=总行数）、100（>总行数）` 全部通过，断言：

1. 分批拼接的行数、逐行内容、顺序与一次导出完全相同（并做了字节级 `diff`）；
2. 所有 `num` 构成完整的 `1..N` 集合，无缺失、无重复；
3. 拼接结果全局满足 `(ts, num)` 有序；每个时间戳组的行数与参照一致
   （快照中最大同时间戳组 7 行，且存在批次边界从组内切分的情况，组内全行保留）；
4. 每条 `cursor_out` 精确指向该批最后一行，下一批首行严格位于其后；
5. manifest 的快照哈希与磁盘文件一致。

另验证：末尾 `has_more=false`、越过末尾游标返回 0 行、损坏/负值游标被拒绝。
