# StarRocks Flink Connector 项目分析

## 项目概述

这是一个 Apache Flink 与 StarRocks 的连接器项目，用于从 StarRocks 读取数据和向 StarRocks 写入数据。本分析主要关注**如何从 StarRocks 查询和读取数据**的机制。

## 核心架构特点

与 Flink 的 JDBC Connector 不同，StarRocks Flink Connector 支持**并行读取多个 BE（Backend）节点**的数据，大大提升了读取性能。

### 传统 JDBC Connector
- Flink 只能从单个 FE（Frontend）逐一读取数据
- 性能慢，无法并行

### StarRocks Flink Connector
- Flink 先从 FE 获取查询计划（Query Plan）
- 将查询计划分发到多个 BE 节点
- 并行从多个 BE 读取数据
- 大幅提升读取性能

## 数据查询交互流程

### 1. 整体流程架构

```
Flink Application
    ↓
StarRocksDynamicSourceFunction（Source 函数）
    ↓
StarRocksSourceCommonFunc（通用函数）
    ↓
StarRocksQueryPlanVisitor（查询计划访问器）
    ↓ HTTP POST
FE（Frontend - 通过 HTTP 接口获取查询计划）
    ↓
返回 QueryPlan（包含：opaqued_query_plan + tablet 分布信息）
    ↓
分配到各个并行 SubTask
    ↓
StarRocksSourceBeReader（BE 读取器）
    ↓ Thrift RPC
BE（Backend - 通过 Thrift 接口读取数据）
    ↓
返回 Arrow 格式数据
    ↓
ArrowFieldConverter（Arrow 转换器）
    ↓
Flink RowData
```

### 2. 详细步骤说明

#### 步骤 1: 构建 SQL 并获取查询计划

**类：** `StarRocksSourceCommonFunc` 和 `StarRocksQueryPlanVisitor`

1. 根据用户配置生成 SQL 查询语句
```java
// StarRocksDynamicSourceFunction.genSQL()
String SQL = "SELECT " + columns + 
             " FROM `" + database + "`.`" + table + "`" + 
             " WHERE " + filter;
```

2. 向 FE 发送 HTTP POST 请求获取查询计划
```java
// StarRocksQueryPlanVisitor.getQueryPlan()
URL: http://<fe_host>:<fe_http_port>/api/<database>/<table>/_query_plan
Method: POST
Headers: 
  - Content-Type: application/json
  - Authorization: Basic <base64(username:password)>
Body: {"sql": "<SQL查询语句>"}
```

3. FE 返回 QueryPlan，包含：
   - `opaqued_query_plan`: 序列化的查询执行计划（Base64 编码）
   - `partitions`: Tablet ID 到 BE 节点的映射关系
   ```json
   {
     "status": 200,
     "opaqued_query_plan": "<base64_encoded_plan>",
     "partitions": {
       "12345": {"routings": ["192.168.1.10:9060", "192.168.1.11:9060"]},
       "12346": {"routings": ["192.168.1.11:9060", "192.168.1.12:9060"]}
     }
   }
   ```

#### 步骤 2: 将 Tablet 分配给 BE 节点

**类：** `StarRocksQueryPlanVisitor.transferQueryPlanToBeXTablet()`

将查询计划中的 tablet 信息转换为 BE 节点到 Tablet 列表的映射：

```java
Map<String, Set<Long>> beXTablets = {
    "192.168.1.10:9060": [12345, 12348, ...],
    "192.168.1.11:9060": [12346, 12349, ...],
    "192.168.1.12:9060": [12347, 12350, ...]
}
```

负载均衡策略：
- 优先选择还没有分配 tablet 的 BE
- 否则选择当前 tablet 数量最少的 BE

#### 步骤 3: 分配任务到并行 SubTask

**类：** `StarRocksSourceCommonFunc.splitQueryBeXTablets()`

根据 Flink 的并行度（parallelism）将 BE×Tablets 分配给各个 SubTask：

```
并行度 = 5, BE×Tablets = 3
  SubTask 0 -> BE1 的 tablets
  SubTask 1 -> BE2 的 tablets  
  SubTask 2 -> BE3 的 tablets
  SubTask 3 -> (空)
  SubTask 4 -> (空)

并行度 = 2, BE×Tablets = 5
  SubTask 0 -> BE1, BE3, BE5
  SubTask 1 -> BE2, BE4
```

#### 步骤 4: 打开 Scanner（扫描器）

**类：** `StarRocksSourceBeReader.openScanner()`

每个 SubTask 通过 **Thrift RPC** 连接到分配的 BE 节点：

1. 建立 Thrift 连接
```java
TSocket socket = new TSocket(be_ip, be_port, timeout, timeout);
TProtocol protocol = new TBinaryProtocol.Factory().getProtocol(socket);
TStarrocksExternalService.Client client = new TStarrocksExternalService.Client(protocol);
```

2. 调用 `open_scanner` 方法
```java
TScanOpenParams params = new TScanOpenParams();
params.setTablet_ids([12345, 12348, ...]); // 分配给这个 BE 的 tablet IDs
params.setOpaqued_query_plan(opaqued_query_plan); // FE 返回的查询计划
params.setDatabase(database);
params.setTable(table);
params.setUser(username);
params.setPasswd(password);
params.setBatch_size(batch_size); // 每批返回的行数
params.setKeep_alive_min(keep_alive_min);
params.setQuery_timeout(query_timeout);
params.setMem_limit(mem_limit);

TScanOpenResult result = client.open_scanner(params);
String context_id = result.getContext_id(); // BE 返回的上下文 ID
```

#### 步骤 5: 批量读取数据

**类：** `StarRocksSourceBeReader.startToRead()`

使用返回的 `context_id` 批量读取数据：

```java
TScanNextBatchParams params = new TScanNextBatchParams();
params.setContext_id(context_id);
params.setOffset(offset); // 当前读取偏移量

TScanBatchResult result = client.get_next(params);
// result.rows: Arrow 格式的二进制数据
// result.eos: 是否结束（End of Stream）
```

循环调用 `get_next` 直到 `eos = true`。

#### 步骤 6: 解析 Arrow 数据

**类：** `StarRocksSourceFlinkRows` 和 `ArrowFieldConverter`

BE 返回的数据是 **Apache Arrow** 格式的二进制数据，需要解析：

1. 创建 Arrow Reader
```java
byte[] bytes = result.getRows();
ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
ArrowStreamReader arrowStreamReader = new ArrowStreamReader(inputStream, rootAllocator);
VectorSchemaRoot root = arrowStreamReader.getVectorSchemaRoot();
```

2. 逐批读取并转换为 Flink RowData
```java
while (arrowStreamReader.loadNextBatch()) {
    List<FieldVector> fieldVectors = root.getFieldVectors();
    int rowCount = root.getRowCount();
    
    for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        GenericRowData row = new GenericRowData(columnCount);
        for (int colIndex = 0; colIndex < fieldVectors.size(); colIndex++) {
            FieldVector fieldVector = fieldVectors.get(colIndex);
            ArrowFieldConverter converter = fieldConverters.get(colIndex);
            Object value = converter.convert(fieldVector, rowIndex);
            row.setField(colIndex, value);
        }
        flinkRows.add(row);
    }
}
```

#### 步骤 7: 关闭 Scanner

**类：** `StarRocksSourceBeReader.close()`

读取完成后关闭扫描器：

```java
TScanCloseParams closeParams = new TScanCloseParams();
closeParams.setContext_id(context_id);
client.close_scanner(closeParams);
```

## 核心类说明

### 数据读取相关类

| 类名 | 作用 |
|------|------|
| `StarRocksSource` | 入口类，创建 DataStream Source |
| `StarRocksDynamicSourceFunction` | Flink Source Function，负责协调数据读取 |
| `StarRocksSourceCommonFunc` | 通用函数，包含查询计划获取、tablet 分配等逻辑 |
| `StarRocksQueryPlanVisitor` | 通过 HTTP 从 FE 获取查询计划 |
| `StarRocksSourceBeReader` | 通过 Thrift 从 BE 读取数据 |
| `StarRocksSourceFlinkRows` | Arrow 数据解析为 Flink RowData |
| `ArrowFieldConverter` | Arrow 字段类型转换器 |

### 数据结构类

| 类名 | 作用 |
|------|------|
| `QueryPlan` | 查询计划，包含 opaqued_query_plan 和 partitions |
| `QueryInfo` | 查询信息，包含 QueryPlan 和 BE×Tablets 分配 |
| `QueryBeXTablets` | BE 节点及其负责的 Tablet 列表 |
| `SelectColumn` | 选择的列信息 |
| `ColumnRichInfo` | 列的详细信息（名称、类型、索引） |

### Thrift 服务定义

**服务：** `TStarrocksExternalService`

```thrift
service TStarrocksExternalService {
    // 打开扫描器
    TScanOpenResult open_scanner(TScanOpenParams params);
    
    // 获取下一批数据
    TScanBatchResult get_next(TScanNextBatchParams params);
    
    // 关闭扫描器
    TScanCloseResult close_scanner(TScanCloseParams params);
}
```

## 关键技术点

### 1. 并行读取
- 利用 Flink 的并行度机制，每个 SubTask 负责读取一部分 BE 节点
- 通过合理的 tablet 分配算法，实现负载均衡

### 2. HTTP + Thrift 混合协议
- **HTTP**: 与 FE 通信获取查询计划（元数据操作）
- **Thrift**: 与 BE 通信读取数据（数据传输）

### 3. Apache Arrow 数据格式
- BE 返回高效的列式存储格式（Arrow）
- 减少序列化/反序列化开销
- 支持零拷贝内存访问

### 4. 查询下推（Predicate Pushdown）
- 过滤条件（WHERE）在 SQL 中指定
- FE 生成优化的查询计划
- BE 在存储层进行过滤，减少数据传输

### 5. 容错机制
- 支持重试（max_retries）
- 连接超时控制
- Scanner 保活机制（keep_alive_min）

## 配置参数

### 连接参数
- `scan-url`: FE HTTP 地址（获取查询计划）
- `jdbc-url`: FE JDBC 地址（元数据查询）
- `username/password`: 认证信息

### 性能参数
- `scan.params.batch-size`: 每批返回的行数（默认 4096）
- `scan.params.mem-limit-byte`: BE 查询内存限制（默认 1GB）
- `scan.params.keep-alive-min`: Scanner 保活时间（默认 10 分钟）
- `scan.params.query-timeout-s`: 查询超时时间（默认 600 秒）
- `scan.max-retries`: 最大重试次数（默认 1）

### 查询参数
- `scan.columns`: 选择的列（空表示 SELECT *）
- `scan.filter`: WHERE 过滤条件

## 数据类型映射

| StarRocks | Flink |
|-----------|-------|
| BOOLEAN | BOOLEAN |
| TINYINT/SMALLINT/INT/BIGINT | TINYINT/SMALLINT/INT/BIGINT |
| LARGEINT | STRING |
| FLOAT/DOUBLE | FLOAT/DOUBLE |
| DATE | DATE |
| DATETIME | TIMESTAMP |
| DECIMAL* | DECIMAL |
| CHAR/VARCHAR | CHAR/STRING |
| JSON | STRING |
| ARRAY | ARRAY |
| STRUCT | ROW |
| MAP | MAP |

## 使用示例

### Flink SQL 方式
```sql
CREATE TABLE flink_test (
    id INT,
    name STRING,
    score INT
) WITH (
    'connector' = 'starrocks',
    'scan-url' = '192.168.1.10:8030',
    'jdbc-url' = 'jdbc:mysql://192.168.1.10:9030',
    'username' = 'root',
    'password' = '',
    'database-name' = 'test',
    'table-name' = 'score_board'
);

SELECT id, name FROM flink_test WHERE score > 20;
```

### DataStream API 方式
```java
StarRocksSourceOptions options = StarRocksSourceOptions.builder()
    .withProperty("scan-url", "192.168.1.10:8030")
    .withProperty("jdbc-url", "jdbc:mysql://192.168.1.10:9030")
    .withProperty("username", "root")
    .withProperty("password", "")
    .withProperty("database-name", "test")
    .withProperty("table-name", "score_board")
    .build();

TableSchema schema = TableSchema.builder()
    .field("id", DataTypes.INT())
    .field("name", DataTypes.STRING())
    .field("score", DataTypes.INT())
    .build();

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.addSource(StarRocksSource.source(schema, options))
   .setParallelism(5)
   .print();
env.execute("StarRocks Flink Source");
```

## 总结

StarRocks Flink Connector 通过以下机制实现高效的数据查询：

1. **查询计划获取**: 通过 HTTP 从 FE 获取优化的查询计划和 tablet 分布信息
2. **并行扫描**: 将 tablets 分配给多个 Flink SubTask，并行从多个 BE 读取数据
3. **高效传输**: 使用 Thrift RPC + Apache Arrow 格式，减少网络开销和序列化开销
4. **查询优化**: 支持谓词下推、列裁剪等优化技术
5. **容错保障**: 重试机制、超时控制、保活机制确保稳定性

这种架构充分利用了 StarRocks 的分布式特性和 Flink 的并行计算能力，实现了高性能的数据读取。
