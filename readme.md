# BiliData

基于 Lambda 架构的视频平台实时数据仓库，支持离线批处理与实时流处理的统一分析。

## 特性

- **维度整合**：整合维表，提供实体信息的单点查询能力
- **主题分析**：进行多表关联，提供面向主题的分析能力
- **实时看板**：实现离线、实时链路合并，搭建实时看板

## 技术栈

| 组件 | 用途 |
|------|------|
| Hive | 离线数仓存储与批处理 |
| Flink SQL / Flink Java | 实时流处理 |
| HBase | 维度表存储，支持高并发单点查询 |
| ClickHouse | OLAP 分析引擎 |
| Redis | 热数据缓存 |
| Kafka | 消息队列 |
| DolphinScheduler | 作业调度 |

## 项目结构

```
BiliData/
├── layer-batch/          # 离线批处理层
│   ├── dim/              # 维度表 (账号、视频、评论)
│   ├── dwd/              # 明细层 (行为、统计、订单)
│   └── dws/              # 汇总层
├── layer-speed/          # 实时处理层
│   ├── *.sql             # Flink SQL 作业
│   └── vip-order-flink/  # Flink Java 作业
├── layer-serving/        # 服务层
│   ├── Hive2ChDws.sql    # Hive -> ClickHouse 同步
│   └── Hive2HbDim.sql    # Hive -> HBase 同步
└── mock/                 # 模拟数据生成
```

## 数仓分层

```
ODS (原始数据层)
    ↓
DWD (明细数据层) ─── 行为明细 / 统计明细 / 订单明细
    ↓
DWS (汇总数据层) ─── 视频统计 / 账号注册来源 / VIP订单来源
    ↓
DIM (维度层) ─────── 账号维度 / 视频维度 / 评论维度
    ↓
ADS (应用数据层) ─── ClickHouse / HBase
```

## 架构图

> [飞书云文档](https://y1hmnem2vtl.feishu.cn/docx/C4Hfddk9MoCSY4xxdVTcaR2Pnsh?openbrd=1&doc_app_id=501&blockId=doxcnUUICwp6ysqHSfXZppon4Nf&blockType=whiteboard&blockToken=MqnGwDFC3ho8mqboOKOc3wNCnfn#doxcnUUICwp6ysqHSfXZppon4Nf)

![数据链路架构图](./whiteboard.jpg)

## 看板预览

![看板预览](./kanban.jpg)

## 调度依赖

DolphinScheduler 作业依赖关系：

![调度作业依赖图](./dolphinscheduler.png)
