## 账号之间的图型关系难以处理

  - 旧处理方式
    - 使用Redis存储热数据
    - 使用HBase存储和查询，rowkey要适合按用户扫描
    - 已注销用户丢弃，只在Hive保留数据
  - 新处理方式
    - 关联用户以Hive列表保存到用户维表

## 为什么离线链路的dim_account_df dim_video_df dim_comment_df部分维度直接从ods_app_di获取

  - 因为ods_app_di中相关topic已经包含完整的维度，不涉及复杂连接和处理
  - ods_app_di已经通过合理的分区保证了读取性能，按天、埋点话题分区

  - 相比之下，dim_video_df和dim_action_df因为涉及历史累计，所以创建了dwd并进行聚合得到每日增量的度量值stats，每日累加到维度表
  - 天内的更新在实时链路完成，T+1的更新在离线链路完成

## 订单表到12点之后怎么处理？跨天支付的vip订单怎么办？

  - 因为是累计快照事实表，离线链路根据已经支付的事实得到T+1，实时链路的待支付订单保存在redis，能正常处理跨天支付

## 使用AI的时候有没有遇到什么困难

  - AI的数分思维很严重，因为AI倾向于将所有情况进行穷举，设计的表不仅扩展性非常差，字段也特别多，性能很差

## 为什么用HBase存维度
  - 因为HBase的单条记录检索能力强，而且扩展性很好。Redis难以扩展，Hive适合批量处理
  - 因为这不只是一个分析型数据仓库，这还是一个后端数据库。通过降低实时链路的延迟，我相信这个数据仓库可以应对后端查询场景

## 对齐粒度的问题

  - 在开发维度表的时候，需要进行大量连接，因为Mock生成的评论出现了“一个评论ID对应多个视频ID”，导致使用评论ID连接产生笛卡尔积。解决办法是将视频ID也加入连接条件

## Hive函数的问题

  - SIZE(NULL)返回-1，SIZE(ARRAY())返回0
  - CAST(ARRAY() AS ARRAY<BIGINT>)返回NULL，SIZE(CAST(ARRAY() AS ARRAY<BIGINT>))返回-1（气笑了，不得不用`array_slice(array(0L),1,0)`生成空的ARRAY<BIGINT>）

## 为什么Serving-Layer选用HBase存放实时维度？

  - 因为HBase支持高并发单点查询
  - Redis扩展性不好，Hive、ClickHouse单点并发性不好

## 实时处理开窗有哪些注意的

  - 使用event_ts并设置watermark允许迟到的时候，需要注意每个taskmanager的时区需要和event_ts时区一致，否则无法落入窗内

## 已知问题

  - 如果用户还未收藏视频，就将其取消收藏了，会导致视频收藏数-1，需要确保unfavorite和unlike行为是合法的
  - 实时链路DimAccount没有实时更新用户关系列表(following_list)，只实时更新计数(following_chg)
  - 实时链路的dws作业考虑性能问题未进行look-up join，会比离线dws少很多维度
  - dolphin scheduler对flinksql的变量解析有问题，需要每日手动终端运行或crontab调度
  - Java的hbase-client可能有问题（即便版本号对上）
