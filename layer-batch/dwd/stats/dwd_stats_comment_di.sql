-- 每日聚合评论度量值
INSERT OVERWRITE TABLE dwd.dwd_stats_comment_di
PARTITION (dt = '${bizdate}')
SELECT
    rpid,
    oid,
    -- 各事件类型计数
    SUM(CASE WHEN event_id = 'comment_like' THEN 1 ELSE 0 END)      AS like_count_delta,
    SUM(CASE WHEN event_id = 'comment_unlike' THEN 1 ELSE 0 END)    AS unlike_count_delta,
    SUM(CASE WHEN event_id = 'comment_dislike' THEN 1 ELSE 0 END)   AS dislike_count_delta,
    SUM(CASE WHEN event_id = 'comment_undislike' THEN 1 ELSE 0 END) AS undislike_count_delta,
    SUM(CASE WHEN event_id = 'comment_reply' THEN 1 ELSE 0 END)     AS reply_count_delta,
    SUM(CASE WHEN event_id = 'comment_report' THEN 1 ELSE 0 END)    AS report_count,
    -- 净增量
    SUM(CASE WHEN event_id = 'comment_like' THEN 1
             WHEN event_id = 'comment_unlike' THEN -1
             ELSE 0 END)                                             AS net_like_delta,
    SUM(CASE WHEN event_id = 'comment_dislike' THEN 1
             WHEN event_id = 'comment_undislike' THEN -1
             ELSE 0 END)                                             AS net_dislike_delta
FROM dwd.dwd_action_comment_di
WHERE dt = '${bizdate}'
  AND rpid IS NOT NULL
GROUP BY rpid, oid;
