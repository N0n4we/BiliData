```sh
hbase shell
create_namespace 'dim'
```

dim_account
rowkey: reverse(mid)
```sh
create 'dim:dim_account',
  {NAME => 'basic', VERSIONS => 1},
  {NAME => 'status', VERSIONS => 1},
  {NAME => 'official', VERSIONS => 1},
  {NAME => 'setting', VERSIONS => 1},
  {NAME => 'tag', VERSIONS => 1},
  {NAME => 'relation', VERSIONS => 1},
  {NAME => 'etl', VERSIONS => 1}
```

dim_comment
rowkey: reverse(rpid)
```sh
create 'dim:dim_comment',
  {NAME => 'info', VERSIONS => 1},
  {NAME => 'content', VERSIONS => 1},
  {NAME => 'stats', VERSIONS => 1},
  {NAME => 'meta', VERSIONS => 1}
```

dim_video
rowkey: reverse(bvid)
```sh
create 'dim:dim_video',
  {NAME => 'basic', VERSIONS => 1},
  {NAME => 'content', VERSIONS => 1},
  {NAME => 'stats', VERSIONS => 1},
  {NAME => 'meta', VERSIONS => 1}
```
