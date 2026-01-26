```sh
hbase shell
create_namespace 'dim'
```

dim_account
rowkey: reverse(mid)
```sh
create 'dim:dim_account',
  {NAME => 'basic', VERSIONS => 1, COMPRESSION => 'SNAPPY'},
  {NAME => 'status', VERSIONS => 1, COMPRESSION => 'SNAPPY'},
  {NAME => 'official', VERSIONS => 1, COMPRESSION => 'SNAPPY'},
  {NAME => 'setting', VERSIONS => 1, COMPRESSION => 'SNAPPY'},
  {NAME => 'tag', VERSIONS => 1, COMPRESSION => 'SNAPPY'},
  {NAME => 'relation', VERSIONS => 1, COMPRESSION => 'SNAPPY'},
  {NAME => 'etl', VERSIONS => 1, COMPRESSION => 'SNAPPY'}
```

dim_comment
rowkey: reverse(rpid)
```sh
create 'dim:dim_comment',
  {NAME => 'info', VERSIONS => 1, COMPRESSION => 'SNAPPY'},
  {NAME => 'content', VERSIONS => 1, COMPRESSION => 'SNAPPY'},
  {NAME => 'stats', VERSIONS => 1, COMPRESSION => 'SNAPPY'},
  {NAME => 'meta', VERSIONS => 1, COMPRESSION => 'SNAPPY'}
```

dim_video
rowkey: reverse(bvid)
```sh
create 'dim:dim_video',
  {NAME => 'basic', VERSIONS => 1, COMPRESSION => 'SNAPPY'},
  {NAME => 'content', VERSIONS => 1, COMPRESSION => 'SNAPPY'},
  {NAME => 'stats', VERSIONS => 1, COMPRESSION => 'SNAPPY'},
  {NAME => 'meta', VERSIONS => 1, COMPRESSION => 'SNAPPY'}
```
