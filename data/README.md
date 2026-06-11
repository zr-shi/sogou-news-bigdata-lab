# Input data

Place an optional `sougou.log` file in this directory.

Each non-empty row must contain exactly six comma-separated fields:

```text
time,user_id,news_topic,rank,page,source
```

When `sougou.log` is absent, the producer generates demo news events automatically.
