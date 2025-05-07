# solana-geyser-sqs
This plugin is no longer maintained.

### Build

```bash 
$ cargo build --release
```

### Plugin for validator

Plugin config example located in [config.json](config.json). **You need to change `libpath` field.**

Now you can run validator with plugin:

```bash
$ solana-validator --geyser-plugin-config ./config.json
```

### Accounts data compression

Since SQS payload can be only 256KB long and Solana accounts can be up to 10MB not all accounts will fit to SQS message. Currently messages with these accounts are dropped. But it's possible to decrease number of dropped messages if we will compress data. Right now `zstd` and `gzip` are supported.

```json
"messages": {
    "commitment_level": "confirmed",
    "accounts_data_compression": {"algo": "none"}
}
"messages": {
    "commitment_level": "confirmed",
    "accounts_data_compression": {"algo": "zstd", "level": 3}
}
"messages": {
    "commitment_level": "confirmed",
    "accounts_data_compression": {"algo": "gzip", "level": 6}
}
```

`level` field is optional for both compression algorithms.

### AWS Credentials

**Required permissions:**

- `sqs:Write`
- `sqs:GetAttribute`

Currently two types of authentication are supported: `Static` and `Chain`.

`Static` config:

```json
{
    "sqs": {
        "auth": {
            "access_key_id": "...",
            "secret_access_key": "..."
        }
    }
}
```

https://docs.rs/rusoto_credential/latest/rusoto_credential/struct.StaticProvider.html

`Chain` config:

```json
{
    "sqs": {
        "auth": {
            "credentials_file": "/home/user/.aws/credentials",
            "profile": "rpcpool"
        }
    }
}
```

`credentials_file` and `profile` fields are optional.

https://docs.rs/rusoto_credential/latest/rusoto_credential/struct.ChainProvider.html

### Account filters

Accounts can be filtered by:

- `account`  - acount Pubkey, match to any Pubkey from the array
- `owner` — account owner Pubkey, match to any Pubkey from the array
- `data_size` — account data size, match to any value in the array
- `tokenkeg_owner` —  `spl-token` accounts when `owner` field field in `Account` state match to any Pubkey from the array
- `tokenkeg_delegate` —  `spl-token` accounts when `delegate` field field in `Account` state match to any Pubkey from the array

All fields in filter are optional but at least 1 is required. Fields works as logical `AND`. Values in the arrays works as logical `OR`.

##### Examples

Filter accounts:

```json
{
    "accounts": {
        "filter-by-account": {
            "account": [
                "83astBRguLMdt2h5U1Tpdq5tjFoJ6noeGwaY3mDLVcri",
                "vines1vzrYbzLMRdu58ou5XTby4qAqVRLmqo36NKPTg"
            ]
        }
    }
}
```

Filter accounts with `owner` `Vote111111111111111111111111111111111111111` and size 128:

```json
{
    "accounts": {
        "filter-by-owner-and-size": {
            "owner": ["Vote111111111111111111111111111111111111111"],
            "data_size": [128]
        }
    }
}
```

Filter accounts with size 256:

```json
{
    "accounts": {
        "filter-by-size": {
            "data_size": [256]
        }
    }
}
```

```json
{
    "accounts": {
        "filter-by-owner-and-delegate": {
            "tokenkeg_owner": ["GUfCR9mK6azb9vcpsxgXyj7XRPAKJd4KMHTTVvtncGgp"],
            "tokenkeg_delegate": ["1BWutmTvYPwDtmw9abTkS4Ssr8no61spGAvW1X6NDix"]
        }
    }
}
```

### Transaction filters

```json
{
    "transactions": {
        "all-txs": {
            "logs": true,
            "vote": false,
            "failed": false,
            "accounts": {
                "include": [],
                "exclude": []
            }
        }
    }
}
```

It's possible to log filter names to redis for transactions which were sent to SQS.

```json
        "redis_logs": {
            "url": "redis://127.0.0.1:6379/1",
            "map_key": "transactions-%Y-%m-%d-%H-%M",
            "batch_size": 10,
            "concurrency": 50
        },
```

`map_key` value will be evaluated with `strftime` modifiers: https://docs.rs/chrono/latest/chrono/format/strftime/

There is not any auto cleaner process, so you need to clean redis yourself.

### Sample SQS messages

Message matching an owner filter:

```json
{
    "data": "JEUwEoDhfYfMelONKGC7EeZcXrRT6lUHSnYcdIMkwvz7+JphEypNMqodwc1cnR16QKXhycV448DIRhZ2cEf0xZvhaQgSIsjxAMOd0AAAAAA=",
    "executable": false,
    "filters": [
        "my-filter-name-1"
    ],
    "lamports": 1447680,
    "owner": "MEisE1HzehtrDpAAT8PnLHjpSSkRYakotTuJRPjTpo8",
    "pubkey": "7nFcBnqt83uQdootsfpDi5vaVuXuL4DjvsnxnqQ1ps4e",
    "rent_epoch": 285,
    "slot": 123290000,
    "type": "account",
    "write_version": 236443918472
}
```

Message matching a tokenkeg filter:

```json
{
    "type": "account",
    "filters": ["my-filter-1", "my-filter-2"],
    "data": "B7cGehJ8vTDz6yS3Dy2+/irpr0uFNvJGOk1/J8Go5mCXh3ML3zliZHwPURW1a39T5sRm3WJ4Kjw7jIIruUXOmAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "executable": false,
    "lamports": 2039280,
    "owner": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
    "pubkey": "3dcHyQ4SjMqgDjr2Maeq5HrgiuWRzRqovJxb4bWMmyxh",
    "rent_epoch": 285,
    "slot": 123309601,
    "write_version": 236563574827
}
```

Message matching a transaction filter:

```json
{
    "type": "transaction",
    "filters": ["all-txs"],
    "meta": {
        "err": null,
        "fee": 10000,
        "innerInstructions": [],
        "logMessages": [
            "Program Vote111111111111111111111111111111111111111 invoke [1]",
            "Program Vote111111111111111111111111111111111111111 success"
        ],
        "postBalances": [499997095000, 1000000000000000, 143487360, 1169280, 1],
        "postTokenBalances": [],
        "preBalances": [499997105000, 1000000000000000, 143487360, 1169280, 1],
        "preTokenBalances": [],
        "rewards": [],
        "status": {
            "Ok": null
        }
    },
    "signature": "5k6VtaMh9iTifgZ5pNTm31T4BCobMeAFXvxDjsYpTjPLAVhgMPvwYRaMg5yaxHAqtjDa2o5bFwx4NFcuASoMxFz",
    "slot": 621,
    "transaction": "AgQXFwSYxFH87h7wrfgTOGKkB7cp0ALUn/sa+ZIC0BtEZfC0izqw8nSrHSsTgJyR3UopMS/il7Sw7/TCGatw/AGoeHVRKjzcouhH1sOcO8jPd8DUZUjjkSCxCQsoGev+EiamdRH76HINb1jmwSsRZy5ltQDtehV/GACjf6YsI8IGAgADBWq/1tY1Q0Uxay2cZ2AuZ6ns5oPAK6d7x+QITzsrbAqGeD6V2vys7Aji1g5puZKmytQC/polXFV7zhON6w8wt0kGp9UXGS8Kr8byZeP7d8x62oLFKdC+OxNuLQBVIAAAAAan1RcYx3TJKFZjmGkdXraLXrijm0ttXHNVWyEAAAAAB2FIHTV0dLt8TXYk69O9s9g1XnPREEP8DaNTgAAAAABknYvFYW4mcQxPTajheD5ptJrxqhd0jf4UN3jqnQ+99AEEBAECAwE9AgAAAAEAAAAAAAAAbAIAAAAAAADZBHfCuXX27yHwPoK7eTXB5FG8qIhJX8RSiTsCsF4KJwEbfFZiAAAAAA=="
}
```

### FIFO Queues

FIFO Queues are supported but you need to enable `ContentBasedDeduplication` for your queue. `message_group_id` will be set to `solana`.

### Message attributes

Except always existed message attributes `compression` and `md5` it's possible to set custom attributes in `sqs` config key:

```json
"sqs": {
    "attributes": {
        "node": "my-node-1",
        "any-other-attribute": "value of any other attribute"
    }
}
```

### Large Public Keys sets in config

If we want to start watch for a lot of Public Keys our config will be huge and for any change we will need upload a large object to Redis. As alternative plugin allow to use Redis `Set` which store Public Keys.

Config example:

```json
{
    "accounts": {
        "filter-by-account": {
            "account": [{"set": "filter-by-account-keys"}]
        }
    }
}
```

With these config plugin will expect Public Keys in `filter-by-account-keys`.

### Admin channel

Plugin support communication through redis, for activate it you need to add `redis` object:

```json
"redis": {
    "url": "redis://127.0.0.1:6379/",
    "channel": "admin",
    "config": "config"
}
```

#### Startup notification

On startup plugin sent notification message: `{"node":"my-unique-node-name","id":null,"result":"started"}`.

#### Ping

It's possible to ping the plugin to check that it's alive.

Request:

```json
{"id":0,"method":"ping"}
```

Response:

```json
{"node":"my-unique-node-name","id":0,"result":"pong"}
```

#### Config reload

It's also possible to reload filters without re-starting solana-validator.

You can check CLI-tool for this: `cargo run --bin config -- --help`.

Plugin will load config from Redis on startup, `config` key from Redis in current example (value should be stored as `String`), `slots`/`accounts`/`transactions` from json file will be overwritten. Config format in Redis is same as in `filters` section in JSON config file.

Each time when config need to be reloaded these data should be published to the channel (`admin` in current example): `{"id":0,"method":"global"}`. As response you will get: `{"node":"my-unique-node-name","id":0,"result":"ok"}`.

#### Small reloads

If we have large set of Public Keys it's not convenient reload whole config because we will to fetch all the data from Redis. Instead you can sent command which add or remove Public Key from the filter. Data in Redis should be updated by you, same as for global reload.

For example:

```bash
$ cargo run --bin config -- --config ./config.json send-signal pubkeys-set --action add --filter accounts --kind account --name example --pubkey MY_PUBLIC_KEY
Send message: {"id":13228547606808916809,"method":"pubkeys_set","params":{"filter":{"accounts":{"name":"example","kind":"account"}},"action":"add","pubkey":"MY_PUBLIC_KEY"}}
Received msg from node "my-unique-node-name": {"node":"my-unique-node-name","id":13228547606808916809,"result":"ok"}
$ cargo run --bin config -- --config ./config.json send-signal pubkeys-set --action remove --filter accounts --kind account --name example --pubkey MY_PUBLIC_KEY
Send message: {"id":5831892823480507891,"method":"pubkeys_set","params":{"filter":{"accounts":{"name":"example","kind":"account"}},"action":"remove","pubkey":"MY_PUBLIC_KEY"}}
Received msg from node "my-unique-node-name": {"node":"my-unique-node-name","id":5831892823480507891,"result":"ok"}
```
