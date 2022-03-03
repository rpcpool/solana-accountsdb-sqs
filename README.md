# solana-accountsdb-sqs

### Build

```bash 
$ cargo build --release
```

### Plugin for validator

Plugin config example located in [config.json](config.json). **You need change `libpath` field.**

Now you can run validator with plugin:

```bash
$ solana-validator --accountsdb-plugin-config ./config.json
```

### AWS Credentials

**Required permissions:**

- `sqs:Write`
- `sqs:GetAttribute`

Currently two types of authentication are supported: `Static` and `File`.

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

`File` config:

```json
{
    "sqs": {
        "auth": {
            "credentials_file": "/home/kirill/.aws/credentials",
            "profile": "rpcpool"
        }
    }
}
```

`profile` field is optional, default value is `default`.

### Account filters

Accounts can be filtered by:

- `owner` — account owner Pubkey
- `data_size` — account data size

Filter accounts with `owner` Vote111111111111111111111111111111111111111` and size 128:

```json
{
    "owner": "Vote111111111111111111111111111111111111111",
    "data_size": 128
}
```

Filter accounts with size 256:

```json
{
    "data_size": 256
}
```

### Tokenkeg filters

Accounts can be filtered by `owner` and `delegate` fields in Tokenkeg account (when field changed *from* or *to* specified value):

```json
{
    "tokenkeg_owner": [
        "GUfCR9mK6azb9vcpsxgXyj7XRPAKJd4KMHTTVvtncGgp"
    ],
    "tokenkeg_delegate": [
        "1BWutmTvYPwDtmw9abTkS4Ssr8no61spGAvW1X6NDix"
    ]
}
```

### Sample SQS messages

Message matching an owner filter:
```
{
    "data": "JEUwEoDhfYfMelONKGC7EeZcXrRT6lUHSnYcdIMkwvz7+JphEypNMqodwc1cnR16QKXhycV448DIRhZ2cEf0xZvhaQgSIsjxAMOd0AAAAAA=",
    "executable": false,
    "filters": [
        "owner_data_size"
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
