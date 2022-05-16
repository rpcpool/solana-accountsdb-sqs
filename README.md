# solana-geyser-sqs

### Build

```bash 
$ cargo build --release
```

### Plugin for validator

Plugin config example located in [config.json](config.json). **You need change `libpath` field.**

Now you can run validator with plugin:

```bash
$ solana-validator --geyser-plugin-config ./config.json
```

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
            "credentials_file": "/home/kirill/.aws/credentials",
            "profile": "rpcpool"
        }
    }
}
```

`credentials_file` and `profile` fields are optional.

https://docs.rs/rusoto_credential/latest/rusoto_credential/struct.ChainProvider.html

### Account filters

Accounts can be filtered by:

- `owner` — account owner Pubkey, match to any Pubkey from the array
- `data_size` — account data size, match to any value in the array
- `tokenkeg_owner` —  `spl-token` accounts when `owner` field field in `Account` state match to any Pubkey from the array
- `tokenkeg_delegate` —  `spl-token` accounts when `delegate` field field in `Account` state match to any Pubkey from the array

All fields in filter are optional but at least 1 is required. Fields works as logical `AND`. Values in the arrays works as logical `OR`.

##### Examples

Filter accounts with `owner` Vote111111111111111111111111111111111111111` and size 128:

```json
{
    "owner": ["Vote111111111111111111111111111111111111111"],
    "data_size": [128]
}
```

Filter accounts with size 256:

```json
{
    "data_size": [256]
}
```

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

### Transaction filters

Currently only one filter exists: `vote`.

```json
{
    "transactions": {
        "active": true,
        "vote": false
    }
}
```

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
    "data": "B7cGehJ8vTDz6yS3Dy2+/irpr0uFNvJGOk1/J8Go5mCXh3ML3zliZHwPURW1a39T5sRm3WJ4Kjw7jIIruUXOmAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "executable": false,
    "filters": [
        "my-filter-1", "my-filter-2"
    ],
    "lamports": 2039280,
    "owner": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
    "pubkey": "3dcHyQ4SjMqgDjr2Maeq5HrgiuWRzRqovJxb4bWMmyxh",
    "rent_epoch": 285,
    "slot": 123309601,
    "type": "account",
    "write_version": 236563574827
}
```

Message matching a transaction filter:

```json
{
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
    "transaction": "AgQXFwSYxFH87h7wrfgTOGKkB7cp0ALUn/sa+ZIC0BtEZfC0izqw8nSrHSsTgJyR3UopMS/il7Sw7/TCGatw/AGoeHVRKjzcouhH1sOcO8jPd8DUZUjjkSCxCQsoGev+EiamdRH76HINb1jmwSsRZy5ltQDtehV/GACjf6YsI8IGAgADBWq/1tY1Q0Uxay2cZ2AuZ6ns5oPAK6d7x+QITzsrbAqGeD6V2vys7Aji1g5puZKmytQC/polXFV7zhON6w8wt0kGp9UXGS8Kr8byZeP7d8x62oLFKdC+OxNuLQBVIAAAAAan1RcYx3TJKFZjmGkdXraLXrijm0ttXHNVWyEAAAAAB2FIHTV0dLt8TXYk69O9s9g1XnPREEP8DaNTgAAAAABknYvFYW4mcQxPTajheD5ptJrxqhd0jf4UN3jqnQ+99AEEBAECAwE9AgAAAAEAAAAAAAAAbAIAAAAAAADZBHfCuXX27yHwPoK7eTXB5FG8qIhJX8RSiTsCsF4KJwEbfFZiAAAAAA==",
    "type": "transaction"
}
```
