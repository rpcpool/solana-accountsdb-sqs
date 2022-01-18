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

Only one filter type is supported right now:

```json
{
    "owner": "Vote111111111111111111111111111111111111111"
}
```
