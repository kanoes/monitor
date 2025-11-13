# 準備

`.env` ファイルをプロジェクトのルートディレクトリーに作成して、以下の内容にします。

```dotenv
# Log Analytics のワークスペース ID
ALM_WORKSPACE_ID=xxxxxxxxxx
BRAIN_WORKSPACE_ID=xxxxxxxxxx
DOC_WORKSPACE_ID=xxxxxxxxxx
MA_BOT_WORKSPACE_ID=xxxxxxxxxx
MA_WEB_WORKSPACE_ID=xxxxxxxxxx
CA_WORKSPACE_ID=xxxxxxxxxx

# stg または prod
TEMPLATE_TYPE=stg

AZURE_BLOB_CONNECTION_STRING=DefaultEndpointsProtocol=xxxxx
AZURE_BLOB_CONTAINER_NAME=core-analytics

# デバッグ用
SCHEDULER_DEBUG_MODE=1
```

# 起動

```shell
$env:Path = "C:\Users\takat\.local\bin;$env:Path"
uv run gunicorn -k uvicorn.workers.UvicornWorker -w 1 app:app
```

1: if you need to add new workspace:
modify config file

2: if you to add query type
add a new strategy class

3: if you need add new report format: 
Extend report factory


## Dockerを立ち上げるときのコマンド

```shell
docker build -f Dockerfile.dev -t core-analytics .
docker run --env-file .env -it core-analytics /bin/bash
```

一回目の起動に失敗したとき

```shell
docker build -f Dockerfile.dev -t core-analytics --no-cache .
```
