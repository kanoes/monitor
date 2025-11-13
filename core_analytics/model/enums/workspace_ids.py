from enum import Enum
import os
import yaml


# 環境名を環境変数や引数で指定（デフォルトはdev）
env = os.environ.get("ENV")
print("#"*100)
print(env)

# YAMLファイルを読み込む
with open("./config/config.yaml", "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

class WorkspaceIds(Enum):
    ALM_WORKSPACE_ID = os.environ.get("ALM_WORKSPACE_ID")
    DOC_WORKSPACE_ID = os.environ.get("DOC_WORKSPACE_ID")
    BRAIN_WORKSPACE_ID = os.environ.get("BRAIN_WORKSPACE_ID")
    MA_BOT_WORKSPACE_ID = os.environ.get("MA_BOT_WORKSPACE_ID")
    MA_WEB_WORKSPACE_ID = os.environ.get("MA_WEB_WORKSPACE_ID")
    CA_WORKSPACE_ID = os.environ.get("CA_WORKSPACE_ID")

if __name__ == "__main__":
    print(WorkspaceIds.ALM_WORKSPACE_ID.value)
    print(WorkspaceIds.DOC_WORKSPACE_ID.value)
    print(WorkspaceIds.BRAIN_WORKSPACE_ID.value)
    print(WorkspaceIds.MA_BOT_WORKSPACE_ID.value)
    print(WorkspaceIds.MA_WEB_WORKSPACE_ID.value)
    print(WorkspaceIds.CA_WORKSPACE_ID.value)
