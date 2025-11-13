import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import logging

import dotenv
dotenv.load_dotenv()

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def test_azure_auth():
    """Azure認証のテスト"""
    print("🔐 Azure認証テスト")
    
    try:
        from azure.identity import DefaultAzureCredential
        from azure.monitor.query import LogsQueryClient
        
        # 認証情報取得
        credential = DefaultAzureCredential()
        
        # トークン取得テスト
        token = credential.get_token("https://api.loganalytics.io/.default")
        
        print("  ✅ 認証成功")
        print(f"  🎫 トークン取得: {token.token[:20]}...")
        
        # LogsQueryClient初期化
        client = LogsQueryClient(credential)
        print("  ✅ LogsQueryClient初期化成功")
        
        return client
        
    except Exception as e:
        print(f"  ❌ 認証失敗: {e}")
        return None

def test_workspace_access(client, workspace_id: str, workspace_name: str):
    """Workspaceアクセステスト"""
    print(f"\n🏢 Workspace接続テスト: {workspace_name}")
    
    if not client:
        print("  ❌ クライアントが初期化されていません")
        return False
    
    if not workspace_id:
        print("  ❌ Workspace IDが設定されていません")
        return False
    
    try:
        # 簡単なテストクエリ
        test_query = "AppServiceConsoleLogs | take 1"
        
        jst = ZoneInfo("Asia/Tokyo")
        end_jst = datetime.now(jst)
        start_jst = end_jst - timedelta(hours=1)
        end_time = end_jst.astimezone(ZoneInfo("UTC"))
        start_time = start_jst.astimezone(ZoneInfo("UTC"))
        
        result = client.query_workspace(
            workspace_id, 
            test_query, 
            timespan=(start_time, end_time)
        )
        
        print(f"  ✅ 接続成功 (Workspace ID: {workspace_id[:8]}...)")
        
        if result.tables and len(result.tables) > 0:
            table = result.tables[0]
            print(f"  📊 データ取得成功: {len(table.rows)}行, {len(table.columns)}列")
        else:
            print("  ⚠️  データは空ですが、接続は成功")
        
        return True
        
    except Exception as e:
        print(f"  ❌ 接続失敗: {e}")
        return False

def test_daily_monitor_query(client, workspace_id: str, query_name: str):
    """日次監視クエリのテスト"""
    print(f"\n📊 日次監視クエリテスト: {query_name}")
    
    if not client or not workspace_id:
        print("  ❌ クライアントまたはWorkspace IDが無効")
        return False
    
    # 簡単な日次監視風クエリ
    test_queries = {
        "alm_chat": """
            AppServiceConsoleLogs
            | where ResultDescription startswith "{\\"extendedUser"
            | take 5
        """,
        "doc_search": """
            AppServiceConsoleLogs
            | where ResultDescription contains "{\\"general_search_qa"
            | take 5
        """,
        "basic": """
            AppServiceConsoleLogs
            | where TimeGenerated >= ago(1h)
            | take 5
        """
    }
    
    query = test_queries.get("basic", test_queries["basic"])
    
    try:
        jst = ZoneInfo("Asia/Tokyo")
        end_jst = datetime.now(jst)
        start_jst = end_jst - timedelta(hours=24)
        end_time = end_jst.astimezone(ZoneInfo("UTC"))
        start_time = start_jst.astimezone(ZoneInfo("UTC"))
        
        result = client.query_workspace(
            workspace_id, 
            query, 
            timespan=(start_time, end_time)
        )
        
        if result.tables and len(result.tables) > 0:
            table = result.tables[0]
            row_count = len(table.rows)
            col_count = len(table.columns)
            
            print(f"  ✅ クエリ成功: {row_count}行, {col_count}列")
            print(f"  📋 カラム: {', '.join(table.columns)}")
            
            # サンプルデータ表示
            if row_count > 0:
                print(f"  📄 サンプル (最初の2行):")
                for i, row in enumerate(table.rows[:2]):
                    print(f"    行{i+1}: {row}")
            
            return True
        else:
            print("  ⚠️  クエリは成功しましたが、データがありません")
            return True
            
    except Exception as e:
        print(f"  ❌ クエリ失敗: {e}")
        return False

def main():
    """メイン関数"""
    print("🧪 Azure認証・接続クイックテスト")
    print("=" * 50)
    
    # 環境変数確認
    print("\n📋 環境変数確認:")
    env_vars = {
        "AZURE_CLIENT_ID": os.environ.get("AZURE_CLIENT_ID"),
        "AZURE_CLIENT_SECRET": os.environ.get("AZURE_CLIENT_SECRET"), 
        "AZURE_TENANT_ID": os.environ.get("AZURE_TENANT_ID"),
        "DAILY_ALM_WORKSPACE_ID": os.environ.get("DAILY_ALM_WORKSPACE_ID"),
        "DAILY_DOC_WORKSPACE_ID": os.environ.get("DAILY_DOC_WORKSPACE_ID"),
        "DAILY_MA_WEB_WORKSPACE_ID": os.environ.get("DAILY_MA_WEB_WORKSPACE_ID"),
        "DAILY_CA_WORKSPACE_ID": os.environ.get("DAILY_CA_WORKSPACE_ID"),
        "TEMPLATE_TYPE": os.environ.get("TEMPLATE_TYPE", "stg")
    }
    
    for key, value in env_vars.items():
        if value:
            if "ID" in key and len(value) > 10:
                print(f"  ✅ {key}: {value[:8]}...")
            else:
                print(f"  ✅ {key}: {value}")
        else:
            print(f"  ❌ {key}: 未設定")
    
    # 認証テスト
    client = test_azure_auth()
    
    if not client:
        print("\n❌ 認証に失敗したため、テストを終了します")
        return
    
    # Workspaceテスト
    workspaces = [
        ("DAILY_ALM_WORKSPACE_ID", "ALM"),
        ("DAILY_DOC_WORKSPACE_ID", "Document Search"),
        ("DAILY_MA_WEB_WORKSPACE_ID", "Market Analysis Web"),
        ("DAILY_CA_WORKSPACE_ID", "Company Analysis")
    ]
    
    success_count = 0
    total_count = 0
    
    for env_var, name in workspaces:
        workspace_id = os.environ.get(env_var)
        if workspace_id:
            total_count += 1
            if test_workspace_access(client, workspace_id, name):
                success_count += 1
                
                # 簡単なクエリテスト
                test_daily_monitor_query(client, workspace_id, name)
    
    # 結果サマリー
    print("\n" + "=" * 50)
    print("📊 テスト結果サマリー:")
    print(f"  テスト対象Workspace: {total_count}")
    print(f"  接続成功: {success_count}")
    print(f"  接続失敗: {total_count - success_count}")
    
    if success_count > 0:
        print("\n✅ 少なくとも1つのWorkspaceに接続できました！")
        print("   日次監視クエリの実行が可能です。")
    else:
        print("\n❌ どのWorkspaceにも接続できませんでした。")
        print("   認証設定やWorkspace IDを確認してください。")

if __name__ == "__main__":
    main()
