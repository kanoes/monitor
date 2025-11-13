from typing import Dict, Any
from azure.monitor.query import LogsQueryResult
from model.kql_builder import build_kql

class LogQueryModel:
    """Model層: 純粋なデータ取得・加工を担当"""
    
    def __init__(self, logs_query_client):
        self.logs_query_client = logs_query_client
    
    def fetch_logs(self, queries_config: dict, workspace_mapping: dict, start_time, end_time) -> dict[str, LogsQueryResult]:
        """
        純粋なデータ取得処理
        Controller層から設定を受け取り、データを返す
        """
        results = {}
        for key, query_config in queries_config.items():
            # クエリを構築
            query = build_kql(
                query_type=query_config["query_type"],
                contains_keyword=query_config.get("contains_keyword"),
                startswith_keyword=query_config.get("startswith_keyword")
            )
            
            # ワークスペースIDを取得（Controller層から渡される）
            workspace_id = workspace_mapping[query_config["workspace"]]
            
            # クエリを実行（正しいメソッド名を使用）
            results[key] = self.logs_query_client.query_workspace(
                workspace_id=workspace_id,
                query=query,
                timespan=(start_time, end_time)
            )
        
        return results
    
    def validate_log_data(self, log_results: dict[str, LogsQueryResult]) -> bool:

        #LogsQueryResultのオブジェクトでtablesプロパティが存在するか確認する
        for key, result in log_results.items():
            if not result or not hasattr(result, 'tables'):
                print(f"警告: {key}のデータが不正です")
                return False
        return True
    
