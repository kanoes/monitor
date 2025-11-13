import os
import sys
import datetime
from zoneinfo import ZoneInfo
import logging
from pathlib import Path
from typing import Dict, Any
import json

import dotenv
dotenv.load_dotenv()

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from core_analytics.config.settings import ConfigurationService
from core_analytics.model.repositories.azure_log_repository import AzureLogRepository
from core_analytics.services.analytics_service import AnalyticsService
from core_analytics.services.query_strategies.strategy_factory import QueryStrategyFactory
from core_analytics.view.factories.daily_monitor_factory import DailyMonitorFactory
from core_analytics.core.logging_config import LoggerSetup
from core_analytics.core.models import ProcessData
from core_analytics.services.email_service import EmailService
from core_analytics.services.cost_service import AzureCostService

class DailyMonitorQueryTester:
    """日次監視用クエリのテスター"""
    
    def __init__(self):
        # ログ設定
        self.logger = LoggerSetup.setup_logger()
        self.logger.setLevel(logging.INFO)
        
        # 時間範囲設定（過去1日）- JST ベースで計算し、Azure には UTC を渡す
        jst = ZoneInfo("Asia/Tokyo")
        end_jst = datetime.datetime.now(jst)
        start_jst = end_jst - datetime.timedelta(days=1)
        self.end_time = end_jst.astimezone(datetime.UTC)
        self.start_time = start_jst.astimezone(datetime.UTC)
        
        # サービス初期化
        try:
            self.config_service = ConfigurationService()
            self.log_repository = AzureLogRepository(self.config_service)
            self.strategy_factory = QueryStrategyFactory()
            self.analytics_service = AnalyticsService(
                self.log_repository, 
                self.config_service, 
                self.strategy_factory
            )
            self.daily_monitor_factory = DailyMonitorFactory()
            
            print("✅ サービス初期化成功")
            
        except Exception as e:
            print(f"❌ サービス初期化失敗: {e}")
            raise
    
    def check_environment(self):
        """環境変数の確認"""
        print("\n🔍 環境変数チェック:")
        
        env_vars = {
            "AZURE_CLIENT_ID": os.environ.get("AZURE_CLIENT_ID"),
            "AZURE_CLIENT_SECRET": os.environ.get("AZURE_CLIENT_SECRET"), 
            "AZURE_TENANT_ID": os.environ.get("AZURE_TENANT_ID"),
            "AZURE_SUBSCRIPTION_ID": os.environ.get("AZURE_SUBSCRIPTION_ID"),
            "DAILY_ALM_WORKSPACE_ID": os.environ.get("DAILY_ALM_WORKSPACE_ID"),
            "DAILY_DOC_WORKSPACE_ID": os.environ.get("DAILY_DOC_WORKSPACE_ID"),
            "DAILY_MA_WEB_WORKSPACE_ID": os.environ.get("DAILY_MA_WEB_WORKSPACE_ID"),
            "DAILY_CA_WORKSPACE_ID": os.environ.get("DAILY_CA_WORKSPACE_ID"),
            "TEMPLATE_TYPE": os.environ.get("TEMPLATE_TYPE", "stg")
        }
        
        for var in env_vars:
            value = os.environ.get(var)
            if value:
                print(f"  ✅ {var}: {value[:8]}...")
            else:
                print(f"  ❌ {var}: 未設定")
        
        template_type = os.environ.get("TEMPLATE_TYPE", "stg")
        print(f"\n📋 使用テンプレート: {template_type}")
    
    def test_query_configs(self):
        """クエリ設定のテスト"""
        print("\n📝 日次監視用クエリ設定:")
        
        try:
            # 日次監視用クエリ設定を取得
            daily_queries = self.config_service.get_query_configs_by_group("daily_monitor_queries")
            
            print(f"  📊 設定されたクエリ数: {len(daily_queries)}")
            
            for query_name, query_config in daily_queries.items():
                print(f"    - {query_name}:")
                print(f"      query_type: {query_config.query_type}")
                print(f"      workspace: {query_config.workspace}")
                
                # ワークスペース設定確認
                try:
                    workspace_config = self.config_service.get_workspace_config(query_config.workspace)
                    print(f"      workspace_id: {workspace_config.workspace_id[:8]}...")
                except Exception as e:
                    print(f"      ❌ ワークスペース設定エラー: {e}")
            
            return daily_queries
            
        except Exception as e:
            print(f"❌ クエリ設定取得失敗: {e}")
            return {}
    
    def test_single_query(self, query_name: str, query_config: Any):
        """単一クエリのテスト実行"""
        print(f"\n🔍 クエリテスト: {query_name}")
        
        try:
            # クエリ設定を辞書形式で準備
            test_configs = {query_name: query_config}
            
            # ログ取得実行
            results = self.log_repository.fetch_logs(
                test_configs, 
                self.start_time, 
                self.end_time
            )
            
            if query_name in results:
                result = results[query_name]
                
                if result and hasattr(result, 'tables') and result.tables:
                    table = result.tables[0]
                    row_count = len(table.rows)
                    col_count = len(table.columns)
                    
                    print(f"  ✅ 成功: {row_count}行, {col_count}列")
                    print(f"  📋 カラム: {', '.join(table.columns)}")
                    
                    # 最初の数行を表示
                    if row_count > 0:
                        print(f"  📄 サンプルデータ (最大3行):")
                        for i, row in enumerate(table.rows[:3]):
                            print(f"    行{i+1}: {row}")
                    
                    return result
                else:
                    print(f"  ⚠️  データなし")
                    return None
            else:
                print(f"  ❌ 結果なし")
                return None
                
        except Exception as e:
            print(f"  ❌ エラー: {e}")
            return None
    
    def test_all_queries(self):
        """全クエリのテスト実行"""
        print(f"\n🚀 全日次監視クエリテスト開始")
        print(f"📅 期間: {self.start_time.strftime('%Y-%m-%d %H:%M')} ～ {self.end_time.strftime('%Y-%m-%d %H:%M')}")
        
        # クエリ設定取得
        daily_queries = self.test_query_configs()
        
        if not daily_queries:
            print("❌ テスト対象のクエリがありません")
            return None
        
        # 全クエリ実行
        all_results = {}
        success_count = 0
        
        for query_name, query_config in daily_queries.items():
            result = self.test_single_query(query_name, query_config)
            if result:
                all_results[query_name] = result
                success_count += 1
        
        print(f"\n📊 テスト結果サマリー:")
        print(f"  総クエリ数: {len(daily_queries)}")
        print(f"  成功: {success_count}")
        print(f"  失敗: {len(daily_queries) - success_count}")
        
        return all_results
    
    def test_analytics_service(self):
        """AnalyticsServiceを使った統合テスト"""
        print(f"\n🔧 AnalyticsService統合テスト")
        
        try:
            # 環境変数でクエリグループを日次監視に設定
            os.environ["QUERY_GROUP"] = "daily_monitor_queries"
            
            # データ取得・処理実行
            processed_data = self.analytics_service.fetch_and_process_data(
                self.start_time, 
                self.end_time
            )
            
            print(f"  ✅ データ処理成功")
            print(f"  📊 user_count_results: {len(processed_data.user_count_results)}")
            print(f"  📊 stroke_count_results: {len(processed_data.stroke_count_results)}")
            print(f"  📊 unknown_results: {len(processed_data.unknown_results)}")
            
            # 各結果の詳細表示
            self._display_processed_results("User Count", processed_data.user_count_results)
            self._display_processed_results("Stroke Count", processed_data.stroke_count_results)
            self._display_processed_results("Unknown", processed_data.unknown_results)
            
            return processed_data
            
        except Exception as e:
            print(f"  ❌ エラー: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def _display_processed_results(self, category: str, results: Dict[str, Any]):
        """処理結果の詳細表示"""
        if not results:
            return
            
        print(f"\n  📋 {category} Results:")
        for query_name, result in results.items():
            if result and result.get("data") and result["data"].tables:
                table = result["data"].tables[0]
                row_count = len(table.rows)
                print(f"    - {query_name}: {row_count}行")
            else:
                print(f"    - {query_name}: データなし")
    
    def test_cost_service(self) -> Dict[str, float]:
        """コスト取得のテスト（MTD）"""
        print(f"\n💰 コスト取得テスト")
        try:
            # 事前チェック（任意）
            if not os.environ.get("AZURE_SUBSCRIPTION_ID"):
                print("  ⚠️ AZURE_SUBSCRIPTION_ID が未設定のため、コスト取得をスキップします")
                return {}

            cost_service = AzureCostService()
            mtd_costs = cost_service.get_apps_mtd_costs()

            if mtd_costs:
                print(f"  ✅ コスト取得成功: {len(mtd_costs)}件")
                # 先頭数件だけ表示
                for name, val in list(mtd_costs.items())[:4]:
                    print(f"    - {name}: {val}")
            else:
                print("  ⚠️ コストデータなし（空の結果）")

            return mtd_costs or {}

        except Exception as e:
            print(f"  ❌ コスト取得エラー: {e}")
            import traceback
            traceback.print_exc()
            return {}
    
    def test_daily_monitor_factory(self, processed_data: ProcessData, mtd_costs: Dict[str, float]):
        """DailyMonitorFactoryのテスト"""
        print(f"\n📊 DailyMonitorFactory テスト")
        
        if not processed_data:
            print("  ❌ 処理データがありません")
            return
        
        try:
            # テスト用出力ディレクトリ
            test_output_dir = project_root / "output"
            test_output_dir.mkdir(exist_ok=True)
            
            # Excel生成テスト
            generated_files = self.daily_monitor_factory.generate_daily_monitor_report(
                processed_data, 
                str(test_output_dir), 
                self.end_time,
                mtd_costs
            )
            
            print(f"  ✅ Excel生成成功")
            print(f"  📁 生成ファイル数: {len(generated_files)}")
            
            for file_path in generated_files:
                file_size = Path(file_path).stat().st_size
                print(f"    - {Path(file_path).name}: {file_size:,} bytes")
            
            return generated_files
            
        except Exception as e:
            print(f"  ❌ エラー: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def save_test_results(self, results: Dict[str, Any]):
        """テスト結果をJSONファイルに保存"""
        if not results:
            return
        
        try:
            # テスト結果を辞書形式に変換
            serializable_results = {}
            
            for query_name, result in results.items():
                if result and hasattr(result, 'tables') and result.tables:
                    table = result.tables[0]
                    serializable_results[query_name] = {
                        "columns": table.columns,
                        "row_count": len(table.rows),
                        "sample_rows": table.rows[:5]  # 最初の5行のみ保存
                    }
                else:
                    serializable_results[query_name] = {"error": "No data"}
            
            # JSONファイルに保存
            output_file = project_root / f"daily_monitor_test_results_{self.end_time.strftime('%Y%m%d_%H%M%S')}.json"
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(serializable_results, f, ensure_ascii=False, indent=2, default=str)
            
            print(f"\n💾 テスト結果保存: {output_file}")
            
        except Exception as e:
            print(f"❌ テスト結果保存失敗: {e}")
    
    def run_full_test(self):
        """フルテストの実行"""
        print("🧪 日次監視クエリ フルテスト開始")
        print("=" * 60)
        
        # 1. 環境確認
        self.check_environment()
        
        # 2. 個別クエリテスト
        query_results = self.test_all_queries()
        
        # 3. 統合テスト
        processed_data = self.test_analytics_service()
        
        # 4. Excel生成テスト
        if processed_data:
            mtd_costs = self.test_cost_service()
            excel_files = self.test_daily_monitor_factory(processed_data, mtd_costs)
            
            if excel_files:
                try:
                    email_service = EmailService()
                    date_str = self.end_time.strftime('%Y年%m月%d日')
                    email_sent = email_service.send_daily_monitor_report(excel_files, date_str)
                    if email_sent:
                        print("✅ メール送信成功")
                    else:
                        print("❌ メール送信失敗")
                except Exception as e:
                    print(f"⚠️ メールサービス利用不可: {e}")
        
        # 5. 結果保存
        if query_results:
            self.save_test_results(query_results)
        
        print("\n" + "=" * 60)
        print("🎉 テスト完了!")


def main():
    """メイン関数"""
    try:
        tester = DailyMonitorQueryTester()
        tester.run_full_test()
        
    except KeyboardInterrupt:
        print("\n⚠️ テスト中断")
    except Exception as e:
        print(f"\n❌ テスト失敗: {e}")
        import traceback
        traceback.print_exc()


def test_specific_query(query_name: str):
    """特定のクエリのみをテストする関数"""
    try:
        tester = DailyMonitorQueryTester()
        
        print(f"🎯 単一クエリテスト: {query_name}")
        print("=" * 50)
        
        # 環境確認
        tester.check_environment()
        
        # 指定されたクエリの設定を取得
        daily_queries = tester.config_service.get_query_configs_by_group("daily_monitor_queries")
        
        if query_name not in daily_queries:
            print(f"❌ クエリ '{query_name}' が見つかりません")
            print(f"利用可能なクエリ: {list(daily_queries.keys())}")
            return
        
        query_config = daily_queries[query_name]
        
        # 単一クエリテスト実行
        result = tester.test_single_query(query_name, query_config)
        
        if result:
            print(f"\n✅ '{query_name}' テスト成功!")
            
            # 結果を詳細表示
            if hasattr(result, 'tables') and result.tables:
                table = result.tables[0]
                print(f"\n📊 詳細結果:")
                print(f"  行数: {len(table.rows)}")
                print(f"  列数: {len(table.columns)}")
                print(f"  カラム: {table.columns}")
                
                # 全データ表示（少量の場合）
                if len(table.rows) <= 10:
                    print(f"\n📄 全データ:")
                    for i, row in enumerate(table.rows):
                        print(f"    行{i+1}: {row}")
                else:
                    print(f"\n📄 サンプルデータ (最初の5行):")
                    for i, row in enumerate(table.rows[:5]):
                        print(f"    行{i+1}: {row}")
        else:
            print(f"\n❌ '{query_name}' テスト失敗")
        
    except Exception as e:
        print(f"❌ テストエラー: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # ========== テスト設定 ==========
    # 🎯 単一クエリをテストしたい場合は、以下の変数にクエリ名を設定してください
    # 利用可能なクエリ名:
    # ┌─ ALM関連 ─────────────────────────────────────┐
    # │ "daily_alm_chat_count"          - ALMチャット使用人数      │
    # │ "daily_alm_chat_history"        - ALMチャット使用履歴      │
    # │ "daily_alm_dashboard_count"     - ALMダッシュボード使用人数  │
    # │ "daily_alm_dashboard_history"   - ALMダッシュボード使用履歴  │
    # └──────────────────────────────────────────────┘
    # ┌─ Document Search関連 ──────────────────────────┐
    # │ "daily_doc_search_count"        - 文書検索使用人数        │
    # │ "daily_doc_search_history"      - 文書検索使用履歴        │
    # └──────────────────────────────────────────────┘
    # ┌─ MyAssistant関連 ─────────────────────────────┐
    # │ "daily_my_assistant_search_count"   - MyAssistant検索使用人数  │
    # │ "daily_my_assistant_search_history" - MyAssistant検索使用履歴  │
    # │ "daily_my_assistant_upload_count"   - MyAssistantアップロード使用人数│
    # │ "daily_my_assistant_upload_history" - MyAssistantアップロード使用履歴│
    # └──────────────────────────────────────────────┘
    # ┌─ Market Report関連 ────────────────────────────┐
    # │ "daily_market_report_web_count"     - マーケットレポートWeb使用人数│
    # │ "daily_market_report_web_history"   - マーケットレポートWeb使用履歴│
    # └──────────────────────────────────────────────┘
    # ┌─ Company Analysis関連 ─────────────────────────┐
    # │ "daily_company_analyze_count"    - 会社分析使用人数       │
    # │ "daily_company_analyze_history"  - 会社分析使用履歴       │
    # └──────────────────────────────────────────────┘


    # 全テストを実行したい場合は None に設定してください
    TEST_SINGLE_QUERY = None
    
    # ===============================
    
    if TEST_SINGLE_QUERY:
        print(f"🎯 単一クエリテストモード: {TEST_SINGLE_QUERY}")
        test_specific_query(TEST_SINGLE_QUERY)
    else:
        print("📊 全テスト実行モード")
        main()
