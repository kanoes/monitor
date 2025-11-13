from model.repositories.azure_log_repository import AzureLogRepository
from view.excel_utils import generate_stroke_count_excel
from config.settings import ConfigurationService
from services.analytics_service import AnalyticsService
from services.query_strategies.strategy_factory import QueryStrategyFactory
import unittest
import datetime
from zoneinfo import ZoneInfo

class TestFetchAndProcessData(unittest.TestCase):

    def setUp(self):
        """テスト実行前の初期化処理"""
        days_range = 30

        # Calculate time range
        jst = ZoneInfo("Asia/Tokyo")
        end_jst = datetime.datetime.now(jst)
        start_jst = end_jst - datetime.timedelta(days=days_range)
        self.end_time = end_jst.astimezone(datetime.UTC)
        self.start_time = start_jst.astimezone(datetime.UTC)

        # Initialize dependencies
        self.config_service = ConfigurationService(days_range=days_range)
        self.azure_log_repository = AzureLogRepository(self.config_service)
        self.strategy_factory = QueryStrategyFactory()
        self.analytics_service = AnalyticsService(
            self.azure_log_repository, 
            self.config_service, 
            self.strategy_factory
        )
    
                
    def review_the_query_result(self):
        self.setUp()
        processed_data = self.analytics_service.fetch_and_process_data(
                self.start_time, 
                self.end_time
            )

        return processed_data

def main():
    
    # テストクラスのインスタンスを作成
    test_instance = TestFetchAndProcessData()
    processed_data = test_instance.review_the_query_result()
    print(processed_data)


if __name__ == "__main__":
    main()