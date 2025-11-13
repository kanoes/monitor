import unittest
from unittest.mock import patch
from view.excel_utils import generate_user_count_excel
from azure.monitor.query import LogsQueryResult
import pandas as pd


class MockTable:
    def __init__(self, columns, rows):
        self.columns = columns
        self.rows = rows

class MockLogsQueryResult:
    def __init__(self, tables):
        self.tables = tables

# 例のデータ
columns = ["DisplayName", "Department"]
rows = [
    ["辛 ジャスティン", "市統E"],
    ["テスト ユーザー6", "市統E"]
]

MockLogsQueryResult = MockLogsQueryResult([MockTable(columns, rows)])
mock_df = pd.DataFrame(rows, columns)

class TestGenerateDailyReport(unittest.TestCase):

    @patch("view.excel_utils._read_excel_to_df")
    @patch("view.excel_utils._create_excel_from_LogsQueryResult")
    def test_generate_user_count_excel(
        self, 
        mock_create_excel_from_LogsQueryResult,
        mock_read_excel_to_df
        ):

        mock_create_excel_from_LogsQueryResult.return_value = MockLogsQueryResult
        mock_read_excel_to_df.return_value = (mock_df, "Department")
        generate_user_count_excel(MockLogsQueryResult, "output/test.xlsx")

if __name__ == "__main__":
    print("#"*100)
    unittest.main() 