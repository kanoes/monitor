import unittest
from unittest.mock import patch
from view.excel_utils import _create_excel_from_LogsQueryResult
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

class TestCreateExcelFromLogsQueryResult(unittest.TestCase):

    @patch("view.excel_utils._create_excel_from_LogsQueryResult")
    def test_create_excel_from_LogsQueryResult(
        self, 
        mock_create_excel_from_LogsQueryResult
        ):

        mock_create_excel_from_LogsQueryResult.return_value = MockLogsQueryResult
        _create_excel_from_LogsQueryResult(MockLogsQueryResult, "output/test.xlsx", "result")

if __name__ == "__main__":
    print("#"*100)
    unittest.main() 