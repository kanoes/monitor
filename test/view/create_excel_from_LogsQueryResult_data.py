import json
import re
import ast

class MockTable:
    def __init__(self, columns, rows):
        self.columns = columns
        self.rows = rows

class MockLogsQueryResult:
    def __init__(self, tables:MockTable):
        self.tables = tables

def clean_json_string(s):
    # 制御文字を除去またはエスケープ
    s = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', s)
    return s

DocUserColumns = ["DisplayName", "Department"]
DocUserRows = [
    ["テスト ユーザー1", "市統E"],
    ["テスト ユーザー2", "市統E"],
    ["テスト ユーザー3", "市統E"],
    ["テスト ユーザー4", "市統E"],
    ["テスト ユーザー5", "市統E"],
    ["テスト ユーザー6", "市統E"],
    ["テスト ユーザー7", "市統E"],
    ["テスト ユーザー8", "市資"],
]
DocUserContentColumns = ["Timestamp", "DisplayName", "Query", "Answer", "SourceList", "ResultDescription"]
DocUserContentRows = [
    ["25-07-08-23:18", 
     "五藤 陸／天王法／SMBC (Goto Riku)／291DLWeOPBSACgGTPbMLQZ6gwF6hdT0neP5FyhDUtlBZtQhCPflcXTBWPk8XJLTBaPuyckgVKDry86PO7TYLJ7imA', 'query': 'コーラブル\u3000印紙\u3000	",
     "# 回答\n\nコーラブル預金の約定書につきましては、原本には印紙が必要ですが、控えには印紙は不要となります。[1](meta://1)', 'reasoning': '	コーラブル預金の約定書の控えに印紙は必要か",
     "{'core': {'display_name': '五藤 陸／天王法／SMBC (GotoRiku)／291DLWeOPBSACgGTPbMLQZ6gwF6hdT0neP5FyhDUtlBZtQhCPflcXTBWPk8XJLTBaPuyckgVKDry86PO7TYLJ7imA', 'query': 'コーラブル\u3000印紙\u3000', 'status': 'response', 'queries': 'コーラブルとは何か？,印紙についての詳細情報', 'answer': '# 回答\n\nコーラブル預金の約定書につきましては、原本には印紙が必要ですが、控えには印紙は不要となります。[1](meta://1)', 'reasoning': '', 'sourceList': 'コーラブル預金の約定書の控えに印紙は必要か', 'similarResults': 'INVEST(デリバティブ) 簡易マニュアル,金利デリバティブ\u3000組込預金事務フロー（ランク Ⅰ・Ⅱ 編）,金利デリバティブ\u3000組込預金事務フロー（ランク Ⅰ・Ⅱ 編）,金利デリバティブ\u3000組込預金事務フロー（ランク Ⅲ 編）,金利デリバティブ\u3000商品ラインナップ,金利デリバティブ\u3000販売ルール,金利デリバティブ組込預金約定(個別方式),ＩＮＶＥＳＴ（デリバティブ）操作マニュアル,コーラブル預金の説明用資料は INVEST の提案前チェック前に顧客手交可か？,i-Deal オペレーションマニュアル\u3000行内用機能編【営業機密】 第 2 章 預貸金取引約定,金利デリバティブ\u3000金利スワップ系商品事務フロー,第２章 デリバティブ預金の販売・勧誘ルール,金利デリバティブ組込預金期中（解約判定・解約）,外貨コーラブル預金預利税不突合検証マニュアル', 'images': 0, 'filenames': ''}}"]
    
]
# DocUserQueryData = [MockTable(DocUserColumns, DocUserRows)]
# DocUserContentQueryData = [MockTable(DocUserContentColumns, DocUserContentRows)]
# AlmChatQueryData = [MockTable(columns, rows)]
# AlmDashboardQueryData = [MockTable(columns, rows)]

# DocUserQueryResult = MockLogsQueryResult(DocUserQueryData)
# DocUserContentQueryResult = MockLogsQueryResult(DocUserContentQueryData)
# AlmChatQueryResult = MockLogsQueryResult(AlmChatQueryData)
# AlmDashboardQueryResult = MockLogsQueryResult(AlmDashboardQueryData)

if __name__ == "__main__":    
    # 元データが文字列の場合
    clean_str = clean_json_string(DocUserContentRows[0][3])
    json_compatible_str = clean_str.replace("'", '"')
    print(json_compatible_str)
    print(type(json_compatible_str))
    dict_data = json.loads(json_compatible_str)
    print(dict_data.get("core").get("answer"))