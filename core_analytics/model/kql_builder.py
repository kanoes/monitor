from jinja2 import Template
import yaml
from pathlib import Path
from typing import Dict, Any
import pathlib
import os

def load_kql_templates() -> dict:
    """KQLテンプレートをYAMLファイルから読み込み"""
    # template_path = Path(__file__).parent.parent.parent / "config" / "kql_templates.yaml"
    template_path = Path("./config/kql_templates.yaml")
    
    if not template_path.exists():
        raise FileNotFoundError(f"テンプレートファイルが見つかりません: {template_path}")
    
    with open(template_path, "r", encoding="utf-8") as f:
        templates = yaml.safe_load(f)
    
    return templates


def build_kql(query_type: str, contains_keyword: str="", startswith_keyword: str="") -> str:
    
    # テンプレートを読み込み
    templates = load_kql_templates()
    
    if query_type not in templates:
        available_types = list(templates.keys())
        raise ValueError(f"未知のクエリタイプ: {query_type}. 利用可能: {available_types}")
    
    # テンプレートを取得
    template_str: str = templates[query_type].get(f"template_{os.getenv('TEMPLATE_TYPE')}")

    template = Template(template_str)
    
    # パラメータを準備
    params = {}
    if contains_keyword:
        params["contains_keyword"] = contains_keyword
    if startswith_keyword:
        params["startswith_keyword"] = startswith_keyword
    
    # パラメータの妥当性をチェック
    if not params:
        raise ValueError("contains_keyword または startswith_keyword のいずれかが必要です")
    
    # テンプレートをレンダリング
    query = template.render(**params)

    return query.strip()
