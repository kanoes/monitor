import os
import logging
from typing import Dict, List, Optional
import requests
from azure.identity import DefaultAzureCredential

class AzureCostService:
    def __init__(self):
        self.logger = logging.getLogger("CoreAnalytics")
        self.credential = DefaultAzureCredential()
        self._token: Optional[str] = None

    def _get_token(self) -> str:
        if self._token:
            return self._token
        token = self.credential.get_token("https://management.azure.com/.default")
        self._token = token.token
        return self._token

    def _post_query(self, scope: str, payload: Dict) -> Dict:
        url = f"https://management.azure.com{scope}/providers/Microsoft.CostManagement/query?api-version=2023-03-01"
        headers = {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json"
        }
        resp = requests.post(url, json=payload, headers=headers, timeout=60)
        if resp.status_code >= 400:
            raise RuntimeError(f"Cost query failed: {resp.status_code} {resp.text}")
        return resp.json()

    def query_mtd_cost(self,
                       scope: str,
                       dimension_name: Optional[str] = None,
                       dimension_values: Optional[List[str]] = None,
                       tag_name: Optional[str] = None,
                       tag_values: Optional[List[str]] = None) -> float:
        dataset_filter: Dict = {}
        if tag_name and tag_values:
            dataset_filter = {"tags": {"name": tag_name, "operator": "In", "values": tag_values}}
        elif dimension_name and dimension_values:
            dataset_filter = {"dimensions": {"name": dimension_name, "operator": "In", "values": dimension_values}}

        payload = {
            "type": "ActualCost",
            "timeframe": "MonthToDate",
            "dataset": {
                "aggregation": {"totalCost": {"name": "PreTaxCost", "function": "Sum"}}
            }
        }
        if dataset_filter:
            payload["dataset"]["filter"] = dataset_filter

        data = self._post_query(scope, payload)
        rows = data.get("properties", {}).get("rows", [])
        if rows and len(rows[0]) > 0:
            return float(rows[0][0])
        return 0.0

    def get_apps_mtd_costs(self, prefixes: List[str] = None) -> Optional[Dict[str, float]]:
        prefixes = prefixes or ["APP1", "APP2", "APP3", "APP4"]

        subscription_id = os.environ.get("AZURE_SUBSCRIPTION_ID")
        if not subscription_id:
            self.logger.warning("AZURE_SUBSCRIPTION_ID is not set; skipping cost query")
            return None
        scope = f"/subscriptions/{subscription_id}"

        def build_filter(prefix: str) -> Dict:
            tag_name = os.environ.get(f"{prefix}_COST_TAG_NAME")
            tag_values = os.environ.get(f"{prefix}_COST_TAG_VALUES")
            dim_name = os.environ.get(f"{prefix}_COST_DIMENSION_NAME")
            dim_values = os.environ.get(f"{prefix}_COST_DIMENSION_VALUES")
            rg = os.environ.get(f"{prefix}_RESOURCE_GROUP")

            if tag_name and tag_values:
                return {"tag_name": tag_name, "tag_values": [v.strip() for v in tag_values.split(",") if v.strip()]}
            if dim_name and dim_values:
                return {"dimension_name": dim_name, "dimension_values": [v.strip() for v in dim_values.split(",") if v.strip()]}
            if rg:
                return {"dimension_name": "ResourceGroupName", "dimension_values": [rg]}
            return {}

        costs: Dict[str, float] = {}
        for p in prefixes:
            app_name = os.environ.get(f"{p}_NAME")
            if not app_name:
                continue
            f = build_filter(p)
            try:
                costs[app_name] = self.query_mtd_cost(scope, **f) if f else 0.0
            except Exception as e:
                self.logger.error(f"Failed to query cost for {app_name}: {e}")
                costs[app_name] = 0.0
        return costs
