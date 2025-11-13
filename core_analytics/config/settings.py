"""
Centralized configuration management for Core Analytics application.
"""
import os
import yaml
from pathlib import Path
from typing import Dict, Any
from dataclasses import dataclass


@dataclass
class WorkspaceConfig:
    """Configuration for a single workspace."""
    workspace_id: str
    name: str
    
@dataclass
class QueryConfig:
    """Configuration for a single query."""
    query_type: str
    contains_keyword: str = ""
    startswith_keyword: str = ""
    workspace: str = ""

@dataclass
class AppSettings:
    """Main application settings."""
    query_days_range: int = 30
    output_base_dir: str = "output"
    stroke_count_dir: str = "output/stroke_count"
    blob_container_name: str = os.environ.get("AZURE_BLOB_CONTAINER_NAME")


class ConfigurationService:
    """Centralized configuration service."""
    
    def __init__(self, config_file_path: str = "./config/config.yaml", days_range: int = 30):
        self.config_file_path = config_file_path
        self._workspaces = {}
        self._queries = {}
        self._app_settings = AppSettings(days_range)
        self._load_configurations()
    
    def _load_configurations(self) -> None:
        """Load all configurations from various sources."""
        self._load_workspace_configs()
        self._load_query_configs()
    
    def _load_workspace_configs(self) -> None:
        """Load workspace configurations from environment variables."""

        workspace_mappings = {
            "doc": ("DOC_WORKSPACE_ID", "Document Search"),
            "alm": ("ALM_WORKSPACE_ID", "ALM Chat"),
            "brain": ("BRAIN_WORKSPACE_ID", "Brain Chat"),
            "ma_bot": ("MA_BOT_WORKSPACE_ID", "Market Analysis Bot"),
            "ma_web": ("MA_WEB_WORKSPACE_ID", "Market Analysis Web"),
            "ca": ("CA_WORKSPACE_ID", "Company Analysis"),
            "daily_alm": ("DAILY_ALM_WORKSPACE_ID", "Daily ALM Monitor"),
            "daily_doc": ("DAILY_DOC_WORKSPACE_ID", "Daily Doc Monitor"),
            "daily_ma_web": ("DAILY_MA_WEB_WORKSPACE_ID", "Daily MA Web Monitor"),
            "daily_ma_bot": ("DAILY_MA_BOT_WORKSPACE_ID", "Daily MA Bot Monitor"),
            "daily_ca": ("DAILY_CA_WORKSPACE_ID", "Daily CA Monitor"),
            "daily_brain": ("DAILY_BRAIN_WORKSPACE_ID", "Daily Brain Monitor"),
            "daily_doc_k8s": ("DAILY_DOC_K8S_WORKSPACE_ID", "Daily Doc K8s Monitor"),
        }
        
        for key, (env_var, name) in workspace_mappings.items():
            workspace_id = os.environ.get(env_var)
            if workspace_id:
                self._workspaces[key] = WorkspaceConfig(
                    workspace_id=workspace_id,
                    name=name
                )
    
    def _load_query_configs(self) -> None:
        """Load query configurations from YAML file."""
        if not Path(self.config_file_path).exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_file_path}")
        
        with open(self.config_file_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        
        queries_config = config.get("queries", {})
        for key, query_data in queries_config.items():
            self._queries[key] = QueryConfig(
                query_type=query_data["query_type"],
                contains_keyword=query_data.get("contains_keyword", ""),
                startswith_keyword=query_data.get("startswith_keyword", ""),
                workspace=query_data["workspace"]
            )
    
    def get_workspace_config(self, workspace_key: str) -> WorkspaceConfig:
        """Get workspace configuration by key."""
        if workspace_key not in self._workspaces:
            raise ValueError(f"Workspace configuration not found: {workspace_key}")
        return self._workspaces[workspace_key]
    
    def get_query_config(self, query_key: str) -> QueryConfig:
        """Get query configuration by key."""
        if query_key not in self._queries:
            raise ValueError(f"Query configuration not found: {query_key}")
        return self._queries[query_key]
    
    def get_all_query_configs(self) -> Dict[str, QueryConfig]:
        """Get all query configurations."""
        return self._queries.copy()
    
    def get_workspace_id(self, workspace_key: str) -> str:
        """Get workspace ID by key."""
        return self.get_workspace_config(workspace_key).workspace_id
    
    def get_app_settings(self) -> AppSettings:
        """Get application settings."""
        return self._app_settings
    
    def is_user_count_query(self, query_key: str) -> bool:
        """Check if query is for user count."""
        return "user_count" in query_key
    
    def is_stroke_count_query(self, query_key: str) -> bool:
        """Check if query is for stroke count."""
        return "stroke_count" in query_key
    
    def get_query_configs_by_group(self, group_name: str) -> Dict[str, QueryConfig]:
        """Get query configurations by group name."""
        if not Path(self.config_file_path).exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_file_path}")
        
        with open(self.config_file_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        
        group_queries = config.get(group_name, {})
        result = {}
        
        for key, query_data in group_queries.items():
            result[key] = QueryConfig(
                query_type=query_data["query_type"],
                contains_keyword=query_data.get("contains_keyword", ""),
                startswith_keyword=query_data.get("startswith_keyword", ""),
                workspace=query_data["workspace"]
            )
        
        return result
    
    def get_enabled_query_configs(self) -> Dict[str, QueryConfig]:
        """Get enabled query configurations."""
        query_group = os.environ.get("QUERY_GROUP", "daily_monitor_queries")

        if query_group == "all":
            return self.get_all_query_configs()
        else:
            return self.get_query_configs_by_group(query_group)
