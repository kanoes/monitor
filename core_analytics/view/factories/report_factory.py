"""
Factory for creating different types of reports.
"""
from typing import Dict, Any, List
from pathlib import Path
from datetime import datetime
import logging

from azure.monitor.query import LogsQueryResult

from core_analytics.core.interfaces import IReportGenerator
from core_analytics.core.logging_config import ReportGenerationError
import pandas as pd
from core_analytics.view.excel_utils import generate_user_count_excel, generate_stroke_count_excel, generate_stroke_count_summary_excel, add_bar_graph_to_stroke_count_excel, create_line_graph

from core_analytics.core.models import ProcessData

class ExcelReportGenerator(IReportGenerator):
    """Excel report generator implementation."""
    
    def __init__(self):
        self.logger = logging.getLogger("CoreAnalytics")
    
    def generate_report(self, data: LogsQueryResult, filepath: str, report_type: str) -> None:
        """Generate an Excel report from log data."""
        try:
            if report_type == "user_count":
                generate_user_count_excel(data, filepath)
            elif report_type == "stroke_count":
                generate_stroke_count_excel(data, filepath)
            else:
                raise ReportGenerationError(f"Unknown report type: {report_type}")
                
            self.logger.info(f"Generated {report_type} report: {filepath}")
            
        except Exception as e:
            self.logger.error(f"Failed to generate {report_type} report at {filepath}: {e}")
            raise ReportGenerationError(f"Failed to generate report: {e}")

class ReportFactory:
    """Factory for creating and managing reports."""
    
    def __init__(self):
        self.logger = logging.getLogger("CoreAnalytics")
        self.excel_generator = ExcelReportGenerator()
    
    def generate_user_count_reports(self, 
                                  processed_data: ProcessData, 
                                  output_dir: Path, 
                                  end_time: datetime) -> List[str]:
        """Generate all user count reports."""
        generated_files = []
        user_count_results = processed_data.user_count_results

        self.logger.info(f"Generating {len(user_count_results)} user count reports")
        
        for query_key, result in user_count_results.items():
            try:
                filepath = output_dir / f"{query_key}_{end_time.strftime('%Y%m%d')}.xlsx"
                self.excel_generator.generate_report(
                    data=result["data"],
                    filepath=str(filepath),
                    report_type="user_count"
                )
                generated_files.append(str(filepath))
                
            except Exception as e:
                self.logger.error(f"Failed to generate user count report for {query_key}: {e}")
                continue
        
        return generated_files
    
    def generate_stroke_count_reports(self, 
                                    processed_data: ProcessData, 
                                    output_dir: Path, 
                                    end_time: datetime) -> List[str]:
        """Generate all stroke count reports."""
        generated_files = []
        stroke_count_results = processed_data.stroke_count_results
        
        self.logger.info(f"Generating {len(stroke_count_results)} stroke count reports")
        
        for query_key, result in stroke_count_results.items():
            try:
                filepath = output_dir / f"{query_key}_{end_time.strftime('%Y%m%d')}.xlsx"
                self.excel_generator.generate_report(
                    data=result["data"],
                    filepath=str(filepath),
                    report_type="stroke_count"
                )
                generated_files.append(str(filepath))
                
            except Exception as e:
                self.logger.error(f"Failed to generate stroke count report for {query_key}: {e}")
                continue
        
        return generated_files
    
    def generate_stroke_count_summary(self, 
                                  processed_data: ProcessData, 
                                  stroke_count_dir: Path, 
                                  end_time: datetime) -> List[str]:
        """Update the stroke count summary Excel file."""
        stroke_count_results = processed_data.stroke_count_results
        
        self.logger.info(f"Updating stroke count summary for {len(stroke_count_results)} queries")
        
        for query_key, result in stroke_count_results.items():
            try:
                filepath = stroke_count_dir / f"{query_key}_{end_time.strftime('%Y%m%d')}.xlsx"
                
                from core_analytics.view.excel_utils import _read_stroke_count_excel_to_df
                df = _read_stroke_count_excel_to_df(sheet_name="打鍵数", filepath=filepath)
                generate_stroke_count_summary_excel(
                    datas=df,
                    filepath=str(stroke_count_dir),
                    sheet_name=query_key
                )
            except Exception as e:
                self.logger.error(f"Failed to update stroke count summary for {query_key}: {e}")
                continue
        
        # add_bar_graph_to_stroke_count_excel(
        #             folder_path=str(stroke_count_dir)
        #         )

        stroke_count_summary_files = create_line_graph(
            folder_path=str(stroke_count_dir)
        )
        return stroke_count_summary_files
    
    def generate_all_reports(self, 
                           processed_data: ProcessData, 
                           output_dir: str, 
                           end_time: datetime) -> List[str]:
        """Generate all reports from processed data."""
        self.logger.info("Starting comprehensive report generation")
        
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        stroke_count_path = Path("output/stroke_count")
        stroke_count_path.mkdir(parents=True, exist_ok=True)
        
        all_generated_files = []
        
        try:
            # Generate user count reports
            user_count_files = self.generate_user_count_reports(processed_data, output_path, end_time)
            all_generated_files.extend(user_count_files)
            
            # Generate stroke count reports
            stroke_count_files = self.generate_stroke_count_reports(processed_data, output_path, end_time)
            all_generated_files.extend(stroke_count_files)
            
            # Generate stroke count summary
            stroke_count_summary_files = self.generate_stroke_count_summary(processed_data, output_path, end_time)
            all_generated_files.extend(stroke_count_summary_files)
            
            self.logger.info(f"Successfully generated {len(all_generated_files)} report files")
            
        except Exception as e:
            self.logger.error(f"Failed during report generation: {e}")
            raise ReportGenerationError(f"Report generation failed: {e}")
        
        return all_generated_files
