import os
from typing import Any, Dict, List, Optional, Union
import pandas as pd
import xlwings as xw
from datetime import datetime
from pathlib import Path

class ExcelManager:
    """Manage Excel workbook operations."""
    
    def __init__(self, workbook_path: str):
        """Initialize with workbook path."""
        self.workbook_path = workbook_path
        self._ensure_workbook_exists()
        
    def _ensure_workbook_exists(self):
        """Create workbook if it doesn't exist."""
        if not os.path.exists(self.workbook_path):
            wb = xw.Book()
            wb.save(self.workbook_path)
            wb.close()
            
    def _get_sheet(self, sheet_name: str, create_if_missing: bool = True) -> xw.Sheet:
        """Get or create worksheet."""
        wb = xw.Book(self.workbook_path)
        try:
            sheet = wb.sheets[sheet_name]
        except:
            if create_if_missing:
                sheet = wb.sheets.add(sheet_name)
            else:
                raise ValueError(f"Sheet {sheet_name} not found")
        return sheet
        
    def write_dataframe(
        self,
        df: pd.DataFrame,
        sheet_name: str,
        start_cell: str = "A1",
        include_index: bool = False,
        include_header: bool = True,
        clear_existing: bool = True
    ):
        """Write pandas DataFrame to Excel."""
        sheet = self._get_sheet(sheet_name)
        if clear_existing:
            sheet.clear_contents()
            
        sheet.range(start_cell).options(
            pd.DataFrame,
            index=include_index,
            header=include_header
        ).value = df
        
    def read_dataframe(
        self,
        sheet_name: str,
        start_cell: str = "A1",
        include_index: bool = False,
        include_header: bool = True
    ) -> pd.DataFrame:
        """Read pandas DataFrame from Excel."""
        sheet = self._get_sheet(sheet_name, create_if_missing=False)
        return sheet.range(start_cell).options(
            pd.DataFrame,
            index=include_index,
            header=include_header,
            expand='table'
        ).value
        
    def write_positions(
        self,
        positions: List[Dict],
        sheet_name: str = "Positions"
    ):
        """Write positions data to Excel."""
        sheet = self._get_sheet(sheet_name)
        sheet.clear_contents()
        
        # Headers
        headers = [
            "Symbol", "Product", "Quantity", "Average Price",
            "Last Price", "P&L", "Day P&L"
        ]
        sheet.range("A1").value = headers
        
        # Data
        rows = []
        for pos in positions:
            rows.append([
                pos.get("tradingsymbol", ""),
                pos.get("product", ""),
                pos.get("quantity", 0),
                pos.get("average_price", 0),
                pos.get("last_price", 0),
                pos.get("pnl", 0),
                pos.get("day_pnl", 0)
            ])
            
        if rows:
            sheet.range("A2").value = rows
            
        # Format
        self._format_sheet(sheet)
        
    def write_orders(
        self,
        orders: List[Dict],
        sheet_name: str = "Orders"
    ):
        """Write orders data to Excel."""
        sheet = self._get_sheet(sheet_name)
        sheet.clear_contents()
        
        # Headers
        headers = [
            "Order ID", "Symbol", "Type", "Side", "Product",
            "Quantity", "Price", "Status", "Time"
        ]
        sheet.range("A1").value = headers
        
        # Data
        rows = []
        for order in orders:
            rows.append([
                order.get("order_id", ""),
                order.get("tradingsymbol", ""),
                order.get("order_type", ""),
                order.get("transaction_type", ""),
                order.get("product", ""),
                order.get("quantity", 0),
                order.get("price", 0),
                order.get("status", ""),
                order.get("order_timestamp", "")
            ])
            
        if rows:
            sheet.range("A2").value = rows
            
        # Format
        self._format_sheet(sheet)
        
    def write_trades(
        self,
        trades: List[Dict],
        sheet_name: str = "Trades"
    ):
        """Write trades data to Excel."""
        sheet = self._get_sheet(sheet_name)
        sheet.clear_contents()
        
        # Headers
        headers = [
            "Trade ID", "Order ID", "Symbol", "Side",
            "Quantity", "Price", "Time"
        ]
        sheet.range("A1").value = headers
        
        # Data
        rows = []
        for trade in trades:
            rows.append([
                trade.get("trade_id", ""),
                trade.get("order_id", ""),
                trade.get("tradingsymbol", ""),
                trade.get("transaction_type", ""),
                trade.get("quantity", 0),
                trade.get("price", 0),
                trade.get("fill_timestamp", "")
            ])
            
        if rows:
            sheet.range("A2").value = rows
            
        # Format
        self._format_sheet(sheet)
        
    def _format_sheet(self, sheet: xw.Sheet):
        """Apply standard formatting to sheet."""
        data_range = sheet.range("A1").expand('table')
        data_range.api.Borders.Weight = 2
        header_range = sheet.range("A1").expand('right')
        header_range.api.Font.Bold = True
        sheet.autofit()
        
    def save(self):
        """Save workbook."""
        wb = xw.Book(self.workbook_path)
        wb.save()
        
    def close(self):
        """Close workbook."""
        wb = xw.Book(self.workbook_path)
        wb.close()
        
    def __enter__(self):
        """Context manager entry."""
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.save()
        self.close() 