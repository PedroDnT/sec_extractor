#!/usr/bin/env python3
"""
Automated SEC Quarterly Financial Data Extractor
Downloads and parses 10-Q and 10-K filings from SEC EDGAR
Extracts Income Statement, Balance Sheet, and Cash Flow Statement
Formats data with quarters as columns (1Q22, 2Q22, 3Q22, etc.)

USAGE:
    python sec_quarterly_extractor_auto.py WMT 2022 2025
    
    Or in Python:
    from sec_quarterly_extractor_auto import extract_quarterly_financials
    extract_quarterly_financials('AAPL', 2022, 2025)
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
import time
import sys
from typing import Dict, List, Optional, Tuple
import json
from io import StringIO

class SECQuarterlyExtractor:
    """
    Automatically download and extract quarterly financial data from SEC filings
    """
    
    def __init__(self, ticker: str, company_name: str = None):
        self.ticker = ticker.upper()
        self.company_name = company_name
        self.cik = None
        
        self.headers = {
            'User-Agent': 'Financial Research educational@research.com',
            'Accept-Encoding': 'gzip, deflate',
        }
        
        # Known CIKs for quick lookup
        self.known_ciks = {
            'WMT': '0000104169',
            'AAPL': '0000320193',
            'MSFT': '0000789019',
            'GOOGL': '0001652044',
            'GOOG': '0001652044',
            'AMZN': '0001018724',
            'TSLA': '0001318605',
            'META': '0001326801',
            'NVDA': '0001045810',
            'JPM': '0000019617',
            'V': '0001403161',
            'MA': '0001141391',
            'JNJ': '0000200406',
            'PG': '0000080424',
            'KO': '0000021344',
            'PEP': '0000077476',
            'NKE': '0000320187',
            'DIS': '0001744489',
            'NFLX': '0001065280',
            'INTC': '0000050863',
        }
        
    def get_cik(self) -> str:
        """Get CIK for the ticker"""
        if self.ticker in self.known_ciks:
            self.cik = self.known_ciks[self.ticker]
            print(f"✅ CIK: {self.cik}")
            return self.cik
        
        print(f"⚠️  CIK not in cache. Please provide CIK manually or add to known_ciks dict.")
        raise ValueError(f"CIK not found for {self.ticker}")
    
    def get_company_facts(self) -> Dict:
        """Get company facts from SEC API - includes financial data"""
        if not self.cik:
            self.get_cik()
        
        print(f"\n📡 Fetching company data from SEC API...")
        
        try:
            # SEC's Company Facts API endpoint
            url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{self.cik}.json"
            response = requests.get(url, headers=self.headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                print(f"✅ Retrieved company facts for {data['entityName']}")
                self.company_name = data['entityName']
                return data
            else:
                print(f"⚠️  Could not fetch company facts (Status: {response.status_code})")
                return None
                
        except Exception as e:
            print(f"⚠️  Error fetching company facts: {e}")
            return None
    
    def get_filings_list(self, start_year: int, end_year: int) -> List[Dict]:
        """Get list of 10-Q and 10-K filings"""
        if not self.cik:
            self.get_cik()
        
        print(f"\n📂 Searching for SEC filings ({start_year}-{end_year})...")
        
        filings = []
        
        try:
            # Get submissions data
            url = f"https://data.sec.gov/submissions/CIK{self.cik}.json"
            response = requests.get(url, headers=self.headers, timeout=30)
            time.sleep(0.2)
            
            if response.status_code == 200:
                data = response.json()
                recent_filings = data['filings']['recent']
                
                # Extract 10-Q and 10-K filings
                for i in range(len(recent_filings['form'])):
                    form = recent_filings['form'][i]
                    
                    if form in ['10-Q', '10-K']:
                        filing_date = recent_filings['filingDate'][i]
                        report_date = recent_filings['reportDate'][i]
                        accession = recent_filings['accessionNumber'][i]
                        primary_doc = recent_filings['primaryDocument'][i]
                        
                        year = int(filing_date.split('-')[0])
                        
                        if start_year <= year <= end_year:
                            filings.append({
                                'form': form,
                                'filing_date': filing_date,
                                'report_date': report_date,
                                'accession_number': accession,
                                'primary_document': primary_doc,
                            })
                
                # Sort by report date
                filings.sort(key=lambda x: x['report_date'])
                
                print(f"✅ Found {len(filings)} filings")
                for filing in filings:
                    print(f"   • {filing['form']} - Report: {filing['report_date']} - Filed: {filing['filing_date']}")
                
                return filings
            else:
                print(f"❌ Could not fetch filings (Status: {response.status_code})")
                return []
                
        except Exception as e:
            print(f"❌ Error: {e}")
            return []
    
    def download_filing(self, accession_number: str, primary_document: str) -> Optional[str]:
        """Download the actual filing HTML"""
        try:
            # Format accession number (remove dashes)
            acc_no = accession_number.replace('-', '')
            
            # Construct URL
            url = f"https://www.sec.gov/Archives/edgar/data/{self.cik}/{acc_no}/{primary_document}"
            
            print(f"      📥 Downloading {primary_document}...")
            response = requests.get(url, headers=self.headers, timeout=30)
            time.sleep(1.0)  # Increased delay to be respectful to SEC servers
            
            if response.status_code == 200:
                print(f"      ✅ Download complete")
                return response.text
            else:
                print(f"      ❌ Download failed (Status: {response.status_code})")
                return None
                
        except Exception as e:
            print(f"      ⚠️  Download error: {e}")
            return None
    
    def extract_financial_table(self, html: str, statement_type: str) -> Optional[pd.DataFrame]:
        """
        Extract financial statement table from HTML
        
        Args:
            html: HTML content of the filing
            statement_type: 'income', 'balance', or 'cashflow'
        """
        soup = BeautifulSoup(html, 'html.parser')
        
        # Keywords to identify each statement type
        keywords = {
            'income': [
                'consolidated statements of income',
                'consolidated statements of operations',
                'consolidated statements of earnings',
                'condensed consolidated statements of income',
                'condensed consolidated statements of operations',
            ],
            'balance': [
                'consolidated balance sheets',
                'condensed consolidated balance sheets',
                'consolidated statements of financial position',
            ],
            'cashflow': [
                'consolidated statements of cash flows',
                'condensed consolidated statements of cash flows',
            ]
        }
        
        # Find all tables
        tables = soup.find_all('table')
        
        for table in tables:
            # Get table text
            table_text = ' '.join(table.get_text().split()).lower()
            
            # Check if this table matches our statement type
            if any(keyword in table_text for keyword in keywords[statement_type]):
                try:
                    # Try to parse with pandas (fix FutureWarning)
                    dfs = pd.read_html(StringIO(str(table)))
                    
                    if dfs and len(dfs) > 0:
                        df = dfs[0]
                        
                        # Basic validation - should have multiple columns
                        if len(df.columns) >= 2:
                            return df
                            
                except Exception as e:
                    continue
        
        return None
    
    def extract_period_label(self, filing: Dict) -> str:
        """Generate period label like 1Q22, 2Q22, etc."""
        report_date = filing['report_date']
        year = int(report_date[:4])
        month = int(report_date[5:7])
        
        # Determine quarter based on month
        # For most companies: Q1=Mar, Q2=Jun, Q3=Sep, Q4=Dec
        # For Walmart: Q1=Apr, Q2=Jul, Q3=Oct, Q4=Jan
        
        quarter_map = {
            1: 4, 2: 4, 3: 1, 4: 1, 5: 1, 6: 2,
            7: 2, 8: 2, 9: 3, 10: 3, 11: 3, 12: 4
        }
        
        quarter = quarter_map[month]
        
        # For fiscal year: if Q4 and month is 1-2, it's previous calendar year
        fiscal_year = year % 100
        if month <= 2:
            fiscal_year = (year - 1) % 100
        
        return f"{quarter}Q{fiscal_year:02d}"
    
    def extract_period_date(self, filing: Dict) -> str:
        """Extract the fiscal period end date from filing"""
        return filing['report_date']  # Already in YYYY-MM-DD format
    
    def process_dataframe(self, df: pd.DataFrame, period: str, period_date: str) -> pd.DataFrame:
        """Process and clean the extracted dataframe"""
        if df is None:
            return None
        
        try:
            # Create a copy
            df = df.copy()
            
            # Find the column with the most recent period data
            # Usually this is in the second or third column
            numeric_cols = []
            for col in df.columns[1:]:  # Skip first column (line items)
                try:
                    # Try to convert to numeric
                    pd.to_numeric(df[col].astype(str).str.replace(',', '').str.replace('$', '').str.replace('(', '-').str.replace(')', ''), errors='coerce')
                    numeric_cols.append(col)
                except:
                    pass
            
            if len(numeric_cols) == 0:
                return None
            
            # Create column name with period and date (format: "1Q22 (2022-04-30)")
            column_name = f"{period} ({period_date})"
            
            # Take first column as line items and first numeric column as values
            result = pd.DataFrame({
                'Line Item': df.iloc[:, 0],
                column_name: df[numeric_cols[0]]
            })
            
            return result
            
        except Exception as e:
            print(f"      ⚠️  Processing error: {e}")
            return None
    
    def extract_all_financials(self, start_year: int, end_year: int) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Extract all financial statements across quarters
        
        Returns:
            Tuple of (income_statement_df, balance_sheet_df, cashflow_df)
        """
        print("\n" + "="*80)
        print(f"EXTRACTING QUARTERLY FINANCIALS: {self.ticker}")
        print("="*80)
        
        # Get filings list
        filings = self.get_filings_list(start_year, end_year)
        
        if not filings:
            print("❌ No filings found")
            return None, None, None
        
        # Storage for each statement type
        income_data = {}
        balance_data = {}
        cashflow_data = {}
        
        print(f"\n📊 Processing {len(filings)} filings...\n")
        
        # Process in batches to avoid overwhelming
        batch_size = 5
        for batch_start in range(0, len(filings), batch_size):
            batch_end = min(batch_start + batch_size, len(filings))
            print(f"🔄 Processing batch {batch_start//batch_size + 1}/{(len(filings)-1)//batch_size + 1} (filings {batch_start+1}-{batch_end})")
            
            for i in range(batch_start, batch_end):
                filing = filings[i]
                period = self.extract_period_label(filing)
                period_date = self.extract_period_date(filing)
                
                print(f"[{i+1}/{len(filings)}] {filing['form']} - {filing['report_date']} → {period}")
                
                try:
                    # Download filing with timeout
                    html = self.download_filing(filing['accession_number'], filing['primary_document'])
                    
                    if not html:
                        print(f"      ❌ Download failed")
                        continue
                    
                    # Extract each statement
                    income_df = self.extract_financial_table(html, 'income')
                    balance_df = self.extract_financial_table(html, 'balance')
                    cashflow_df = self.extract_financial_table(html, 'cashflow')
                    
                    # Process and store
                    if income_df is not None:
                        processed = self.process_dataframe(income_df, period, period_date)
                        if processed is not None:
                            income_data[period] = processed
                            print(f"      ✅ Income Statement")
                    
                    if balance_df is not None:
                        processed = self.process_dataframe(balance_df, period, period_date)
                        if processed is not None:
                            balance_data[period] = processed
                            print(f"      ✅ Balance Sheet")
                    
                    if cashflow_df is not None:
                        processed = self.process_dataframe(cashflow_df, period, period_date)
                        if processed is not None:
                            cashflow_data[period] = processed
                            print(f"      ✅ Cash Flow Statement")
                            
                except Exception as e:
                    print(f"      ❌ Error processing filing: {e}")
                    continue
            
            # Add extra delay between batches
            if batch_end < len(filings):
                print("⏸️  Pausing between batches...")
                time.sleep(2.0)
        
        print("\n📊 Combining quarterly data...")
        
        # Combine all quarters into single dataframes
        income_combined = self.combine_quarters(income_data)
        balance_combined = self.combine_quarters(balance_data)
        cashflow_combined = self.combine_quarters(cashflow_data)
        
        return income_combined, balance_combined, cashflow_combined
    
    def combine_quarters(self, data_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Combine quarterly dataframes into one with quarters as columns"""
        if not data_dict:
            return None
        
        # Sort periods chronologically by extracting the period label (before the date)
        # Format is like "1Q22 (2022-04-30)" so we extract just "1Q22" for sorting
        def sort_key(period_str):
            # Extract just the period label (e.g., "1Q22" from "1Q22 (2022-04-30)")
            period_label = period_str.split(' ')[0] if ' ' in period_str else period_str
            # Parse quarter and year (e.g., "1Q22" -> quarter=1, year=22)
            quarter = int(period_label[0])
            year = int(period_label[2:4])
            return (year, quarter)
        
        periods = sorted(data_dict.keys(), key=sort_key)
        
        # Start with first period - use copy to avoid modifying original
        result = data_dict[periods[0]][['Line Item']].copy()
        
        # Add each period as a new column (more memory efficient)
        for period in periods:
            df = data_dict[period]
            # Get the column name (should be the period with date)
            col_name = [c for c in df.columns if c != 'Line Item'][0]
            # Merge just this column
            result = result.merge(df[['Line Item', col_name]], on='Line Item', how='outer')
        
        return result
    
    def save_to_excel(self, income_df: pd.DataFrame, balance_df: pd.DataFrame, 
                      cashflow_df: pd.DataFrame, output_file: str):
        """Save all statements to Excel"""
        
        print(f"\n💾 Saving to Excel: {output_file}")
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Save each statement
            if income_df is not None:
                income_df.to_excel(writer, sheet_name='Income Statement', index=False)
                print(f"   ✅ Income Statement - {len(income_df.columns)-1} quarters")
            
            if balance_df is not None:
                balance_df.to_excel(writer, sheet_name='Balance Sheet', index=False)
                print(f"   ✅ Balance Sheet - {len(balance_df.columns)-1} quarters")
            
            if cashflow_df is not None:
                cashflow_df.to_excel(writer, sheet_name='Cash Flow Statement', index=False)
                print(f"   ✅ Cash Flow Statement - {len(cashflow_df.columns)-1} quarters")
            
            # Summary
            summary = pd.DataFrame({
                'Field': ['Company', 'Ticker', 'CIK', 'Data Source', 'Extract Date', 'Currency', 'Format'],
                'Value': [
                    self.company_name or self.ticker,
                    self.ticker,
                    self.cik,
                    'SEC EDGAR API',
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'USD',
                    'Quarters as columns (e.g., 1Q22, 2Q22, 3Q22, etc.)'
                ]
            })
            summary.to_excel(writer, sheet_name='Summary', index=False)
            print(f"   ✅ Summary")
        
        print("\n✅ Complete!")
        return output_file


def extract_quarterly_financials(ticker: str, start_year: int, end_year: int, output_file: str = None, test_mode: bool = False) -> str:
    """
    Main function to extract quarterly financials for any company
    
    Args:
        ticker: Stock ticker symbol (e.g., 'AAPL', 'MSFT', 'WMT')
        start_year: Starting year
        end_year: Ending year
        output_file: Output file path (optional)
    
    Returns:
        Path to output file
    
    Example:
        extract_quarterly_financials('AAPL', 2022, 2025)
    """
    
    if output_file is None:
        output_file = f"{ticker.lower()}_quarterly_{start_year}_{end_year}.xlsx"
    
    # Create extractor
    extractor = SECQuarterlyExtractor(ticker)
    
    # Extract data
    income_df, balance_df, cashflow_df = extractor.extract_all_financials(start_year, end_year)
    
    # Save to Excel
    if income_df is not None or balance_df is not None or cashflow_df is not None:
        extractor.save_to_excel(income_df, balance_df, cashflow_df, output_file)
        print(f"\n📁 File: {output_file}")
        return output_file
    else:
        print("\n❌ No data extracted")
        return None


if __name__ == "__main__":
    print("="*80)
    print("AUTOMATED SEC QUARTERLY FINANCIAL DATA EXTRACTOR")
    print("Downloads and parses 10-Q filings automatically")
    print("="*80)
    print()
    
    # Command line usage
    if len(sys.argv) >= 2:
        ticker = sys.argv[1]
        start_year = int(sys.argv[2]) if len(sys.argv) >= 3 else 2022
        end_year = int(sys.argv[3]) if len(sys.argv) >= 4 else 2025
    else:
        # Default: Walmart
        ticker = 'WMT'
        start_year = 2022
        end_year = 2025
    
    print(f"Company: {ticker}")
    print(f"Period: {start_year}-{end_year}\n")
    
    # Extract
    output = extract_quarterly_financials(ticker, start_year, end_year)
    
    if output:
        print("\n" + "="*80)
        print("SUCCESS!")
        print("="*80)
        print(f"\n📊 Quarterly data extracted with quarters as columns")
        print(f"📁 {output}")
        print("\n" + "="*80)
        print("TO USE FOR OTHER COMPANIES:")
        print("="*80)
        print("""
# Command line:
python sec_quarterly_extractor_auto.py AAPL 2022 2025
python sec_quarterly_extractor_auto.py MSFT 2020 2025
python sec_quarterly_extractor_auto.py TSLA 2022 2025

# In Python:
from sec_quarterly_extractor_auto import extract_quarterly_financials

extract_quarterly_financials('AAPL', 2022, 2025)
extract_quarterly_financials('GOOGL', 2022, 2025)
extract_quarterly_financials('NVDA', 2022, 2025)
        """)