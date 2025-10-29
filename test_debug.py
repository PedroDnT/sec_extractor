import requests
import time

def test_sec_connection():
    """Test connection to SEC API"""
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
    }
    
    print("🔍 Testing SEC API connection...")
    
    try:
        # Test company facts API
        url = "https://data.sec.gov/api/xbrl/companyfacts/CIK0000104169.json"
        print(f"📡 Requesting: {url}")
        
        response = requests.get(url, headers=headers, timeout=10)
        print(f"✅ Response status: {response.status_code}")
        print(f"📊 Response size: {len(response.content)} bytes")
        
        if response.status_code == 200:
            print("✅ SEC API connection successful")
            return True
        else:
            print(f"❌ SEC API error: {response.status_code}")
            return False
            
    except requests.exceptions.Timeout:
        print("❌ Timeout - SEC API not responding")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_filing_download():
    """Test downloading a specific filing"""
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
    }
    
    print("\n🔍 Testing filing download...")
    
    try:
        # Test a specific WMT filing
        url = "https://www.sec.gov/Archives/edgar/data/104169/000010416923000004/wmt-20230131.htm"
        print(f"📥 Requesting: {url}")
        
        response = requests.get(url, headers=headers, timeout=10)
        print(f"✅ Response status: {response.status_code}")
        print(f"📊 Response size: {len(response.content)} bytes")
        
        if response.status_code == 200:
            print("✅ Filing download successful")
            return True
        else:
            print(f"❌ Download error: {response.status_code}")
            return False
            
    except requests.exceptions.Timeout:
        print("❌ Timeout - filing download not responding")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    print("="*50)
    print("SEC CONNECTION DEBUG TEST")
    print("="*50)
    
    # Test API connection
    api_ok = test_sec_connection()
    
    # Test filing download
    download_ok = test_filing_download()
    
    print("\n" + "="*50)
    print("SUMMARY:")
    print(f"API Connection: {'✅' if api_ok else '❌'}")
    print(f"Filing Download: {'✅' if download_ok else '❌'}")
    
    if api_ok and download_ok:
        print("✅ All tests passed - SEC connection is working")
    else:
        print("❌ Issues detected - check network or SEC servers")
