import os
import datetime
import requests
import pandas as pd
import openpyxl
import json
from urllib.parse import urlparse, parse_qs
from io import BytesIO

def check_required_directories():
    """
    Check if required directories exist, if not create them
    """
    config = load_config()
    required_dirs = [
        config['paths']['downloads_dir'],
        config['paths']['logs_dir'],
        config['paths']['summary_dir']
    ]
    
    for directory in required_dirs:
        if not os.path.exists(directory):
            os.makedirs(directory)
            print(f"Created directory: {directory}")
        else:
            print(f"Directory exists: {directory}")

def load_config():
    with open('config.json', 'r') as f:
        return json.load(f)

def create_folder():
    # Load configuration
    config = load_config()
    downloads_dir = config['paths']['downloads_dir']
    
    # Create folder with format data_probacionismo_yyyymmdd_hhmmss
    current_time = datetime.datetime.now()
    folder_name = f"data_probacionismo_{current_time.strftime('%Y%m%d_%H%M%S')}"
    full_path = os.path.join(downloads_dir, folder_name)
    
    if not os.path.exists(full_path):
        os.makedirs(full_path)
    
    return full_path, current_time

def get_direct_download_url(url):
    """Convert OneDrive share link to direct download link"""
    if 'onedrive.live.com' in url:
        # Extract the share link from the URL if it exists
        if '1drv.ms' in url:
            return url  # Return short URLs as-is
            
        parsed = urlparse(url)
        query_params = parse_qs(parsed.query)
        
        if 'id' in query_params:
            file_id = query_params['id'][0]
            # Use the short format URL which is more reliable
            return f"https://1drv.ms/{file_id}"
            
    return url

def format_sede_name(sede):
    """Format sede name by replacing consecutive spaces and hyphens with a single underscore"""
    # First replace all hyphens with spaces
    sede = sede.replace('-', ' ')
    # Then split by any number of spaces and join with single underscore
    return '_'.join(word for word in sede.split() if word)

def process_excel_file(excel_content, folder_path, sede, timestamp):
    try:
        # Load workbook from memory
        wb = openpyxl.load_workbook(filename=BytesIO(excel_content), read_only=True)
        
        # Create new workbook for selected sheets
        new_wb = openpyxl.Workbook()
        # Remove default sheet
        new_wb.remove(new_wb.active)
        
        # List of sheets to copy
        sheets_to_copy = ['Probacionistas', 'Resultado para Informe']
        sheets_found = False
        
        for sheet_name in sheets_to_copy:
            if sheet_name in wb.sheetnames:
                sheets_found = True
                # Copy sheet
                source = wb[sheet_name]
                target = new_wb.create_sheet(title=sheet_name)
                
                # Copy data
                for row in source.rows:
                    target.append([cell.value for cell in row])
        
        if not sheets_found:
            print(f"Warning: No required sheets found in the Excel file for {sede}")
            return False
        
        # Get nivel from config
        config = load_config()
        nivel = config['excel_urls'][sede]['nivel']
        
        # Generate filename with new format: nombreSede-nivel-yyyymmdd_HHMMSS.xlsx
        formatted_sede = format_sede_name(sede)
        file_name = f"{formatted_sede}-{nivel}-{timestamp.strftime('%Y%m%d_%H%M%S')}.xlsx"
        file_path = os.path.join(folder_path, file_name)
        
        # Save the new workbook
        new_wb.save(file_path)
        print(f"Successfully processed and saved: {file_name}")
        return True
        
    except Exception as e:
        print(f"Error processing Excel file for {sede}: {str(e)}")
        return False

def download_and_process_file(url, folder_path, sede, timestamp):
    try:
        # Get direct download URL
        download_url = get_direct_download_url(url)
        
        # Create a session to handle cookies and redirects
        session = requests.Session()
        
        # Set up headers to mimic a browser more accurately
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        }
        
        # First request to get the sharing URL
        response = session.get(download_url, headers=headers, allow_redirects=True)
        response.raise_for_status()
        
        # Get the final URL after redirects
        final_url = response.url
        
        # Special handling for 1drv.ms links
        if '1drv.ms' in download_url:
            # Extract the sharing URL from the response
            if 'sharepoint.com' in final_url or 'onedrive.live.com' in final_url:
                # Parse the URL and get the embed URL
                parsed = urlparse(final_url)
                query_params = parse_qs(parsed.query)
                
                # If we have a sharing token, use it
                if 'share' in query_params:
                    share_token = query_params['share'][0]
                    # Use the sharing API endpoint
                    share_api_url = f"https://api.onedrive.com/v1.0/shares/{share_token}/driveItem/content"
                    response = session.get(share_api_url, headers=headers)
                    response.raise_for_status()
                
                # Otherwise try with resid and authkey
                elif 'resid' in query_params and 'authkey' in query_params:
                    resid = query_params['resid'][0]
                    authkey = query_params['authkey'][0].replace('!', '')
                    
                    # Try different URL patterns
                    urls_to_try = [
                        f"https://onedrive.live.com/download?resid={resid}&authkey={authkey}",
                        f"https://onedrive.live.com/download.aspx?resid={resid}&authkey={authkey}",
                        f"https://api.onedrive.com/v1.0/drives/items/{resid}/content"
                    ]
                    
                    for try_url in urls_to_try:
                        try:
                            response = session.get(try_url, headers=headers)
                            response.raise_for_status()
                            
                            # If we got HTML instead of a file, continue trying
                            if 'text/html' in response.headers.get('Content-Type', ''):
                                continue
                                
                            # If we got the file, break the loop
                            if response.content.startswith(b'PK'):
                                break
                        except:
                            continue
        
        # Check if we actually got an Excel file
        content_type = response.headers.get('Content-Type', '')
        if not response.content.startswith(b'PK'):
            print(f"Content-Type received: {content_type}")
            print(f"First few bytes: {response.content[:20]}")
            
            # One final attempt with the download parameter
            if '?' in final_url:
                final_url += '&download=1'
            else:
                final_url += '?download=1'
            response = session.get(final_url, headers=headers)
            response.raise_for_status()
            
            # If still not a ZIP file, raise error
            if not response.content.startswith(b'PK'):
                raise ValueError(f"Downloaded content for {sede} is not a valid Excel file")
        
        # Process the Excel file
        return process_excel_file(response.content, folder_path, sede, timestamp)
    
    except Exception as e:
        print(f"Error downloading file for {sede}: {str(e)}")
        if 'response' in locals():
            print(f"Response headers: {response.headers}")
            print(f"Response URL: {response.url}")
        return False

def download_excel_files():
    """
    Execute the download process for all configured Excel files.
    Returns the path of the folder where files were downloaded.
    """
    try:
        # Load configuration
        config = load_config()
        excel_urls = config['excel_urls']
        
        # Create folder for today's downloads and get timestamp
        folder_path, timestamp = create_folder()
        print(f"Created folder: {folder_path}")
        
        # Process each sede from config
        successful_downloads = 0
        total_files = len(excel_urls)
        
        for sede, info in excel_urls.items():
            # Get the URL from the config
            download_url = info['url']
            if download_and_process_file(download_url, folder_path, sede, timestamp):
                successful_downloads += 1
                print(f"Successfully processed {sede} (Nivel {info['nivel']})")
        
        print(f"\nDownload and processing complete!")
        print(f"Successfully processed {successful_downloads} files out of {total_files} URLs")
        print(f"Files are saved in: {folder_path}")
        
        return folder_path
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None

def main():
    # Check and create required directories first
    check_required_directories()
    # Then proceed with downloading files
    download_excel_files()

if __name__ == "__main__":
    main()