import requests
from bs4 import BeautifulSoup
import json
import time
import re
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from urllib.parse import urljoin

BASE_URL = 'https://bfzy5.tv'
START_URL_TEMPLATE = 'https://bfzy5.tv/index.php/vod/type/id/41/page/{page}.html'
METADATA_PATH = os.environ.get('AIRFLY_METADATA', r'd:\project\airfly\metadata.json')
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}
MAX_WORKERS = 10

def load_metadata():
    if os.path.exists(METADATA_PATH):
        with open(METADATA_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    return []

def save_metadata(data):
    # Backup first
    if os.path.exists(METADATA_PATH):
        import shutil
        try:
            shutil.copy(METADATA_PATH, METADATA_PATH + '.bfzy.bak')
        except Exception:
            pass
    
    # Retry mechanism for Windows file locking issues
    for i in range(5):
        try:
            with open(METADATA_PATH, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            return
        except PermissionError:
            if i < 4:
                time.sleep(0.5)
                continue
            raise

def get_soup(url):
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.encoding = 'utf-8'
        if response.status_code == 200:
            return BeautifulSoup(response.text, 'html.parser')
        else:
            print(f"Failed to fetch {url}: {response.status_code}")
    except Exception as e:
        print(f"Error fetching {url}: {e}")
    return None

def parse_detail(url):
    soup = get_soup(url)
    if not soup:
        return None
    
    info = {}
    
    # Extract Title
    title_div = soup.find('div', class_='people')
    if title_div:
        right_div = title_div.find('div', class_='right')
        if right_div:
            # Helper to extract text by prefix
            def get_text_by_prefix(prefix):
                p_tag = right_div.find('p', string=lambda t: t and t.startswith(prefix))
                if p_tag:
                    return p_tag.get_text(strip=True).replace(prefix, '').strip()
                return ""

            info['片名'] = get_text_by_prefix('片名：')
            info['别名'] = get_text_by_prefix('别名：')
            info['豆瓣'] = get_text_by_prefix('豆瓣：')
            info['豆瓣ID'] = get_text_by_prefix('豆瓣ID：')
            info['状态'] = get_text_by_prefix('状态：')
            info['类型'] = get_text_by_prefix('类型：')
            info['导演'] = get_text_by_prefix('导演：')
            info['演员'] = get_text_by_prefix('演员：')
            info['年代'] = get_text_by_prefix('年代：')
            info['地区'] = get_text_by_prefix('地区：')
            info['语言'] = get_text_by_prefix('语言：')
            info['TAG标签'] = get_text_by_prefix('TAG标签：')
            info['更新时间'] = get_text_by_prefix('更新时间：')
            
            # Image
            img_tag = title_div.find('img')
            if img_tag:
                info['封面'] = img_tag.get('src', '')
                if info['封面'] and not info['封面'].startswith('http'):
                     info['封面'] = urljoin(BASE_URL, info['封面'])
            else:
                 info['封面'] = ""

    if not info.get('片名'):
         # Fallback if structure matches detail_direct.html but extraction failed
         # Try finding h1?
         h1 = soup.find('h1', class_='whitetitle')
         if h1:
             # Usually "资源详情"
             pass
    
    # Description
    desc_div = soup.find('div', class_='vod_content')
    if desc_div:
        info['剧情介绍'] = desc_div.get_text(strip=True)
    else:
        info['剧情介绍'] = ""
        
    info['detail_url'] = url
    
    # Playlists
    # Look for div with class "playlist wbox bfzym3u8"
    playlists = []
    # bfzy usually has 'bfzym3u8'
    
    # Find all playlist divs? Or just specific one?
    # User mentioned "bfzy5.tv", usually "暴风资源"
    
    bfzy_div = soup.find('div', class_='playlist wbox bfzym3u8')
    if bfzy_div:
        source_name = "暴风资源"
        episodes = []
        # Checkboxes have value "Name$URL"
        checkboxes = bfzy_div.find_all('input', {'name': 'copy_bfzym3u8[]'})
        for cb in checkboxes:
            val = cb.get('value')
            if val and '$' in val:
                parts = val.split('$')
                ep_name = parts[0]
                ep_url = parts[1]
                episodes.append({
                    "name": ep_name,
                    "url": ep_url
                })
        
        if episodes:
             playlists.append({
                 "source": source_name,
                 "episodes": episodes
             })
             
    info['播放地址'] = playlists
    
    return info

def get_total_pages():
    # Check page 1 to find total pages
    url = START_URL_TEMPLATE.format(page=1)
    soup = get_soup(url)
    if not soup:
        return 1
    
    # Look for "尾页" link
    last_page_link = soup.find('a', string='尾页')
    if last_page_link:
        href = last_page_link.get('href')
        # href like /index.php/vod/type/id/41/page/109.html
        match = re.search(r'/page/(\d+)\.html', href)
        if match:
            return int(match.group(1))
            
    return 1

def main():
    print("Loading metadata...")
    metadata = load_metadata()
    print(f"Loaded {len(metadata)} entries.")
    
    # Build indices
    douban_id_map = {} # ID -> index in metadata
    name_map = {}      # Name -> list of indices (could be duplicates)
    
    for i, item in enumerate(metadata):
        did = item.get('豆瓣ID', '0')
        if did and did != '0' and did != 0:
            douban_id_map[str(did)] = i
            
        name = item.get('片名', '').strip()
        if name:
            if name not in name_map:
                name_map[name] = []
            name_map[name].append(i)
            
    print("Getting total pages...")
    import sys
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--pages", type=int, default=0, help="Number of pages to crawl. 0 for all.")
    # Use parse_known_args to avoid conflicts if other args are passed
    args, _ = parser.parse_known_args()
    
    if args.pages > 0:
        total_pages = args.pages
        print(f"Crawling first {total_pages} pages as requested.")
    else:
        total_pages = get_total_pages()
        print(f"Total pages: {total_pages}")
    
    # Collect all detail URLs first
    detail_urls = []
    
    print("Collecting detail URLs...")
    # with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    #     futures = []
    #     for page in range(1, total_pages + 1):
    #         url = START_URL_TEMPLATE.format(page=page)
    #         futures.append(executor.submit(get_soup, url))
            
    #     for future in tqdm(as_completed(futures), total=len(futures), desc="Scanning pages"):
    #         soup = future.result()
    #         if soup:
    #             # Find detail links
    #             # Usually in <a class="videoName" ... href="...">
    #             links = soup.find_all('a', class_='videoName')
    #             for link in links:
    #                 href = link.get('href')
    #                 if href:
    #                     full_url = urljoin(BASE_URL, href)
    #                     detail_urls.append(full_url)
    
    # Sequential for debugging and safety first
    for page in range(1, total_pages + 1):
        url = START_URL_TEMPLATE.format(page=page)
        print(f"Scanning {url}")
        soup = get_soup(url)
        if soup:
            links = soup.find_all('a', class_='videoName')
            print(f"Found {len(links)} links on page {page}")
            for link in links:
                href = link.get('href')
                if href:
                    full_url = urljoin(BASE_URL, href)
                    detail_urls.append(full_url)
        else:
            print(f"Failed to load page {page}")
    
    # Deduplicate URLs
    detail_urls = list(set(detail_urls))
    print(f"Found {len(detail_urls)} videos.")
    
    print("Crawling details and updating...")
    
    updated_count = 0
    added_count = 0
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(parse_detail, url): url for url in detail_urls}
        
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing videos"):
            url = futures[future]
            try:
                info = future.result()
                if not info:
                    continue
                
                # Logic to match
                matched_index = -1
                
                # 1. Try Douban ID match
                crawled_did = info.get('豆瓣ID', '0')
                if crawled_did and crawled_did != '0':
                    if crawled_did in douban_id_map:
                        matched_index = douban_id_map[crawled_did]
                
                # 2. If not matched, try Name match
                if matched_index == -1:
                    name = info.get('片名', '').strip()
                    if name in name_map:
                        # Pick the first one? Or try to be smarter?
                        # For now pick first
                        matched_index = name_map[name][0]
                
                # Update or Add
                if matched_index != -1:
                    # Update
                    entry = metadata[matched_index]
                    
                    # Update fields if missing or just update playlists?
                    # User said "json播放资源的补充"
                    # We should append "暴风资源" to "播放地址"
                    
                    if '播放地址' not in entry:
                        entry['播放地址'] = []
                    
                    # Check if "暴风资源" already exists
                    existing_sources = {p['source'] for p in entry['播放地址']}
                    
                    for new_p in info['播放地址']:
                        if new_p['source'] in existing_sources:
                            # Update existing source episodes?
                            # Find the source dict
                            for p in entry['播放地址']:
                                if p['source'] == new_p['source']:
                                    p['episodes'] = new_p['episodes'] # Replace episodes with new ones
                                    break
                        else:
                            entry['播放地址'].append(new_p)
                    
                    updated_count += 1
                else:
                    # Add new
                    # Use all fields
                    metadata.append(info)
                    
                    # Update indices for future matches in this run? 
                    # Probably not needed if unique enough, but good practice
                    idx = len(metadata) - 1
                    if crawled_did and crawled_did != '0':
                        douban_id_map[crawled_did] = idx
                    name = info.get('片名', '').strip()
                    if name:
                        if name not in name_map:
                            name_map[name] = []
                        name_map[name].append(idx)
                        
                    added_count += 1
                    
            except Exception as e:
                # print(f"Error processing {url}: {e}")
                pass

    print(f"Updated {updated_count} entries.")
    print(f"Added {added_count} new entries.")
    
    save_metadata(metadata)
    print("Metadata saved.")

if __name__ == '__main__':
    main()
