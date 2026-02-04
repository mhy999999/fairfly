
import pandas as pd
import json
import re

file_path = r'd:\project\airfly\web\202601v2.13.xlsx'
output_path = r'd:\project\airfly\web\anime_data.json'

try:
    # Read the '2026年1月新番表' sheet, skipping the first few rows if necessary, 
    # but based on previous output, row 4 (index 3 in 0-based) starts data? 
    # Actually, row 4 in previous output (index 4) has "暗芝居...".
    # Let's read without header and iterate.
    df = pd.read_excel(file_path, sheet_name='2026年1月新番表', header=None)
    
    anime_list = []
    
    # Iterate rows
    for index, row in df.iterrows():
        name = row[2]
        day = row[3]
        time = row[4]
        
        # Check if valid row
        if pd.isna(name) or pd.isna(day):
            continue
            
        name = str(name).strip()
        day = str(day).strip()
        
        # Filter out header/footer rows
        if "新番表" in name or "周更时间" in str(day):
            continue
            
        # Clean time
        if pd.isna(time):
            time_str = ""
        else:
            # Time might be a datetime object or string
            if hasattr(time, 'strftime'):
                time_str = time.strftime('%H:%M')
            else:
                time_str = str(time).strip()
        
        anime_list.append({
            "name": name,
            "day": day,
            "time": time_str
        })
        
    # Sort or organize? 
    # The user wants a calendar, so maybe grouping by day is useful, 
    # but a flat list is also fine for frontend to filter.
    # Let's just dump the flat list.
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(anime_list, f, ensure_ascii=False, indent=2)
        
    print(f"Extracted {len(anime_list)} anime entries to {output_path}")
    
except Exception as e:
    print(f"Error: {e}")
