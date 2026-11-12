import requests

url = 'https://huggingface.co/api/datasets/McAuley-Lab/Amazon-Reviews-2023/tree/main/raw/review_categories'
r = requests.get(url, timeout=15)
for item in r.json():
    sz_mb = (item.get('size') or 0) / (1024*1024)
    print(f"{item.get('path')} : {sz_mb:.2f} MB")
