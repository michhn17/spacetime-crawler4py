import json
import os

if os.path.exists('stats.json'):
    with open('stats.json', 'r') as f:
        data = json.load(f)
    unique_urls = set(data['unique_urls'])
    word_counter = data['word_counter']
    longest_page = data['longest_page']
    subdomains = data['subdomains']
    
with open('report.txt', 'w') as f:
    f.write("WEB CRAWLER REPORT\n")
    f.write("="*80 + "\n\n")
    
    f.write(f"1. Unique pages found: {len(unique_urls)}\n\n")
    
    f.write(f"2. Longest page:\n")
    f.write(f"   URL: {longest_page['url']}\n")
    f.write(f"   Words: {longest_page['count']}\n\n")
    
    f.write(f"3. Top 50 common words:\n")
    sorted_words = sorted(word_counter.items(), key=lambda x: x[1], reverse=True)[:50]
    for word, count in sorted_words:
        f.write(f"   {word}, {count}\n")
    f.write("\n")
    
    f.write(f"4. Subdomains ({len(subdomains)} total):\n")
    for subdomain, count in sorted(subdomains.items()):
        f.write(f"   {subdomain}, {count}\n")

print(f"Unique pages: {len(unique_urls)}")
print(f"Subdomains: {len(subdomains)}")
print(f"Longest page: {longest_page['count']} words")