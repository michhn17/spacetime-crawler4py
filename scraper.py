import re
import json
from urllib.parse import urlparse, urljoin, urldefrag
from bs4 import BeautifulSoup

unique_urls = set()
word_counter = {}
longest_page = {'url': '', 'count': 0}
subdomains = {}
count = 0

def save_stats():
    try:
        data = {
            'unique_urls': list(unique_urls),
            'word_counter': word_counter,
            'longest_page': longest_page,
            'subdomains': subdomains,
            'count': count
        }
        with open('stats.json', 'w') as f:
            json.dump(data, f)
    except:
        pass

def scraper(url, resp):
    global count
    count += 1
    print(f"[{count}] {url[:80]}")
    
    links = extract_next_links(url, resp)
    valid = [link for link in links if is_valid(link)]
    
    if count % 100 == 0:
        save_stats()
    
    return valid

def extract_next_links(url, resp):
    if resp.status != 200:
        return []
        
    if not resp.raw_response or not resp.raw_response.content:
        return []

    if len(resp.raw_response.content) < 500:
        return []
        
    try: 
        soup = BeautifulSoup(resp.raw_response.content, 'lxml')
        text = soup.get_text()
        words = text.lower().split()
        
        stop_words = {'the','a','an','and','or','but','in','on','at','to','for','of','with','is','it','that','this','as','be','by','from','are','was','were','been','have','has','had','do','does','did','will','would','should','could','can','may','might','must','shall'}
        filtered = [w for w in words if len(w) > 2 and w.isalpha() and w not in stop_words]
        
        unique_urls.add(url)
        for w in filtered:
            word_counter[w] = word_counter.get(w, 0) + 1
        
        if len(filtered) > longest_page['count']:
            longest_page['url'] = url
            longest_page['count'] = len(filtered)
        
        subdomain_match = re.search(r'https?://([^/]+\.uci\.edu)', url)
        if subdomain_match:
            sd = subdomain_match.group(1)
            subdomains[sd] = subdomains.get(sd, 0) + 1
        
        links = []
        for link in soup.find_all('a', href=True):
            absolute_url = urljoin(resp.url, link['href'])
            defragged_url, _ = urldefrag(absolute_url)

            if any(trap in defragged_url.lower() for trap in ['calendar', 'event']):
                continue
            
            links.append(defragged_url)
    
    except Exception as e:
        print(f"Error parsing {url}: {e}")
        return []

    return links

def is_valid(url):
    try:
        parsed = urlparse(url)
        
        if parsed.scheme not in set(["http", "https"]):
            return False
        
        allowed_domains = [
            ".ics.uci.edu",
            ".cs.uci.edu", 
            ".informatics.uci.edu",
            ".stat.uci.edu"   
        ]

        if not any(parsed.netloc.endswith(domain) or 
                   parsed.netloc == domain[1:] for domain in allowed_domains):
            return False

        if re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower()):
            return False
        
        return True

    except TypeError:
        print ("TypeError for ", parsed)
        raise