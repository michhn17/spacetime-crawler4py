import re
import time
from urllib.parse import urlparse, urljoin, urldefrag
from bs4 import BeautifulSoup   # HTML parsing library to extract links
from console_monitor import monitor

def scraper(url, resp):
    """
    Main scraper function that processes URL and its response.
    
    Args: 
        url: The URL to be downloaded from frontier
        resp: Response object containing page content and metadata
    
    Returns: 
        List of valid URLs scraped from the page
    """
    print(f"[SCRAPER START] Processing: {url}")
    start_time = time.time()
    
    # Extract all links from the page
    print(f"[SCRAPER] Extracting links from {url[:60]}...")
    links = extract_next_links(url, resp)
    print(f"[SCRAPER] Extracted {len(links)} links")
    
    # Calculate metrics for monitoring
    print(f"[SCRAPER] Calculating metrics...")
    response_time = time.time() - start_time
    word_count = 0
    content_length = 0
    
    if resp.raw_response and resp.raw_response.content:
        content_length = len(resp.raw_response.content)
        if resp.status == 200 and content_length >= 500:
            try:
                print(f"[SCRAPER] Counting words...")
                soup = BeautifulSoup(resp.raw_response.content, 'lxml')
                text = soup.get_text()
                words = text.lower().split()
                
                # Filter stop words - simplified
                stop_words = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for'}
                filtered_words = [w for w in words if len(w) > 2 and w.isalpha() and w not in stop_words]
                word_count = len(filtered_words)
                
                # Log words for analytics
                print(f"[SCRAPER] Found {word_count} words, logging to monitor...")
                monitor.log_words(filtered_words)
                print(f"[SCRAPER] Words logged")
            except Exception as e:
                print(f"[SCRAPER] Error counting words: {e}")
                pass
    
    # Log this URL to monitor
    monitor.log_url(url, resp.status, word_count, content_length, response_time)
    
    # Filter links to only return valid ones (correct domain, not files, etc.)
    valid_links = [link for link in links if is_valid(link)]
    
    # Debug: print how many links found
    print(f"[SCRAPER END] {url[:60]}... -> Found {len(links)} total, {len(valid_links)} valid")
    print(f"[SCRAPER] Returning {len(valid_links)} URLs to frontier\n")
    
    return valid_links

def extract_next_links(url, resp):
    """
    Parse the web response and extract all hyperlinks from page.
    
    Args: 
        url: the URL that was used to get the page
        resp.url: the actual url of the page (after redirects)
        resp.status: the status code returned by the server; 200 is OK, else = error
        resp.error: error message if status is not 200.
        resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
        resp.raw_response.url: the url, again
        resp.raw_response.content: the content of the page!

    Returns: 
        List of URLS (as strings) scrapped from resp.raw_response.content (page)
    """
    links = []

    # Only process pages with successful HTTP status (200)
    if resp.status != 200:
        return links
        
    # Check if response has actual content to parse
    if not resp.raw_response or not resp.raw_response.content:
        return links

    # Check for low information content
    # 500 bytes is reasonable threshold for meaningful content
    if len(resp.raw_response.content) < 500:
        monitor.detect_trap(url, 'low_content')
        return links
        
    try: 
        # Parse HTML content with BeautifulSoup lxml parser
        soup = BeautifulSoup(resp.raw_response.content, 'lxml')

        # Extract all anchor tags with href (links)
        for link in soup.find_all('a', href=True):
            # Convert URL to absolute URL using page's base URL
            absolute_url = urljoin(resp.url, link['href'])
                
            # Defragment the URL
            defragged_url, _ = urldefrag(absolute_url)

            # Avoid traps (calendar pages, event pages, etc.)
            if any(trap in defragged_url.lower() for trap in ['calendar', 'event']):
                monitor.detect_trap(defragged_url, 'calendar/event')
                continue
            
            # Add cleaned URL to list
            links.append(defragged_url)
    
    # Catch parsing errors (malformed HTML, encoding issues, etc.)
    except Exception as e:
        monitor.log_error(url, str(e))
        print(f"Error parsing {url}: {e}")  

    return links

def is_valid(url):
    """
    Determine if a URL should be crawled based on filtering rules.
    Filter URLs to stay within specified domains and avoid non-webpage files.
    
    Args:
        url: The URL to validate

    Returns:
        True if URL should be crawled, else False
    """
    try:
        # Parse URL into componens
        parsed = urlparse(url)
        
        # Accept only HTTPS and HTTP protocols
        if parsed.scheme not in set(["http", "https"]):
            return False
        
        # Define allowed domains 
        allowed_domains = [
            ".ics.uci.edu",
            ".cs.uci.edu", 
            ".informatics.uci.edu",
            ".stat.uci.edu"   
        ]

        # Check if domain ends with any allowed domain and subdomains
        if not any(parsed.netloc.endswith(domain) or 
                   parsed.netloc == domain[1:] for domain in allowed_domains):
            return False

        # Filter out non-webpage files (non-HTML pages)
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
        
        # URL is validated for crawling
        return True

    # Handle unexpected errors during parsing
    except TypeError:
        print ("TypeError for ", parsed)
        raise