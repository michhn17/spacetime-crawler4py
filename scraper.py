import re
from urllib.parse import urlparse, urljoin, urldefrag
from bs4 import BeautifulSoup   # HTML parsing library to extract links

def scraper(url, resp):
    """
    Main scraper function that processes URL and its response.
    
    Args: 
        url: The URL to be downloaded from frontier
        resp: Response object containing page content and metadata
    
    Returns: 
        List of valid URLs scraped from the page
    """
    # Extract all links from the page
    links = extract_next_links(url, resp)

    # Filter links to only return valid ones (correct domain, not files, etc.)
    return [link for link in links if is_valid(link)]

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
                continue
            
            # Add cleaned URL to list
            links.append(defragged_url)
    
    # Catch parsing errors (malformed HTML, encoding issues, etc.)
    except Exception as e:
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
