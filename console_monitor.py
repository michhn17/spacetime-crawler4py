"""
Console Crawler Monitor - Real-time monitoring for spacetime-crawler4py
Designed for SSH/terminal environments with live console updates.

Usage:
    from console_monitor import monitor
    
    # In your scraper.py, just call monitor functions:
    monitor.log_url(url, resp.status, word_count, content_length)
"""

import time
import os
import sys
from collections import defaultdict, Counter
from datetime import datetime
from threading import Lock
import re
import signal
import atexit

class ConsoleCrawlerMonitor:
    """Real-time console-based crawler monitor with live statistics."""
    
    def __init__(self):
        self.lock = Lock()
        self.start_time = time.time()
        
        # Statistics
        self.total_crawled = 0
        self.unique_urls = set()
        self.queue_size = 0
        
        # Domain tracking
        self.domains = defaultdict(int)
        self.subdomains = defaultdict(int)
        
        # Status codes
        self.status_codes = defaultdict(int)
        
        # Trap detection
        self.traps = defaultdict(int)
        
        # Word tracking
        self.word_counter = Counter()
        
        # Page tracking
        self.longest_page = {'url': '', 'word_count': 0}
        
        # Performance
        self.response_times = []
        self.total_bytes = 0
        
        # Recent activity
        self.recent_urls = []
        self.recent_errors = []
        
        # Last print time for rate limiting
        self.last_print = 0
        self.print_interval = 0  # seconds between updates (0 = always print)
        
        # Register handlers for clean exit
        self._setup_exit_handlers()
    
    def _setup_exit_handlers(self):
        """Set up handlers to print final report on exit."""
        def exit_handler(signum=None, frame=None):
            """Handle exit and print final report."""
            print("\n\n" + "="*80)
            print("🛑 Crawler stopping... Generating final report...")
            print("="*80)
            
            # Print final comprehensive report
            self.print_final_report()
            
            # Save to file
            self.save_report('crawler_report.txt')
            
            print("\n✅ Reports generated successfully!")
            print("   - Console output: See above")
            print("   - Text file: crawler_report.txt")
            print("\n")
            
            if signum is not None:
                sys.exit(0)
        
        # Register for Ctrl+C
        signal.signal(signal.SIGINT, exit_handler)
        
        # Register for normal exit
        atexit.register(lambda: exit_handler(None, None))
        
    def log_url(self, url, status_code, word_count=0, content_length=0, response_time=0):
        """Log a crawled URL."""
        should_print = False
        
        with self.lock:
            self.total_crawled += 1
            self.unique_urls.add(url)
            self.status_codes[status_code] += 1
            
            # Track domain
            domain = self._extract_domain(url)
            if domain:
                self.domains[domain] += 1
            
            # Track subdomain
            subdomain = self._extract_subdomain(url)
            if subdomain:
                self.subdomains[subdomain] += 1
            
            # Track longest page
            if word_count > self.longest_page['word_count']:
                self.longest_page = {'url': url, 'word_count': word_count}
            
            # Performance tracking
            if response_time > 0:
                self.response_times.append(response_time)
            self.total_bytes += content_length
            
            # Recent activity
            self.recent_urls.append({
                'url': url,
                'status': status_code,
                'time': datetime.now().strftime('%H:%M:%S')
            })
            if len(self.recent_urls) > 10:
                self.recent_urls.pop(0)
            
            # Check if should print (but don't print while holding lock!)
            if time.time() - self.last_print > self.print_interval:
                should_print = True
        
        # Print outside of lock to avoid deadlock
        if should_print:
            self.print_stats()
    
    def log_words(self, words_list):
        """Log words from a page."""
        with self.lock:
            self.word_counter.update(words_list)
    
    def detect_trap(self, url, trap_type):
        """Log a detected trap."""
        with self.lock:
            self.traps[trap_type] += 1
            print(f"\n🚨 TRAP DETECTED: {trap_type} - {url}\n")
    
    def log_error(self, url, error_msg):
        """Log an error."""
        with self.lock:
            self.recent_errors.append({
                'url': url,
                'error': error_msg,
                'time': datetime.now().strftime('%H:%M:%S')
            })
            if len(self.recent_errors) > 5:
                self.recent_errors.pop(0)
    
    def update_queue_size(self, size):
        """Update the frontier queue size."""
        with self.lock:
            self.queue_size = size
    
    def _extract_domain(self, url):
        """Extract main domain."""
        if '.ics.uci.edu' in url:
            return 'ics.uci.edu'
        elif '.cs.uci.edu' in url:
            return 'cs.uci.edu'
        elif '.informatics.uci.edu' in url:
            return 'informatics.uci.edu'
        elif '.stat.uci.edu' in url:
            return 'stat.uci.edu'
        return None
    
    def _extract_subdomain(self, url):
        """Extract full subdomain."""
        match = re.search(r'https?://([^/]+\.uci\.edu)', url)
        return match.group(1) if match else None
    
    def print_stats(self):
        """Print live statistics to console."""
        with self.lock:
            self.last_print = time.time()
            
            # Don't clear screen - just print stats
            # os.system('clear' if os.name != 'nt' else 'cls')
            
            elapsed = time.time() - self.start_time
            elapsed_mins = elapsed / 60
            crawl_rate = self.total_crawled / elapsed_mins if elapsed_mins > 0 else 0
            
            print("\n" + "=" * 80)
            print("🕷️  WEB CRAWLER MONITOR - LIVE STATISTICS".center(80))
            print("=" * 80)
            print(f"Runtime: {int(elapsed_mins)}m {int(elapsed % 60)}s | Last Update: {datetime.now().strftime('%H:%M:%S')}")
            print("=" * 80)
            
            # Main stats
            print(f"\n📊 CRAWL PROGRESS:")
            print(f"   Total Crawled:  {self.total_crawled:,}")
            print(f"   Unique Pages:   {len(self.unique_urls):,}")
            print(f"   Queue Size:     {self.queue_size:,}")
            print(f"   Crawl Rate:     {crawl_rate:.1f} pages/min")
            
            # Domain distribution
            print(f"\n🌐 DOMAIN DISTRIBUTION:")
            for domain in ['ics.uci.edu', 'cs.uci.edu', 'informatics.uci.edu', 'stat.uci.edu']:
                count = self.domains[domain]
                pct = (count / self.total_crawled * 100) if self.total_crawled > 0 else 0
                bar = '█' * int(pct / 2) + '░' * (50 - int(pct / 2))
                print(f"   {domain:25} {count:5,} [{bar}] {pct:5.1f}%")
            
            # Status codes
            print(f"\n📡 HTTP STATUS CODES:")
            status_200 = self.status_codes[200]
            status_3xx = sum(self.status_codes[code] for code in range(300, 400))
            status_4xx = sum(self.status_codes[code] for code in range(400, 500))
            status_5xx = sum(self.status_codes[code] for code in range(500, 600))
            print(f"   ✅ 200 OK:        {status_200:,}")
            print(f"   🔄 3xx Redirect:  {status_3xx:,}")
            print(f"   ❌ 4xx Error:     {status_4xx:,}")
            print(f"   ⚠️  5xx Error:     {status_5xx:,}")
            
            # Trap detection
            if sum(self.traps.values()) > 0:
                print(f"\n🚨 TRAPS DETECTED:")
                for trap_type, count in self.traps.items():
                    print(f"   {trap_type:15} {count:,}")
            
            # Performance
            print(f"\n📈 PERFORMANCE:")
            avg_response = sum(self.response_times) / len(self.response_times) if self.response_times else 0
            print(f"   Avg Response:   {avg_response*1000:.0f}ms")
            print(f"   Data Downloaded: {self.total_bytes / (1024*1024):.2f} MB")
            print(f"   Longest Page:   {self.longest_page['word_count']:,} words")
            
            # Top subdomains
            print(f"\n🏢 TOP SUBDOMAINS:")
            top_subdomains = sorted(self.subdomains.items(), key=lambda x: x[1], reverse=True)[:5]
            for subdomain, count in top_subdomains:
                print(f"   {subdomain:40} {count:5,}")
            
            # Top words
            print(f"\n🔤 TOP 10 WORDS:")
            for word, count in self.word_counter.most_common(10):
                print(f"   {word:20} {count:6,}")
            
            # Recent activity
            print(f"\n📝 RECENT CRAWLS:")
            for entry in self.recent_urls[-5:]:
                status_icon = '✅' if entry['status'] == 200 else '❌'
                url_short = entry['url'][:60] + '...' if len(entry['url']) > 60 else entry['url']
                print(f"   [{entry['time']}] {status_icon} {entry['status']} {url_short}")
            
            # Recent errors
            if self.recent_errors:
                print(f"\n⚠️  RECENT ERRORS:")
                for err in self.recent_errors[-3:]:
                    url_short = err['url'][:50] + '...' if len(err['url']) > 50 else err['url']
                    print(f"   [{err['time']}] {url_short}")
                    print(f"              {err['error'][:60]}")
            
            print("\n" + "=" * 80)
            print("Press Ctrl+C to stop the crawler")
            print("=" * 80)
    
    def print_final_report(self):
        """Print final comprehensive report."""
        with self.lock:
            elapsed = time.time() - self.start_time
            
            print("\n\n" + "=" * 80)
            print("📊 FINAL CRAWLER REPORT".center(80))
            print("=" * 80)
            
            print(f"\n⏱️  RUNTIME: {int(elapsed/3600)}h {int((elapsed%3600)/60)}m {int(elapsed%60)}s")
            
            print(f"\n📈 SUMMARY:")
            print(f"   Total URLs Crawled:    {self.total_crawled:,}")
            print(f"   Unique Pages Found:    {len(self.unique_urls):,}")
            print(f"   Subdomains Discovered: {len(self.subdomains):,}")
            
            print(f"\n📄 LONGEST PAGE:")
            print(f"   URL: {self.longest_page['url']}")
            print(f"   Word Count: {self.longest_page['word_count']:,}")
            
            print(f"\n🔤 TOP 50 WORDS:")
            for i, (word, count) in enumerate(self.word_counter.most_common(50), 1):
                print(f"   {i:2}. {word:20} {count:,}")
            
            print(f"\n🏢 ALL SUBDOMAINS (Alphabetical):")
            for subdomain, count in sorted(self.subdomains.items()):
                print(f"   {subdomain}, {count}")
            
            print("\n" + "=" * 80)
    
    def save_report(self, filename='crawler_report.txt'):
        """Save report to file."""
        with self.lock:
            with open(filename, 'w') as f:
                f.write("WEB CRAWLER FINAL REPORT\n")
                f.write("=" * 80 + "\n\n")
                
                f.write(f"1. UNIQUE PAGES FOUND: {len(self.unique_urls):,}\n\n")
                
                f.write(f"2. LONGEST PAGE:\n")
                f.write(f"   URL: {self.longest_page['url']}\n")
                f.write(f"   Word Count: {self.longest_page['word_count']:,}\n\n")
                
                f.write(f"3. TOP 50 MOST COMMON WORDS:\n")
                for word, count in self.word_counter.most_common(50):
                    f.write(f"   {word}, {count}\n")
                f.write("\n")
                
                f.write(f"4. SUBDOMAINS ({len(self.subdomains)} total):\n")
                for subdomain, count in sorted(self.subdomains.items()):
                    f.write(f"   {subdomain}, {count}\n")
        
        print(f"\n✅ Report saved to {filename}")


# Global monitor instance
monitor = ConsoleCrawlerMonitor()


# Example integration with scraper.py
def example_scraper_integration():
    """
    Example of how to integrate this monitor with your scraper.py
    
    Add this to your scraper.py:
    
    from console_monitor import monitor
    import time
    from bs4 import BeautifulSoup
    
    def scraper(url, resp):
        start_time = time.time()
        
        # Check for traps first
        if any(trap in url.lower() for trap in ['calendar', 'event']):
            monitor.detect_trap(url, 'calendar/event')
            return []
        
        if resp.status != 200:
            monitor.log_url(url, resp.status, 0, 0)
            return []
        
        if not resp.raw_response or not resp.raw_response.content:
            monitor.log_url(url, resp.status, 0, 0)
            return []
        
        content_length = len(resp.raw_response.content)
        if content_length < 500:
            monitor.detect_trap(url, 'low_content')
            monitor.log_url(url, resp.status, 0, content_length)
            return []
        
        try:
            soup = BeautifulSoup(resp.raw_response.content, 'lxml')
            
            # Count words (remove HTML)
            text = soup.get_text()
            words = text.lower().split()
            # Filter out stop words
            stop_words = set(['the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for'])
            words = [w for w in words if len(w) > 2 and w.isalpha() and w not in stop_words]
            
            # Log words
            monitor.log_words(words)
            
            # Extract links
            links = []
            for link in soup.find_all('a', href=True):
                absolute_url = urljoin(resp.url, link['href'])
                defragged_url, _ = urldefrag(absolute_url)
                
                if any(trap in defragged_url.lower() for trap in ['calendar', 'event']):
                    continue
                    
                links.append(defragged_url)
            
            # Log this URL
            response_time = time.time() - start_time
            monitor.log_url(url, resp.status, len(words), content_length, response_time)
            
            return [link for link in links if is_valid(link)]
            
        except Exception as e:
            monitor.log_error(url, str(e))
            return []
    """
    pass


if __name__ == "__main__":
    print("Console Crawler Monitor - Demo Mode")
    print("This will simulate crawler activity for 30 seconds\n")
    
    # Simulate some crawling
    test_urls = [
        "https://www.ics.uci.edu/index.html",
        "https://www.cs.uci.edu/research",
        "https://www.informatics.uci.edu/people",
        "https://vision.ics.uci.edu/papers",
        "https://www.stat.uci.edu/courses",
    ]
    
    sample_words = ['machine', 'learning', 'data', 'science', 'algorithm', 
                   'computer', 'research', 'university', 'student', 'professor']
    
    try:
        for i in range(100):
            url = test_urls[i % len(test_urls)]
            monitor.log_url(url + f"/page{i}", 200, 500 + i*10, 5000, 0.2)
            monitor.log_words([sample_words[j % len(sample_words)] for j in range(50)])
            time.sleep(0.3)
            
            if i == 20:
                monitor.detect_trap("https://www.ics.uci.edu/calendar/2024", "calendar")
                
    except KeyboardInterrupt:
        print("\n\nStopping demo...")
    
    monitor.print_final_report()
    monitor.save_report()