import logging
import re
from urllib.parse import urlparse, urljoin
from corpus import Corpus
import urllib.request
import os
import requests
from bs4 import BeautifulSoup
from lxml import html
from collections import defaultdict

logger = logging.getLogger(__name__)

class Crawler:
    """
    This class is responsible for scraping urls from the next available link in frontier and adding the scraped links to
    the frontier
    """

    def __init__(self, frontier):
        self.frontier = frontier
        self.corpus = Corpus()
        self.count_dict = defaultdict(int)
        self.valid_page_links = defaultdict(int)
        self.trap_links = []



    def start_crawling(self):
        """
        This method starts the crawling process which is scraping urls from the next available link in frontier and adding
        the scraped links to the frontier
        """
        downloadable_links = []
        while self.frontier.has_next_url():
            url = self.frontier.get_next_url()
            logger.info("Fetching URL %s ... Fetched: %s, Queue size: %s", url, self.frontier.fetched, len(self.frontier))
            url_data = self.fetch_url(url)

            subdomain = urlparse(url_data['url']).netloc
            self.count_dict[subdomain] += 1

            downloadable_links.append(url)

            for next_link in self.extract_next_links(url_data):
                if self.corpus.get_file_name(next_link) is not None:
                    if self.is_valid(next_link):
                        self.frontier.add_url(next_link)
                        self.valid_page_links[next_link] += 1
                    

        text_string = ""
        for key, value in self.count_dict.items():
            text_string += '{:30} {}\n'.format(key, value)
            
        with open ("analytic_1.txt", 'w') as text_file:
            text_file.write(text_string)

        key_max = max(self.valid_page_links.keys(), key=(lambda k: self.valid_page_links[k]))
        valid_links = key_max + "  " + str(self.valid_page_links[key_max])
        with open ("analytic_2.txt", 'w') as text_file:
            text_file.write(valid_links)
        
        outF_1 = open("analytic_3_part_A.txt", "w")
        for word in downloadable_links:
            outF_1.writelines(word)
            outF_1.write("\n")
        outF_1.close()

        outF_2 = open("analytic_3_part_B.txt", "w")
        for word in self.trap_links:
            outF_2.write(word)
            outF_2.write("\n")
        outF_2.close()
            
                        

    def fetch_url(self, url):
        """
        This method, using the given url, should find the corresponding file in the corpus and return a dictionary
        containing the url, content of the file in binary format and the content size in bytes
        :param url: the url to be fetched
        :return: a dictionary containing the url, content and the size of the content. If the url does not
        exist in the corpus, a dictionary with content set to None and size set to 0 can be returned.
        """
        url_data = {
            "url": url,
            "content": None,
            "size": 0
        }

        current_url = self.corpus.get_file_name(url)
        
        if current_url is None:
           return url_data

        with open(current_url, 'rb') as f:
            content_bytes = f.read() 
    
        content_size  = os.path.getsize(current_url)
        url_data["url"] = url
        url_data["content"] = content_bytes
        url_data["size"] = content_size

        return url_data

    def extract_next_links(self, url_data):
        
        """
        The url_data coming from the fetch_url method will be given as a parameter to this method. url_data contains the
        fetched url, the url content in binary format, and the size of the content in bytes. This method should return a
        list of urls in their absolute form (some links in the content are relative and needs to be converted to the
        absolute form). Validation of links is done later via is_valid method. It is not required to remove duplicates
        that have already been fetched. The frontier takes care of that.

        Suggested library: lxml
        """
        outputLinks = []

        html_page = url_data["content"]
        

        if url_data["url"] is None:
            return outputLinks    

        if not self.is_absolute(url_data["url"]):
            return outputLinks   

        dom = html.fromstring(html_page)
        for link in dom.xpath('//a/@href'):
            link = urljoin(url_data["url"], link)
            outputLinks.append(link)
        
        return outputLinks

    def is_absolute(self, url):
        return bool(urlparse(url).netloc)

    
    def is_valid(self, url):
        """
        Function returns True or False based on whether the url has to be fetched or not. This is a great place to
        filter out crawler traps. Duplicated urls will be taken care of by frontier. You don't need to check for duplication
        in this method
        """
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            self.trap_links.append(url)
            return False
        if parsed.netloc == "":
            self.trap_links.append(url)
            return False
        if len(url) > 110:
            self.trap_links.append(url)
            return False
        if self.ignore_traps(url):
            self.trap_links.append(url)
            return False
        try:
            return ".ics.uci.edu" in parsed.hostname \
                      and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4"\
                                    + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                                    + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                                    + "|thmx|mso|arff|rtf|jar|csv" \
                                    + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower())

        except TypeError:
            print("TypeError for ", parsed)
            self.trap_links.append(url)
            return False


    def ignore_traps(self, url):
    
        query_list = re.split("[=|&]", urlparse(url).query)
        for word in query_list:
            if(word in  set(["login", "mailto", "edit", "download"])):
                return True

        split_list = re.split("[/|=|$|#|&|+|?|%]", url.lower())
        count = {}
        
        for i in split_list: 
            if(i in count):
                count[i] += 1
            else:
                count[i] = 1
                
        for key, val in count.items():
            if val > 3 or ((key == "day" or key == "month" or key == 'year') and count[key] >= 1):
                return True
        return False
