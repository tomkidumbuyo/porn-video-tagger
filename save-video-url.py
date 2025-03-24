import requests
from bs4 import BeautifulSoup
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import os

BASE_URL = 'https://sxyprn.com'

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

def get_posts_from_page(page_link):
    global count
    response = requests.get(page_link)
    soup = BeautifulSoup(response.text, "html.parser")
    search_results = soup.find_all(class_="search_results")
    post_links = [BASE_URL +a["href"] for result in search_results for a in result.find_all("a", href=True) if a["href"].startswith("/post/")]
    post_links = list(set(post_links))
    return post_links

def save_to_csv(df, filename):
    filename = filename.replace(".html", "-").replace("?", "-").replace("=", "-").replace("&", "-").replace("https:--sxyprn.com-", "").replace("--", "_")
    file_exists = os.path.exists(filename)
    df.to_csv(filename, mode='a', index=False, header=not file_exists)

def get_posts_from_category(category_link):
    print('category_link: ' + category_link)
    post_links = []
    response = requests.get(category_link)
    soup = BeautifulSoup(response.text, "html.parser")
    searches_container = soup.find('div', {'id': 'center_control'})
    pagination_elements = searches_container.find_all('a')
    pagination_element_links = [BASE_URL + elem.get('href') for elem in pagination_elements]

    with ThreadPoolExecutor(max_workers=300) as executor:
        results = executor.map(get_posts_from_page, pagination_element_links)

    for result in results:
        post_links += result
        df = pd.DataFrame(list(set(post_links)), columns=["links"])
        save_to_csv(df, 'data/category/' + category_link.replace("/", "-") + "-links.csv")
        print(post_links)
        
    return post_links


def process_page(i):
    post_links = []
    response = requests.get(BASE_URL + f'/searches/{i}.html')
    soup = BeautifulSoup(response.text, "html.parser")
    
    searches_container = soup.find('div', {'id': 'searches_container'})
    if not searches_container:
        return []

    category_link_elements = searches_container.find_all('a')
    category_links = [BASE_URL + elem.get('href') for elem in category_link_elements]

    df = pd.DataFrame(list(set(category_links)), columns=["categories"])
    save_to_csv(df, 'data/categories.csv')

    with ThreadPoolExecutor(max_workers=10) as executor:  
        results = executor.map(get_posts_from_category, category_links)

    for result in results:
        post_links += result

    return post_links

def get_post_links():
    post_links = []
    with ThreadPoolExecutor(max_workers=10) as executor:  
        results = executor.map(process_page, range(0, 26701, 150))

    for result in results:
        post_links += result

    return post_links

get_post_links()