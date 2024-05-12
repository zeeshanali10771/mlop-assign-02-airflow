import requests
from bs4 import BeautifulSoup
import csv
import re
import time

PURPLE = "\033[94m"
GREEN = "\033[92m"
RED = "\033[91m"
RESET = "\033[0m"

def extract_data(url):
    start_time = time.strftime("%Y%m%d-%H%M%S")
    # fetch html
    print(f"\nExtracting data from {PURPLE}{url}{RESET}")
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    # get links
    links = [link.get('href') for link in soup.find_all('a', href=True)]

    # get titles and descriptions
    articles = soup.find_all('article')
    article_data = []
    for idx, article in enumerate(articles):
        title = article.find('h2').text.strip() if article.find('h2') else None
        description = article.find('p').text.strip() if article.find('p') else None
        article_data.append({'id': idx+1, 'title': title, 'description': description, 'source': url})

    # extraction time duration
    end_time = time.strftime("%Y%m%d-%H%M%S")
    print(f"Extracted data in {RED}{calculate_duration(start_time, end_time)}{RESET} seconds")

    # amount of data extracted
    print(f"Extracted {RED}{len(article_data)}{RESET} articles and {RED}{len(links)}{RESET} links from {PURPLE} {url} {RESET}")
    return links, article_data

def save_to_csv(file_name, articles):
    with open(file_name, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['id', 'title', 'description', 'source']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for article in articles:
            writer.writerow(article)

def preprocess(text):
    # Remove HTML tags
    clean_text = re.sub('<.*?>', '', text)
    # Remove special characters and digits
    clean_text = re.sub('[^a-zA-Z]', ' ', clean_text)
    # Convert to lowercase
    clean_text = clean_text.lower()
    # remove extra spaces
    clean_text = re.sub(' +', ' ', clean_text)
    return clean_text

def clean_data(data, url):
    print(f"Cleaning data from {PURPLE}{url}{RESET}\n")
    for article in data:
        article['title'] = preprocess(article['title']) if article.get('title') else None
        article['description'] = preprocess(article['description']) if article.get('description') else None
    return data

def calculate_duration(start_time, end_time):
    start = time.strptime(start_time, "%Y%m%d-%H%M%S")
    end = time.strptime(end_time, "%Y%m%d-%H%M%S")
    duration = time.mktime(end) - time.mktime(start)
    return duration

def main():
    dawn_url = 'https://www.dawn.com/'
    bbc_url = 'https://www.bbc.com/'
    file_name = "./data/extracted.csv"

    dawn_links, dawn_articles = extract_data(dawn_url)
    dawn_data = clean_data(dawn_articles, dawn_url)

    bbc_links, bbc_articles = extract_data(bbc_url)
    bbc_data = clean_data(bbc_articles, bbc_url)

    data = dawn_data + bbc_data
    save_to_csv(file_name, data)

    print(f"Data saved to{GREEN} {file_name} {RESET}\n")

if __name__ == "__main__":
    main()