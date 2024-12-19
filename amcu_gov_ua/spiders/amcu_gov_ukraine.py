import subprocess

import unicodedata
from scrapy.cmdline import execute
from unidecode import unidecode
from datetime import datetime
from lxml.html import fromstring
from typing import Iterable
from scrapy import Request
import pandas as pd
from urllib import parse
import random
import string
import scrapy
import json
import time
import evpn
import os
import re


def df_cleaner(data_frame: pd.DataFrame) -> pd.DataFrame:
    print('Cleaning DataFrame...')
    data_frame = data_frame.astype(str)  # Convert all data to string
    data_frame.drop_duplicates(inplace=True)  # Remove duplicate data from DataFrame

    # Apply the function to all columns for Cleaning
    for column in data_frame.columns:
        data_frame[column] = data_frame[column].apply(set_na)  # Setting "N/A" where data is empty string
        data_frame[column] = data_frame[column].apply(remove_diacritics)  # Remove diacritics characters
        # data_frame[column] = data_frame[column].apply(remove_extra_spaces)  # Remove extra spaces and newline characters from each column
        if 'title' in column:
            data_frame[column] = data_frame[column].str.replace('–', '')  # Remove specific punctuation 'dash' from name string
            # data_frame[column] = data_frame[column].str.translate(str.maketrans('', '', string.punctuation))  # Removing Punctuation from name text
            data_frame[column] = data_frame[column].apply(remove_punctuation)  # Removing Punctuation from name text
        data_frame[column] = data_frame[column].apply(remove_extra_spaces)  # Remove extra spaces and newline characters from each column

    data_frame.replace(to_replace='nan', value=pd.NA, inplace=True)  # After cleaning, replace 'nan' strings back with actual NaN values
    data_frame.fillna(value='N/A', inplace=True)  # Replace NaN values with "N/A"
    print('DataFrame Cleaned...!')
    return data_frame


def set_na(text: str) -> str:
    # Remove extra spaces (assuming remove_extra_spaces is a custom function)
    text = remove_extra_spaces(text=text)
    pattern = r'^([^\w\s]+)$'  # Define a regex pattern to match all the conditions in a single expression
    text = re.sub(pattern=pattern, repl='N/A', string=text)  # Replace matches with "N/A" using re.sub
    return text


# Function to remove all punctuation
def remove_punctuation(text):
    return text if text == 'N/A' else ''.join(char for char in text if not unicodedata.category(char).startswith('P'))


# Function to remove Extra Spaces from Text
def remove_extra_spaces(text: str) -> str:
    return re.sub(pattern=r'\s+', repl=' ', string=text).strip()  # Regular expression to replace multiple spaces and newlines with a single space


def header_cleaner(header_text: str) -> str:
    header_text = header_text.strip()
    header = unidecode('_'.join(header_text.lower().split()))
    return header


def remove_diacritics(input_str):
    return ''.join(char for char in unicodedata.normalize('NFD', input_str) if not unicodedata.combining(char))


# Function Convert a list of dates from 'DD Month YYYY' format to 'YYYY/MM/DD' format.
def get_news_date(date_key: str) -> str:
    if date_key not in ['N/A', '']:
        date_obj = datetime.strptime(date_key, "%d.%m.%Y")  # Parse the date using datetime.strptime
        formatted_date = date_obj.strftime(format="%Y-%m-%d").strip()  # Convert to desired format
        return formatted_date
    else:
        return date_key


def get_news_time(news_dict: dict) -> str:
    news_time = news_dict.get('time', 'N/A').strip()
    return news_time if news_time != '' else 'N/A'


def get_detail_page_url(news_dict: dict) -> str:
    detail_page_url = news_dict.get('url', 'N/A').strip()
    return detail_page_url if detail_page_url != '' else 'N/A'


def get_title(news_dict: dict) -> str:
    title = news_dict.get('title', 'N/A').strip()
    return title if title != '' else 'N/A'


def get_tag_name(news_dict):
    tag_name = ' | '.join([tag_slug.get('name', 'N/A') for tag_slug in news_dict.get('tags', [])]).strip()
    return tag_name if tag_name != '' else 'N/A'


def get_tag_url(news_dict: dict) -> str:
    tag_url = ' | '.join(['https://amcu.gov.ua' + tag_slug.get('url', 'N/A') for tag_slug in news_dict.get('tags', [])]).strip()
    return tag_url if tag_url != '' else 'N/A'


def get_desctription(parsed_tree):
    desctription = ' '.join(parsed_tree.xpath('//p[not(@class)]//text() | //p/following-sibling::ul[not(@class)]/li//text()')).strip()
    return desctription if desctription != '' else 'N/A'


def get_image_url(parsed_tree):
    image_url_slugs = parsed_tree.xpath('//p/img/@src')
    image_url = ' | '.join('https://amcu.gov.ua' + url_slug for url_slug in image_url_slugs)
    return image_url if image_url != '' else 'N/A'


def get_external_url(parsed_tree):
    external_url = ' | '.join(parsed_tree.xpath('//p[not(@class)]//a/@href'))
    return external_url if external_url != '' else 'N/A'


class AmcuGovUkraineSpider(scrapy.Spider):
    name = "amcu_gov_ukraine"

    def __init__(self):
        self.start = time.time()
        super().__init__()
        print('Connecting to VPN (UKRAINE)')
        self.api = evpn.ExpressVpnApi()  # Connecting to VPN (UKRAINE)
        self.api.connect(country_id='87')  # UKRAINE country code for vpn
        time.sleep(5)  # keep some time delay before starting scraping because connecting
        print('VPN Connected!' if self.api.is_connected else 'VPN Not Connected!')

        # self.delivery_date = datetime.now().strftime('%Y%m%d')
        self.final_data_list = list()  # List of data to make DataFrame then Excel

        # Path to store the Excel file can be customized by the user
        self.excel_path = r"../Excel_Files"  # Client can customize their Excel file path here (default: govtsites > govtsites > Excel_Files)
        os.makedirs(self.excel_path, exist_ok=True)  # Create Folder if not exists
        self.filename_native = fr"{self.excel_path}/{self.name}_native.xlsx"  # Filename with Scrape Date
        self.filename_translated = fr"{self.excel_path}/{self.name}_translated.xlsx"  # Filename with Scrape Date

        self.browsers = ["chrome110", "edge99", "safari15_5"]
        self.detail_headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9',
            'priority': 'u=0, i',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        }
        self.detail_cookies = {
            'bm_mi': 'E7C81527FE2976DD13171FB26721DA48~YAAQjDYQYFI/kVCTAQAArwA/ixlS7JRvYE8/ByGHaGguqp6/0pGNBZXkWZJwCakCNIMi+gjmnB9PIocTmTUbVpeRXh2+AC9OnhwVMuOKlCQxrId2G6nAVx7AWhBsjePALpenM4D0BeGanyCyW7LPOfnh/Igeb+Y4RNJF+onlHnwkixH5CwDXnkDX2oRloJJD32ASuMHU975woSIJmll1FOnY5cC0xHCsVrHYl5dIII1SrKIX/RXlo30dicxd5jadCVJlWUBMNJoUlfpvbdC+yDHayn4j7Io1gpa2O6yfjIPvOAx7CgrDwvjuHcVUuHGnaf1rLgLTHLsnowR8C68iknxpJ4w4p450RwTmX5dZQ+1QQfWt0b8+7ZEuNaeWuKbO7bzSwNosedkwa3kLh3YsHrS7jkvSUK6uO4GTPsEAcRsoqWTek7Gp~1',
            '_ga': 'GA1.1.722777445.1733207915',
            'ak_bmsc': 'E5D316FE0C1E88BBE676B8B26B677CDE~000000000000000000000000000000~YAAQjDYQYAhBkVCTAQAAewk/ixlux+rCvlS47ICE3hlw3/HStex2dSNGAGZhJzLkj9BrzZB4tl7nCYzznOm+seo6liUqc1rPKRlkBVbnCvSrR2/n+moiTJBoG+qdVrEwoFvl95+6iESXkoC81DimG4NMjwyJaANeB97fQtitjvM89L+uoAbQevpOBIP7d3sacQiqhazHI+bfIK/TDHUVYRVxixYVjn4h1SWyTGTdVuaGGO9iQWM1ZUw9r133/rbfipeHnSgZWCHGmd0BM3GKi8lfX/ZnsnBICdgPhW5KK2LCGP/z1t7pRYknxgvRn28bRczQ2pc36KQ+ZoBWxtfgZboS1S/LlQFQJjcLu0R7CKt08yA3nvrKnan3Kjnkj6clM9mmSYZpUmg45qrgVOlc+vjzHmEIFLyZMy9GpgO6cha1GI9gFhC//tagO5SV8fhupgpSDxJR43bk+uhmt75NINvX9uuHaCC3dv0yM1xUOVaGDzsDKoi97r57MYr524UT2R0N0Amvv6Kgl6Qbjw26Yfsfwon2oXsNvlc9TH97uJz4kVs3xfxEFweS7GFhCLvQ6IvjwgAPbRwfb75Sd6KbuNcEySR2maw=',
            'amcu_session': 'eyJpdiI6IlwvNUtqZlNNa3RXektwWmI2MVpCaStnPT0iLCJ2YWx1ZSI6Ik1QT2orUnFRdkNiUmlJWFkxMUZDR2pSRU9Ya1hXb2xlU3hiVmhGd29ObDBrK3hxdkhTWllPOEYyY2VteExiYjVhSFhBMk4zekZyRGpDUE5WcjZVK3lVSFwvbzZ1MVwvSnV4NktjTWtNK2FIWVBneXY3TzhoXC9OM2hRQVdhaG4zNXNCIiwibWFjIjoiZDAzZWI0Zjc5YTljM2JmNjk4MjllM2EwYjFlY2Y0NmIwYTkxOTYwODViNDY3YzAzZTFlNjg4ZjllZjFmMWNmNyJ9',
            '_ga_LQTC1RV9HS': 'GS1.1.1733207915.1.1.1733213295.0.0.0',
            'bm_sv': '38AC5521C2769F556991AD57702374FD~YAAQLXLBFy+PKTeTAQAAaViRixk+xsop1w/Mtlk4f9eas0+PeZtUbIAUsgWkUPu5sIpCQBYW6PEsLaLeF4LEhqopnlT8lREHJMj7hLYOuIKr7O2TR7JtZddlyqEwT9Vy7aYPSrqGOQLGry3sQXpTqKdvC4lfwGfpf8i+bNkOr5wWELf8WzPzoJLp4OwvNWhaD0aTr1RG/xkXhgdAHhPp+rn6fICYM6J6mQYB451DWOUq/tnPZjmVraEqn1egmX8LZNPH~1',
        }

        self.browsers = ["chrome110", "edge99", "safari15_5"]

    def start_requests(self) -> Iterable[Request]:

        cookies = {
            '_ga_LQTC1RV9HS': 'GS1.1.1734603890.2.0.1734603890.0.0.0',
            'ak_bmsc': '4060CF4EB31BAB2E31022DEBCF1BC353~000000000000000000000000000000~YAAQjDYQYI7OybqTAQAA1Pd23hr0s5HNNOer80n4fDyyD77gwhC4oClPIG1CUtNvozVvxo9HTjXYclfF54XNbcRc5tOopNGBjRs8d0HY+as7wkzgW5iUDNcZsoJIYyovR9VZ4MXvftZP6e927u1e2rc/HCYg+R/QkrLC8dph/Ki+MMhMGfkQLJocSnCwG5tkV5WdFV/+b+Lo8I7rmBS8YH492FfoJw9Ayupd+jDDIseB34ptpCsP9+CtDetqBIpyCiFAWFlKYVnLWI2qdegM7TWlrUYbKiT7xIRjgxNadUBTgnGhd+V6zWk7kbbVIsUOiPvCS/Hgei+B4mi92UrxzQ5NGjSFJa7XA9AxCKaORWayemiAfcnuGcd/RccKzH4T9j+T7O3/x13PSA==',
            'amcu_session': 'eyJpdiI6InZYbUVwSitiVWVUTElFZnByZHRqZHc9PSIsInZhbHVlIjoidWduYjhHTlhJYTR4dWE1bk5tNHBzd2hpbTFMQUJXcVJBWGIxNUFFeUJUS2VqWUJmWkhzRU5kTlF2eUErXC9kTDJlWjlKN0trcGRJY3R1bEw0Qng5cU5lVndxVkhNeXVSNzFoV1JQYTRxVzhXQnhKR3ZQQXhWQWVTNzVIV0NGWlRSIiwibWFjIjoiMWFmOGQ3YTY3YzdiOWRjYjgyNmJlMDZmODM3OTQyMTNmODJlMDRhMWI2MzIyYzc5N2E0OGM2YmNhNmRkZTk4ZiJ9',
            'bm_sv': '2132488AB7FC63A7C195C1DD61EC9903~YAAQjDYQYJngybqTAQAAsSN33hqmlSLF3Ub+RccgGiqxNe1Fl8iGf8erImnCrrIrbTiIscGNiupG6KqzxKbE3gOHB3FCE5iUn2CKFautZKlHO7LTpicwXWVElUX6OZQR5u4ftgQkmTHXbucUAsuHziAPdga+1U+DkqgOgoXlvTn3zNvSBd57GTDkS4NAPBeZAnZqOVUc0zykCD0IeXs/VdzkY0DQdwXkxRUWxqVso6oCtbowGEs29p+rjC+M14hAkw==~1',
        }
        headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'application/json',
            'priority': 'u=1, i',
            'referer': 'https://amcu.gov.ua/timeline?&type=posts&category_id=2',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'x-csrf-token': 'mP3z2X0e1pzP1dUCR0TeHgtj2nPHcMREWoWJBPaA',
        }
        params = {'page': '1', 'type': 'posts', 'category_id': '2', 'lang': 'uk'}
        url = 'https://amcu.gov.ua/api/timeline'
        yield scrapy.FormRequest(url=url, method="GET", formdata=params,  # This passes query parameters.
                                 cookies=cookies, headers=headers, callback=self.parse, meta={'impersonate': random.choice(self.browsers)},
                                 cb_kwargs={'params': params})

    def parse(self, response, **kwargs):
        if response.status == 200:
            params: dict = kwargs['params']
            json_dict: dict = json.loads(response.text)
            json_data_dicts: dict = json_dict.get('data', {})

            print("Extracting details page url and some data from page:", params['page'])
            # Extract details page url and some data here...
            for date_key in json_data_dicts:
                news_dict_lists = json_data_dicts[date_key]
                for news_dict in news_dict_lists:
                    data_dict = dict()
                    detail_page_url = get_detail_page_url(news_dict)
                    print('detail_page_url:', detail_page_url)
                    data_dict['url'] = 'https://amcu.gov.ua/timeline?&type=posts&category_id=2'
                    data_dict['detail_page_url'] = detail_page_url
                    data_dict['title'] = get_title(news_dict)
                    data_dict['tag_name'] = get_tag_name(news_dict)
                    data_dict['tag_url'] = get_tag_url(news_dict)
                    data_dict['news_date'] = get_news_date(date_key)
                    data_dict['news_time'] = get_news_time(news_dict)

                    # Request on details page for each criminal
                    yield scrapy.Request(url=detail_page_url, cookies=self.detail_cookies, headers=self.detail_headers, method='GET', callback=self.detail_page_parse,
                                         dont_filter=True, meta={'impersonate': random.choice(self.browsers)}, cb_kwargs={'params': params, 'data_dict': data_dict})

            # Find the URL of the next page & Handle Pagination
            next_page_url = ' '.join(json_dict.get('next_page_url', 'N/A'))
            if next_page_url:
                print('next_page_url:', next_page_url)
                parsed_url = parse.urlparse(next_page_url)  # Parse the URL
                query_params = parse.parse_qs(parsed_url.query)  # Extract query parameters
                page = query_params.get('page', [None])[0]  # # Get the value of the 'page' parameter & Use [None] as default if 'page' doesn't exist
                new_params = params.copy()
                new_params['page'] = str(page)

                cookies = {
                    '_ga_LQTC1RV9HS': 'GS1.1.1734603890.2.0.1734603890.0.0.0',
                    'ak_bmsc': '4060CF4EB31BAB2E31022DEBCF1BC353~000000000000000000000000000000~YAAQjDYQYI7OybqTAQAA1Pd23hr0s5HNNOer80n4fDyyD77gwhC4oClPIG1CUtNvozVvxo9HTjXYclfF54XNbcRc5tOopNGBjRs8d0HY+as7wkzgW5iUDNcZsoJIYyovR9VZ4MXvftZP6e927u1e2rc/HCYg+R/QkrLC8dph/Ki+MMhMGfkQLJocSnCwG5tkV5WdFV/+b+Lo8I7rmBS8YH492FfoJw9Ayupd+jDDIseB34ptpCsP9+CtDetqBIpyCiFAWFlKYVnLWI2qdegM7TWlrUYbKiT7xIRjgxNadUBTgnGhd+V6zWk7kbbVIsUOiPvCS/Hgei+B4mi92UrxzQ5NGjSFJa7XA9AxCKaORWayemiAfcnuGcd/RccKzH4T9j+T7O3/x13PSA==',
                    'amcu_session': 'eyJpdiI6InZYbUVwSitiVWVUTElFZnByZHRqZHc9PSIsInZhbHVlIjoidWduYjhHTlhJYTR4dWE1bk5tNHBzd2hpbTFMQUJXcVJBWGIxNUFFeUJUS2VqWUJmWkhzRU5kTlF2eUErXC9kTDJlWjlKN0trcGRJY3R1bEw0Qng5cU5lVndxVkhNeXVSNzFoV1JQYTRxVzhXQnhKR3ZQQXhWQWVTNzVIV0NGWlRSIiwibWFjIjoiMWFmOGQ3YTY3YzdiOWRjYjgyNmJlMDZmODM3OTQyMTNmODJlMDRhMWI2MzIyYzc5N2E0OGM2YmNhNmRkZTk4ZiJ9',
                    'bm_sv': '2132488AB7FC63A7C195C1DD61EC9903~YAAQjDYQYJngybqTAQAAsSN33hqmlSLF3Ub+RccgGiqxNe1Fl8iGf8erImnCrrIrbTiIscGNiupG6KqzxKbE3gOHB3FCE5iUn2CKFautZKlHO7LTpicwXWVElUX6OZQR5u4ftgQkmTHXbucUAsuHziAPdga+1U+DkqgOgoXlvTn3zNvSBd57GTDkS4NAPBeZAnZqOVUc0zykCD0IeXs/VdzkY0DQdwXkxRUWxqVso6oCtbowGEs29p+rjC+M14hAkw==~1',
                }
                headers = {
                    'accept': '*/*',
                    'accept-language': 'en-US,en;q=0.9',
                    'content-type': 'application/json',
                    'priority': 'u=1, i',
                    'referer': 'https://amcu.gov.ua/timeline?&type=posts&category_id=2',
                    'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                    'sec-fetch-dest': 'empty',
                    'sec-fetch-mode': 'cors',
                    'sec-fetch-site': 'same-origin',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                    'x-csrf-token': 'mP3z2X0e1pzP1dUCR0TeHgtj2nPHcMREWoWJBPaA',
                }

                print('Sending request on next page', page)
                yield scrapy.FormRequest(url='https://amcu.gov.ua/api/timeline', method="GET", formdata=new_params,  # This passes query parameters.
                                         cookies=cookies, headers=headers, callback=self.parse, meta={'impersonate': random.choice(self.browsers)},
                                         cb_kwargs={'params': new_params})
            else:
                print(f'No More Pagination found after {params['page']}')
            print('+' * 100)
        else:
            print(f'Http Status code: {response.status}, Response text: {response.text}')

    def detail_page_parse(self, response, **kwargs):
        params = kwargs['params']
        data_dict = kwargs['data_dict']
        parsed_tree = fromstring(response.text)  # Parse the HTML

        # Extract your data here...
        print("Extracting Detail data from page:", params['page'])
        data_dict['description'] = get_desctription(parsed_tree)
        data_dict['image_url'] = get_image_url(parsed_tree)
        data_dict['external_url'] = get_external_url(parsed_tree)
        # print(data_dict)
        self.final_data_list.append(data_dict)

    def close(self, reason):
        print('closing spider...')
        if self.final_data_list:
            try:
                print("Creating Native sheet...")
                native_data_df = pd.DataFrame(self.final_data_list)
                native_data_df = df_cleaner(data_frame=native_data_df)  # Apply the function to all columns for Cleaning
                native_data_df.insert(loc=0, column='id', value=range(1, len(native_data_df) + 1))  # Add 'id' column at position 1
                with pd.ExcelWriter(path=self.filename_native, engine='xlsxwriter', engine_kwargs={"options": {'strings_to_urls': False}}) as writer:
                    native_data_df.to_excel(excel_writer=writer, index=False)
                print("Native Excel file Successfully created.")
            except Exception as e:
                print('Error while Generating Native Excel file:', e)

            # Run the translation script with filenames passed as arguments
            try:
                subprocess.run(
                    args=["python", "translate_and_save.py", self.filename_native, self.filename_translated],  # Define the filenames as arguments
                    check=True
                )
                print("Translation completed successfully.")
            except subprocess.CalledProcessError as e:
                print(f"Error during translation: {e}")
        else:
            print('Final-Data-List is empty.')
        if self.api.is_connected:  # Disconnecting VPN if it's still connected
            self.api.disconnect()
        end = time.time()
        print(f'Scraping done in {end - self.start} seconds.')


if __name__ == '__main__':
    execute(f'scrapy crawl {AmcuGovUkraineSpider.name}'.split())
