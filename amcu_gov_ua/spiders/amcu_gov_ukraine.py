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
        data_frame[column] = data_frame[column].apply(unidecode)  # Remove diacritics characters
        # data_frame[column] = data_frame[column].apply(remove_extra_spaces)  # Remove extra spaces and newline characters from each column
        if 'title' in column:
            data_frame[column] = data_frame[column].str.replace('–', '')  # Remove specific punctuation 'dash' from name string
            data_frame[column] = data_frame[column].str.translate(str.maketrans('', '', string.punctuation))  # Removing Punctuation from name text
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


# Function to remove Extra Spaces from Text
def remove_extra_spaces(text: str) -> str:
    return re.sub(pattern=r'\s+', repl=' ', string=text).strip()  # Regular expression to replace multiple spaces and newlines with a single space


def header_cleaner(header_text: str) -> str:
    header_text = header_text.strip()
    header = unidecode('_'.join(header_text.lower().split()))
    return header


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
        self.filename = fr"{self.excel_path}/{self.name}.xlsx"  # Filename with Scrape Date

        self.cookies = {
            '_ga': 'GA1.1.1573741425.1733199913',
            'ak_bmsc': 'B82DA2097DCF771E11A5D98105E07083~000000000000000000000000000000~YAAQTXLBF9Iob4WTAQAAGffEihnxxMYse1K6h8UXDLMV+r0PnYzxhQUr2POUf3C0cnNloicVPZykqmLvWsu0CCfbQIjmVlTQscdW2b+aI+TBV3QbPROYHmVHCeo4dmoOa3uNuodl1kZe2SdapfdOtbMGwyFm3FxRfp1ZDz1r2ccPGDoS6KIDrgWz8+WFsLcmrncOkU1VLBXus0iYhmmNiCEb60bxNaajU0g2lbPvboLNcDut+p6SbA1iA61gjeL0YN/WeJsg6vI280H20GlbQKxLDWrbbmXa8EwAygoB2WsYi9FAb4GhEcMt46C+mgw+iNVs7BvGf1hHIlP56+NOCZYH47LLOohox8JKyZLE0Nifqro02lGPZOHyui4Dl8VwA2o9Y1J7alNISbJiuYIQOhrsYUbCb0oL7j76djgFLWGQ3d7RjP/MoU3wRO9ukPD1DFodWWg+ZaIC5zCdmA==',
            'amcu_session': 'eyJpdiI6IjhHNlRyZENDYWlFb0ZxTCt6T0dTY0E9PSIsInZhbHVlIjoiZVR1VFd6NzJyZ3pMZ2c1ODRJVU5GQlNUQ1FQUXhJUmwrXC82dkx2b2hmUXVCWWdveGVkSXRJTDQwMGZxMUpsR3BMb0t3UFdySFI3cERWZlM5Vm9mdE1mT0N6d245T1wvdzA5OWk5ZFhRUEFuTlNxNTVXTXhpRXBsNUtcLzQzU01iM1YiLCJtYWMiOiJmNDhkOWYxMWE5ODgwNGVhZDY1MjJjZmQyOTJkY2VmZDZlNmY4YTk0ZGNjNzdmMjhkYWE4NWI0NmJkOWY0ZDY0In0%3D',
            '_ga_LQTC1RV9HS': 'GS1.1.1733199912.1.1.1733202655.0.0.0',
            'bm_sv': '46FB3AD3CE4BC60967BBA1F02D5C5381~YAAQlzYQYCs6onSTAQAAlMjuihkrRizitXfl+id7RSW2Ap+Gqo/t3goePi4p+NsZDwv4+oNN+tAm/pCa4Bm+gRUA2njc3a4YtRZ+cbODS9lgJsH+cgJFkbGOgpAv/zia0wsKo17vX2iHzHmjToGdXe5IEHUX2IY0PWayPRHY2h6UXPe7Upuqrfc16OAMfq6uPzme27mT0V6Axo11yMbXk8nvpYm34QDPqNSBg8CHp5/Zzz/SqKjOHWDxcqw6sKDs0KRu~1',
        }

        self.headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'application/json',
            'priority': 'u=1, i',
            # 'referer': 'https://amcu.gov.ua/timeline?&type=posts&category_id=2',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'x-csrf-token': 'mCw8f051uheZDXlwml76Ao5NV5qUdNh9XeCPuQ2b',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        }
        self.browsers = ["chrome110", "edge99", "safari15_5"]

        self.detail_headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9',
            'priority': 'u=0, i',
            # 'referer': 'https://amcu.gov.ua/timeline?&type=posts&category_id=2',
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

    def start_requests(self) -> Iterable[Request]:
        browsers = ["chrome110", "edge99", "safari15_5"]
        params = {'page': '1', 'type': 'posts', 'category_id': '2', 'lang': 'uk'}
        url = 'https://amcu.gov.ua/api/timeline?' + parse.urlencode(params)
        yield scrapy.Request(url=url, cookies=self.cookies, headers=self.headers, method='GET', meta={'impersonate': random.choice(browsers)},
                             callback=self.parse, dont_filter=True, cb_kwargs={'params': params})

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
                    data_dict['url'] = response.url
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
            next_page_url = ' '.join(json_dict.get('next_page_url'))
            if next_page_url:
                parsed_url = parse.urlparse(next_page_url)  # Parse the URL
                query_params = parse.parse_qs(parsed_url.query)  # Extract query parameters
                page = query_params.get('page', [None])[0]  # # Get the value of the 'page' parameter & Use [None] as default if 'page' doesn't exist
                new_params = params.copy()
                new_params['page'] = page

                print('Sending request on next page', page)
                next_url = 'https://amcu.gov.ua/api/timeline?' + parse.urlencode(new_params)
                yield scrapy.Request(url=next_url, cookies=self.cookies, headers=self.headers, method='GET',
                                     callback=self.parse, dont_filter=True, cb_kwargs={'params': new_params})
            else:
                print('No More Pagination found.')
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
        print(data_dict)
        self.final_data_list.append(data_dict)

    def close(self, reason):
        print('closing spider...')
        print("Converting List of Dictionaries into DataFrame, then into Excel file...")
        try:
            print("Creating Native sheet...")
            data_df = pd.DataFrame(self.final_data_list)
            data_df = df_cleaner(data_frame=data_df)  # Apply the function to all columns for Cleaning
            data_df.insert(loc=0, column='id', value=range(1, len(data_df) + 1))  # Add 'id' column at position 1
            # data_df.set_index(keys='id', inplace=True)  # Set 'id' as index for the Excel output
            with pd.ExcelWriter(path=self.filename, engine='xlsxwriter', engine_kwargs={"options": {'strings_to_urls': False}}) as writer:
                data_df.to_excel(excel_writer=writer, index=False)

            print("Native Excel file Successfully created.")
        except Exception as e:
            print('Error while Generating Native Excel file:', e)
        if self.api.is_connected:  # Disconnecting VPN if it's still connected
            self.api.disconnect()

        end = time.time()
        print(f'Scraping done in {end - self.start} seconds.')


if __name__ == '__main__':
    execute(f'scrapy crawl {AmcuGovUkraineSpider.name}'.split())
