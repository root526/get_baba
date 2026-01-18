import scrapy
from download_jra_files.items import DownloadJraFilesItem
import os
import warnings


class DownloadJraFilesSpider(scrapy.Spider):
    name = "download_jra_files"
    allowed_domains = ["www.jra.go.jp"]
    # start_urls = ["https://www.jra.go.jp/"]

    def __init__(self, start_year, end_year, folder_path=None):
        self.year = int(start_year)
        self.end_year = int(end_year)
        self.folder_path = folder_path

    def start_requests(self):
        yield scrapy.Request(f"https://www.jra.go.jp/keiba/baba/archive/{self.year}.html", callback=self.parse, errback=self.handle_error)

    def handle_error(self, failure):
        warnings.warn(f"{self.year}ページの取得に失敗しました。")

        # 次の年に進む。
        self.year -= 1
        if self.year >= self.end_year:
            next_url = f"https://www.jra.go.jp/keiba/baba/archive/{self.year}.html"
            yield scrapy.Request(url=next_url, callback=self.parse, errback=self.handle_error)

    def parse(self, response):
        item = DownloadJraFilesItem()

        # -------- 既にダウンロード済のファイルを取得 --------
        listdir = os.listdir(self.folder_path) 
        urls_removed = [f"/keiba/baba/archive/{self.year}pdf/{x[4:]}" for x in listdir if str(self.year) in x] # 該当年度の既存ファイルのurlを取得

        # -------- ファイルのURLを取得 --------
        urls_rel = response.xpath('.//a[contains(@href, ".pdf")]/@href').getall()
        urls_rel = [url for url in urls_rel if url not in urls_removed] # ダウンロード済のファイルを除外

        # ------- itemにURLリストを格納してyield --------
        domain = "https://www.jra.go.jp"
        urls = [domain + url_rel for url_rel in urls_rel]
        item['file_urls'] = urls

        print(f"{self.year}年: 以下の{len(urls)}ファイルをダウンロードします。")
        for url in urls:
            print(url)

        yield item

        if self.year > self.end_year:
            self.year -= 1
            next_url = f"https://www.jra.go.jp/keiba/baba/archive/{self.year}.html"
            yield scrapy.Request(url=next_url, callback=self.parse)
