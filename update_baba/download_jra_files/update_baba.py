import os 
# import sys
from datetime import datetime
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from download_jra_files.spiders.download_jra_files_spider import DownloadJraFilesSpider

import pandas as pd
import tabula

def get_folder_path(): # 後で決める
    pass 

def determine_start_year():
    return datetime.now().year

def determine_end_year(folder_path):
    listdir = os.listdir(folder_path)
    years = [int(f[0:4]) for f in listdir if f.endswith(".pdf")]
    if years:
        end_year = min(years)
    else:
        end_year = 2025  # Default to 2025 if no files are found
    return end_year

def execute_crawler(start_year, end_year, folder_path):
    settings = get_project_settings()
    settings.set("FILES_STORE", folder_path)

    process = CrawlerProcess(settings)
    process.crawl(DownloadJraFilesSpider, start_year=start_year, end_year=end_year, folder_path=folder_path)
    process.start()



def create_csv(file_name, folder_path):
    file_path = os.path.join(folder_path, file_name)
    df_condition = tabula.read_pdf(file_path, pages=1, lattice=True, multiple_tables=False)[0]
    df_condition = df_condition.reset_index()
    df_condition.columns = ["開催日次", "測定月日", "曜日", "使用コース", "芝コースクッション値測定時刻", "芝コースクッション値", "含水率測定時刻", "芝ゴール前", "芝4コーナー", "ダートゴール前", "ダート4コーナー"]
    df_condition = df_condition[df_condition["開催日次"].str.contains(r"第.*日", na=False)]



def main():
    folder_path = get_folder_path()
    start_year = determine_start_year()
    end_year = determine_end_year(folder_path=folder_path)

    execute_crawler(start_year, end_year, folder_path)
    print("クロールが終了しました。")

