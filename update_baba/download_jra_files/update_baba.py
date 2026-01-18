import os 
import yaml
from datetime import datetime
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from download_jra_files.spiders.download_jra_files_spider import DownloadJraFilesSpider

import pandas as pd
import tabula

import logging 
logging.getLogger("tabula").setLevel(logging.ERROR)
logging.getLogger('scrapy').setLevel(logging.ERROR)

place_eng_to_code = {
    'sapporo': '01',
    'hakodate': '02',
    'fukushima': '03',
    'niigata': '04',
    'tokyo': '05',
    'nakayama': '06',
    'chukyo': '07',
    'kyoto': '08',
    'hanshin': '09',
    'kokura': '10',
}


def get_config():
    with open("config.yaml", "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return config

def get_folder_path(config):
    return config["FolderPath"]

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



def from_pdf_to_csv(file_name, folder_path):
    # ------------------------PDFから表を取得-------------------------
    file_path = os.path.join(folder_path, file_name)
    df_condition = tabula.read_pdf(file_path, pages=1, lattice=True, multiple_tables=False)[0]
    df_condition = df_condition.reset_index()
    df_condition.columns = ["開催日次", "測定月日", "曜日", "使用コース", "芝コースクッション値測定時刻", "芝コースクッション値", "含水率測定時刻", "芝ゴール前", "芝4コーナー", "ダートゴール前", "ダート4コーナー"]
    df_condition = df_condition[df_condition["開催日次"].str.contains(r"第.*日", na=False)]

    # ------------------------レースの数だけ複製し、レース番号を追加-------------------------
    df_condition = df_condition.loc[df_condition.index.repeat(12)].assign(レース番号=list(range(1, 13))*len(df_condition)).reset_index(drop=True)
    df_condition['レース番号'] = df_condition['レース番号'].astype(str).str.zfill(2)

    # ------------------------レースIDを作成-------------------------
    year = file_name[:4]
    place_eng = file_name.split(".")[0][4:-2]
    place_code = place_eng_to_code[place_eng]
    kaiji = file_name.split(".")[0][-2:]

    df_condition["NN"] = df_condition["開催日次"].astype(str).str.extract(r"第.*?(\d+)日", expand=False).str.zfill(2)
    tmp = df_condition["測定月日"].astype(str).str.extract(r".*?(\d+)月.*?(\d+)日")
    df_condition["MMDD"] = tmp[0].str.zfill(2) + tmp[1].str.zfill(2)

    df_condition["レースID"] = year + df_condition["MMDD"] + place_code + kaiji + df_condition["NN"] + df_condition["レース番号"]

    return df_condition[["レースID", "芝コースクッション値", "芝ゴール前", "ダートゴール前"]]

def update_csv(config, files_added, folder_path):

    df_concat = pd.DataFrame()
    for file in files_added:
        df_condition = from_pdf_to_csv(file, folder_path)
        df_concat = pd.concat([df_concat, df_condition], ignore_index=True)

    # ------------------------必要な列を抽出してCSVに保存（出走頭数分の複製が必要）-------------------------
    df_cushion = df_concat[["レースID", "芝コースクッション値"]]
    for i in range(17):
        df_cushion.loc[f"copy_{i}"] = df_cushion["芝コースクッション値"]

    df_turf_moisture = df_concat[["レースID", "芝ゴール前"]]
    for i in range(17):
        df_turf_moisture.loc[f"copy_{i}"] = df_turf_moisture["芝ゴール前"]

    df_dirt_moisture = df_concat[["レースID", "ダートゴール前"]]
    for i in range(17):
        df_dirt_moisture.loc[f"copy_{i}"] = df_dirt_moisture["ダートゴール前"]
    csv_folder = config["csvFolder"]
    csv_name = config["csvName"]
    cushion_path = os.path.join(csv_folder, f"{csv_name}_芝クッション値.csv")
    turf_moisture_path = os.path.join(csv_folder, f"{csv_name}_芝含水率.csv")
    dirt_moisture_path = os.path.join(csv_folder, f"{csv_name}_ダート含水率.csv")

    if os.path.exists(cushion_path):
        df_cushion_existing = pd.read_csv(cushion_path, header=None)
        df_cushion = pd.concat([df_cushion_existing, df_cushion], ignore_index=True)
        df_cushion.to_csv(cushion_path, index=False, header=False)
    else:
        df_cushion.to_csv(cushion_path, index=False, header=False)

    if os.path.exists(turf_moisture_path):
        df_turf_moisture_existing = pd.read_csv(turf_moisture_path, header=None)
        df_turf_moisture = pd.concat([df_turf_moisture_existing, df_turf_moisture], ignore_index=True)
        df_turf_moisture.to_csv(turf_moisture_path, index=False, header=False)
    else:
        df_turf_moisture.to_csv(turf_moisture_path, index=False, header=False)

    if os.path.exists(dirt_moisture_path):
        df_dirt_moisture_existing = pd.read_csv(dirt_moisture_path, header=None)
        df_dirt_moisture = pd.concat([df_dirt_moisture_existing, df_dirt_moisture], ignore_index=True)
        df_dirt_moisture.to_csv(dirt_moisture_path, index=False, header=False)
    else:
        df_dirt_moisture.to_csv(dirt_moisture_path, index=False, header=False)


def main():
    config = get_config()
    folder_path = get_folder_path(config)
    start_year = determine_start_year()
    end_year = determine_end_year(folder_path=folder_path)

    files_before = set(os.listdir(folder_path))

    execute_crawler(start_year, end_year, folder_path)
    print("クロールが終了しました。")

    files_after = set(os.listdir(folder_path))
    files_added = files_after - files_before

    update_csv(config, files_added, folder_path)
    print("CSVファイルの更新が終了しました。")

if __name__ == "__main__":
    main()