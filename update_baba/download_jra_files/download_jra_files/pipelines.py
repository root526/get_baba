# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.pipelines.files import FilesPipeline


class DownloadJraFilesPipeline(FilesPipeline):
    def file_path(self, request, response=None, info=None, *, item=None):
        # file_paths = request.url.split("/")
        # file_paths.pop(0) # https:
        # file_paths.pop(0) #//
        url_split = request.url.split("/")
        file_name = url_split[-2][0:4] + url_split[-1]

        return file_name

