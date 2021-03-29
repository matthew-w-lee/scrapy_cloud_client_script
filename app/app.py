
import sys
from scrapinghub import ScrapinghubClient
import datetime as dt
import os
import pandas as pd

class ScrapyCloudClient:

    def __init__(self):
        self.apikey = '' # your API key as a string
        self.client = ScrapinghubClient(self.apikey)
        self.project_num = 0
        self.project = self.client.get_project(self.project_num)
        self.neighborhood_spider = self.get_neighborhood_spider()
        self.listing_spider = self.get_listing_spider()
        self.airdna_spider = self.get_airdna_spider()

    def get_neighborhood_spider(self):
        return ScrapyCloudNeighborhoodSearchSpider(self.project.spiders.get('neighborhood_search'))

    def get_listing_spider(self):
        return ScrapyCloudSpider(self.project.spiders.get('listing'))

    def get_airdna_spider(self):
        return ScrapyCloudSpider(self.project.spiders.get('airdna'))

    def listing_ids(self):
        all_ids = self.neighborhood_spider.get_listing_ids()
        print(len(all_ids))
        id_string = ""
        for num, i in enumerate(all_ids):
            if num == 0:
                id_string = str(i)
            else:
                id_string = id_string + "," + str(i)
        return id_string

class ScrapyCloudSpider:

    def __init__(self, spider):
        self.spider = spider
        self.directory_string = "scrape_results/{}".format(self.spider.name)
        self.directory = os.fsencode(self.directory_string)

    def run(self, args=None):
        job = self.spider.jobs.run(job_args = args)
        print(dict(job.metadata.iter()))

    def get_job_keys(self):
        return [j['key'] for j in self.spider.jobs.iter()]

    def get_items_from_spider(self):
        items = []
        for key in self.get_job_keys():
            for item in self.spider.jobs.get(key).items.iter():
                items.append(item)
        return items

    def save_to_file(self):
        filename_job_keys = [os.fsdecode(file).split(" ")[0] for file in os.listdir(self.directory)]
        for job in self.spider.jobs.iter():
            key = job['key']
            filename_key = key.replace("/", "-")
            if filename_key not in filename_job_keys:
                ts = job['finished_time']/1000
                date = dt.datetime.fromtimestamp(ts)
                items_list = [item for item in self.spider.jobs.get(key).items.iter()]
                d_frame = pd.DataFrame(items_list)
                filename_string = "{}/{} {}.csv".format(self.directory_string, filename_key, date.strftime('%Y-%m-%d %I-%M%p'))
                print(filename_string)
                d_frame.to_csv(filename_string)

class ScrapyCloudNeighborhoodSearchSpider(ScrapyCloudSpider):

    def get_listing_ids(self):
        list_ids = []
        for file in os.listdir(self.directory):
            filename = os.fsdecode(file)
            filename_date = filename.split(" ")[1]
            parsed_date = dt.datetime.strptime(filename_date, "%Y-%m-%d")
            one_hundred_eighty_days_ago = dt.datetime.now() - dt.timedelta(days = 180)
            if parsed_date > one_hundred_eighty_days_ago:
                df = pd.read_csv("{}/{}".format(self.directory_string, filename))
                list_ids.extend(df['listing___id'].tolist())
        return list(set(list_ids))

def execute_crawl():
    print("Starting Scrapy Cloud Client Script")
    print("Loading Client")
    client = ScrapyCloudClient()
    print("Saving new files to local hard drive")
    print("-airdna files")
    print("-neighborhood search files")
    client.neighborhood_spider.save_to_file()
    print("-listing files")
    client.listing_spider.save_to_file()
    print("\n")
    print("Running Spiders")
    print("-neighborhood search spider")
    client.neighborhood_spider.run()
    print("-listing spider")
    client.listing_spider.run({'listing_ids': client.listing_ids()})
    print("\n")
    print ("ALL DONE!")