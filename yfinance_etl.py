import requests
import pandas as pd
import datetime
from google.cloud import storage
import google.cloud.storage
import os

def upload_blob(bucket_name, source_file_name, destination_blob_name):
        PATH = os.path.join(os.getcwd(), 'service-key.json')
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = PATH
        storage_client = storage.Client(PATH)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name=destination_blob_name)
        blob.upload_from_filename(source_file_name)
        print(f"File {source_file_name} uploaded to {destination_blob_name}.")


def run_news_etl():
        url = "https://apidojo-yahoo-finance-v1.p.rapidapi.com/news/v2/list"

        querystring = {"region":"US","snippetCount":"28"}

        """
        In the payload field:
        Pass in the value of uuids field returned right in this endpoint to load the next page, or leave empty to load first page
        """
        payload = ""
        headers = {
                "content-type": "text/plain",
                "X-RapidAPI-Key": "YOUR_API_KEY",
                "X-RapidAPI-Host": "apidojo-yahoo-finance-v1.p.rapidapi.com"
        }

        response = requests.post(url, data=payload, headers=headers, params=querystring).json()

        news_list = []

        for news in response['data']['main']['stream']:
                refined_news = {
                        "id": news['id'],
                        "contentType": news['content']['contentType'],
                        "title": news['content']['title'],
                        "published_date": news['content']['pubDate'],
                        "previewUrl": news['content']['previewUrl'],
                        "provider": news['content']['provider']['displayName']
                }

                news_list.append(refined_news)

        df = pd.DataFrame(news_list)

        current_date = datetime.date.today()
        formatted_date = current_date.strftime('%d%B%Y')
        csv_title = f"alpha_top_news_{formatted_date}.csv"

        df.to_csv(csv_title)

        upload_blob('yfinance_news', csv_title, csv_title)