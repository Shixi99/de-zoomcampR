import re
import sys
import bs4
import os
import io
import boto3
import time
import random
import itertools
import requests
import warnings
import numpy as np
import pandas as pd
import urllib.parse
import geocoder
from prefect import flow, task
from bs4 import BeautifulSoup
# from typer import List

from dotenv import load_dotenv

load_dotenv()  


warnings.filterwarnings("ignore")


pd.options.display.max_columns = 1000
pd.options.display.max_rows = 10000
pd.options.display.max_colwidth = -1
sales_bina_url =  'https://kub.az/search?adsDateCat=All&entityType=0&buildingType=-1&purpose=0&ownerType=-1&city=1&documentType=-1&loanType=-1&oneRoom=false&twoRoom=false&threeRoom=false&fourRoom=false&fiveMoreRoom=false&remakeType=-1&minFloor=1&maxFloor=31&minBuildingFloors=1&maxBuildingFloors=31&minPrice=&maxPrice=&minArea=&maxArea=&minParcelArea=&maxParcelArea=&words=&search=&page='
sales_villa_url = 'https://kub.az/search?adsDateCat=All&entityType=1&buildingType=-1&purpose=0&ownerType=-1&city=1&documentType=-1&loanType=-1&oneRoom=false&twoRoom=false&threeRoom=false&fourRoom=false&fiveMoreRoom=false&remakeType=-1&minFloor=1&maxFloor=31&minBuildingFloors=1&maxBuildingFloors=31&minPrice=&maxPrice=&minArea=&maxArea=&minParcelArea=&maxParcelArea=&words=&search=&page='

# rent_bina_url =   'https://kub.az/search?adsDateCat=All&entityType=0&buildingType=-1&purpose=1&ownerType=-1&city=1&documentType=-1&loanType=-1&oneRoom=false&twoRoom=false&threeRoom=false&fourRoom=false&fiveMoreRoom=false&remakeType=-1&minFloor=1&maxFloor=31&minBuildingFloors=1&maxBuildingFloors=31&minPrice=&maxPrice=&minArea=&maxArea=&minParcelArea=&maxParcelArea=&words=&search=&page='
# rent_villa_url =  'https://kub.az/search?adsDateCat=All&entityType=1&buildingType=-1&purpose=1&ownerType=-1&city=1&documentType=-1&loanType=-1&oneRoom=false&twoRoom=false&threeRoom=false&fourRoom=false&fiveMoreRoom=false&remakeType=-1&minFloor=1&maxFloor=31&minBuildingFloors=1&maxBuildingFloors=31&minPrice=&maxPrice=&minArea=&maxArea=&minParcelArea=&maxParcelArea=&words=&search=&page='

@task(log_prints=True)
def page_count(url: str):
    response = requests.get(url, verify=False)
    soup = BeautifulSoup(response.content,'html.parser')
    page_soup = soup.find_all("span",{"class":""})
    count = int(str(page_soup[1]).split("Səhifə  ")[1].split("/")[1].split("<")[0].strip())
    return count

@task(log_prints=True)
def get_content(url: str):
    response = requests.get(url, verify=False)
    soup = BeautifulSoup(response.content, 'html.parser')
    content = soup.find_all("div",{"class":"items"})
    return content

@task(log_prints=True)
def get_item_properties(content):
    item_properities = content[0].find_all("div",{"class":"item-properities"})
    return item_properities

@task(log_prints=True)
def get_item_picture(content):
    item_picture = content[0].find_all("div",{"class":"item-picture"})
    return item_picture

@task(log_prints=True)
def get_item_details(content):
    details = content[0].find_all("div",{"class":"details"})
    return details

@task(log_prints=True)
def get_room(item_properities):
    rooms_list = []
    for i in item_properities:
        try:
            room_count =  i\
                .find_all("h1",{"class":"text-nowrap"})[0]\
                .find_all("span",{"class":"text-nowrap"})[0]\
                .text.split(' ')
            if 'otaq' in ' '.join(room_count):
                rooms_list.append(int(room_count[0]))
            else:
                rooms_list.append(0)
        except:
            rooms_list.append(0)
            continue
    return rooms_list

@task(log_prints=True)
def get_size(item_properities):
    size_list = []
    for i in item_properities:
        try:
            size =  i\
                .find_all("h1",{"class":"text-nowrap"})[0]\
                .find_all("span",{"class":"text-nowrap"})[1]\
                .text.split(' ')
            if 'm²' in ' '.join(size):
                size_list.append(int(size[0]))
            else:
                size_list.append(0)
        except:
            size_list.append(0)
            continue
                   
    return size_list

@task(log_prints=True)
def get_floor(item_properities):
    floor_list = []
    for i in item_properities:
        try:
            floor_list.append(\
            int(
                i\
                .find_all("h1",{"class":"text-nowrap"})[0]\
                .find_all("span",{"class":"text-nowrap"})[2]\
                .text.split('/')[0]
            ))
        except:
            floor_list.append(0)
            continue
    return floor_list

@task(log_prints=True)
def get_age_category(item_properities):
    age_cat_list = []
    for i in item_properities:
        try:
            for j in i.find_all("h1",{"class":"item-category"}):
                age_cat_list.append(j.text.split('/')[1].replace(' ','')[0])
        except:
            age_cat_list.append('NA')
            continue      
    return age_cat_list

@task(log_prints=True)
def get_document(item_picture):
    document_list = []
   
    for i in item_picture:
        document_list.append([j.text for j in i.find_all("div", {"class":"item-certificate"})])
       
    for i in document_list:
        if len(i) == 0:
            i.append('Yoxdur')
    flat_clean_document_list = list(itertools.chain.from_iterable(document_list))
    return flat_clean_document_list

@task(log_prints=True)
def get_price(item_picture):
    price_list = []
    for i in item_picture:
        price_list.append(
        int(
            i\
            .find_all("h3", {"class":"item-price"})[0] \
            .find_all('span', {'class':'price-amount'})[0].text
           )
        )
    return price_list

@task(log_prints=True)
def get_credit(item_picture):
    document_list = []
   
    for i in item_picture:
        document_list.append([j.text for j in i.find_all("div", {"class":"item-credit"})])
       
    for i in document_list:
        if len(i) == 0:
            i.append('Kreditsiz')
    flat_clean_document_list = list(itertools.chain.from_iterable(document_list))
    return flat_clean_document_list

@task(log_prints=True)
def get_type(item_picture):
    type_list = []
   
    for i in item_picture:
        type_list.append([j.text for j in i.find_all("div", {"class":"item-sell-rent-property"})])
       
    for i in type_list:
        if len(i) == 0:
            i.append('NA')
    flat_clean_type_list = list(itertools.chain.from_iterable(type_list))
    return flat_clean_type_list

@task(log_prints=True)
def get_source(details):
    source_list = []
    for i in details:
        source_list.append([j.text.replace(' ','') for j in i.find_all('span', {'class':'item-source'})])
        flat_source_list = list(itertools.chain.from_iterable(source_list))
    return flat_source_list

@task(log_prints=True)
def get_date(details):
    date_list = []
    for i in details:
        date_list.append([j.text.replace(' ','') for j in i.find_all('span', {'class':'item-date'})])
        flat_date_list = list(itertools.chain.from_iterable(date_list))
    return flat_date_list

@task(log_prints=True)
def get_address(details):
    address_list = []
    for i in details:
        address_list.append(' '.join([j.text for j in i.find_all('span', {'class':'text-nowrap'})]).replace('Bakı/Abşeron','Bakı').replace('r.','').replace('m.',''))
    return address_list

@task(log_prints=True)
def get_link(details):
    link_list = []
    for i in details:
        link_list.append('kub.az'+i.find('a').get('href'))
    return link_list

@task(log_prints=True)
def get_district(details):
    district_list = []
    try:
        for i in details:
            d_list = i.find_all("span",{"class":"text-nowrap"})
            if len(d_list) == 0:
                district_list.append('null')
            elif len(d_list) == 1:
                district_list.append(d_list[0].text)
            else:
                district_list.append(d_list[1].text)
    except Exception as e:
        print('ERROR: ', e)
        district_list.append('null')
    return district_list

           
@flow()
def make_df(url, n):
    url += str(n)
    date_list = []
    room_list = []
    floor_list = []
    size_list = []
    price_list = []
    address_list = []
    category_list = []
    document_list = []
    credit_list = []
    type_list = []
    source_list = []
    link_list = []
    district_list = []
    try:
        content = get_content(url)
        item_properties = get_item_properties(content)
        item_picture = get_item_picture(content)
        details = get_item_details(content)
        date_list = get_date(details)
        room_list = get_room(item_properties)
        floor_list = get_floor(item_properties)
        size_list = get_size(item_properties)
        price_list = get_price(item_picture)
        address_list = get_address(details)
        category_list = get_age_category(item_properties)
        document_list = get_document(item_picture)
        credit_list = get_credit(item_picture)
        type_list = get_type(item_picture)
        source_list = get_source(details)
        link_list = get_link(details)
        district_list = get_district(details)
        data = {
          'date': date_list,
          'room': room_list,
          'floor': floor_list,
          'size': size_list,
          'price' : price_list,
          'district': district_list,
          'address' : address_list,
          'category': category_list,
          'document' : document_list,
          'credit': credit_list,
          'type': type_list,
          'source': source_list,
          'link': link_list
        }

#         for k,v in data.items():
#             print(k, len(v))

        df = pd.DataFrame(data)
        return df
    except Exception as e:
        print('ERROR: ', e)


@flow()
def make_full_df(url, n):
    result_df = pd.DataFrame() 
    for i in range(1, n+1):
        df = make_df(url, i)
        result_df = pd.concat([result_df, df])
    return result_df



@flow()
def merge_dfs(url_list):
    final_df = pd.DataFrame()
    for url in url_list:
        n = 1 #url[1]
        df = make_full_df(url[0], n)
        df['object_type'] = url[2]
        final_df = pd.concat([final_df, df])
    final_df = final_df.reset_index()
    final_df['id'] = final_df.link.map(lambda x: x[-8:])
    final_df = final_df.drop('index', axis=1)
    final_df = final_df.drop_duplicates(subset='link')
    return final_df


@task(log_prints=True)
def get_unique_adresses(df):
    return list(set(df.address.to_list()))

@task(log_prints=True)
def make_df_address_map(unique_address: list):
    address_map = { 
    'address': unique_address,
    'lat': [],
    'lon' : []
    }

    for i in unique_address:
        try:
            address = str(i)
            g = geocoder.arcgis(address)    
            # address_map['address'].append(i)
            address_map['lat'].append(g.latlng[0])
            address_map['lon'].append(g.latlng[1])
            
                
        except Exception as e:
    #         print(i, 0, 0)
            # address_map['address'].append(i)
            address_map['lat'].append(0)
            address_map['lon'].append(0)
            continue


    address_map_df = pd.DataFrame(address_map)
    return address_map_df


AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

@task()
def write_csv_s3(path, df):
    with io.StringIO() as buffer:
        df.to_csv(buffer, index=False)

        response = s3_client.put_object(
            Bucket=AWS_S3_BUCKET, Key=path, Body=buffer.getvalue()
        )
    return 

@task()
def write_parquet_s3(path, df):
    with io.BytesIO() as buffer:
        df.to_parquet(buffer, index=False)

        response = s3_client.put_object(
            Bucket=AWS_S3_BUCKET, Key=path, Body=buffer.getvalue()
        )
    return 

@flow()
def main():
    params = [[sales_bina_url, page_count(sales_bina_url+'1'), 'bina_menzil'],
                [sales_villa_url,page_count(sales_villa_url+'1'), 'heyet']]
#             [rent_bina_url, page_count(rent_bina_url+'1'), 'bina_menzil'],
#             [rent_villa_url, page_count(rent_villa_url+'1'), 'heyet']]

    df = merge_dfs(params)
    unique_adresses = get_unique_adresses(df)
    df_address_map = make_df_address_map(unique_adresses)

    df_final = pd.merge(df, df_address_map, on='address', how='inner')
    result = df_final.drop_duplicates()
    # result.to_csv('result.csv')

    dataset_name = 'test'#'parquet_test'

    path = f"data/parquet/{dataset_name}.parquet"
    
    write_parquet_s3(path, result)

    return result


if __name__ == '__main__':
    main()








