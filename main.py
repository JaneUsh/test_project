from datetime import datetime
import xml.etree.ElementTree as ET
import pandas as pd
import json
import difflib
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures

common_fields = ['id', 'name', 'brand_name', 'ean', 'volume']


def work_with_date_1():
    list_need_fields = ['MANUFACTURER', 'NAME', 'EAN', 'id', 'SIZE']
    need_fields = ['id', 'NAME', 'MANUFACTURER', 'EAN', 'SIZE']
    xml_data = open('data/data_Soruce_1.xml', 'r').read()  # Read file
    root = ET.XML(xml_data)  # Parse XML

    data = []
    for child in root:
        data.append([subchild.text for subchild in child if subchild.tag in list_need_fields])

    df = pd.DataFrame(data)  # Write in DF and transpose it
    df.columns = list_need_fields  # Update column names
    df = df.reindex(columns=need_fields)
    df_new = df.mask(df.eq('None')).dropna()
    df_new.rename(columns={'MANUFACTURER': 'brand_name', 'EAN': 'ean', 'SIZE': 'volume', 'NAME': 'name'}, inplace=True)
    volume_list = list(df_new['volume'])
    df_new['volume'] = [str(float(' '.join(volume.split()[:-1]))) for volume in volume_list]
    return df_new


def work_with_date_2():
    list_need_fields = ['EAN', 'id', 'Brand', 'Weight']
    need_fields = ['id', 'name', 'Brand', 'EAN', 'Weight']
    xml_data = open('data/data_Source_2.xml', 'r').read()  # Read file
    root = ET.XML(xml_data)  # Parse XML

    data = []
    for child in root:
        d = []
        for subchild in child:
            if subchild.tag in list_need_fields:
                d.append(subchild.text)
            elif subchild.tag == 'ProductTranslation':
                for s in subchild:
                    if s.tag == 'name':
                        d.append(s.text)
        data.append(d)

    list_need_fields.append('name')

    df = pd.DataFrame(data)
    df.columns = list_need_fields
    df = df.reindex(columns=need_fields)
    df_new = df.mask(df.eq('None')).dropna()
    df_new.rename(columns={'Brand': 'brand_name', 'EAN': 'ean', 'Weight': 'volume'}, inplace=True)

    return df_new


def work_with_date_3():
    list_need_fields = ['Id', 'EANs', 'name', 'BrandName', 'Contenido']
    need_fields = ['Id', 'name', 'BrandName', 'EANs', 'Contenido']
    df = pd.read_json('data/data_Source_3.json')
    df = df[list_need_fields]
    df = df.reindex(columns=need_fields)
    df_new = df.mask(df.eq('None')).dropna()
    df_new.rename(columns={'Id': 'id', 'BrandName': 'brand_name', 'EANs': 'ean', 'Contenido': 'volume'}, inplace=True)

    return df_new.explode('ean')


def get_data_with_the_same_ean(df1, df2, df3):
    df = df1.merge(df2, on='ean')
    df_new = df.merge(df3, on='ean')

    json_data = {"compare_products": []}

    for col_name, data in df_new.iterrows():
        new_data = []
        new_data.append({"source_name": "data_Soruce_1",
                         "name": data["name_x"], "ean_code": data["ean"],
                         "id": data["id_x"]})
        new_data.append({"source_name": "data_Soruce_2",
                         "name": data["name_y"], "ean_code": data["ean"],
                         "id": data["id_y"]})
        new_data.append({"source_name": "data_Soruce_3",
                         "name": data["name"], "ean_code": data["ean"],
                         "id": data["id"]})

        json_data["compare_products"].append(new_data)

    capitals_json = json.dumps(json_data)

    with open("Result.json", "w") as my_file:
        my_file.write(capitals_json)


def similar(a, b):
    ratio = difflib.SequenceMatcher(None, a, b).ratio()
    return ratio


def get_similar_data(df1, df):
    result = []

    time_s = datetime.now()

    for index, row in df1.iterrows():

        df_check = df

        ratio_result = [similar(row['name'], j) for j in df_check['name']]
        ratio = max(ratio_result)

        if ratio > 0.7:
            df_check['score_name'] = ratio_result
            df_check = df_check.loc[df_check['score_name'] > 0.7]

            ratio_result = [similar(row['brand_name'], j) for j in df_check['brand_name']]
            max_ratio = max(ratio_result)

            if max_ratio > 0.7:
                df_check['score_brand_name'] = ratio_result
                df_check = df_check.loc[df_check['score_brand_name'] > 0.7]

                ratio_result = [similar(row['volume'], j) for j in df_check['volume']]

                max_ratio = max(ratio_result)

                if max_ratio > 0.7:
                    df_check['score_volume'] = ratio_result
                    df_check = df_check.loc[df_check['score_volume'] > 0.7]

                    if not df_check.empty:
                        time_f = datetime.now()
                        print(time_f - time_s)
                        pass


if __name__ == '__main__':
    df1 = work_with_date_1()
    df2 = work_with_date_2()
    df3 = work_with_date_3()
    get_data_with_the_same_ean(df1, df2, df3)
    similar_result = []

    for df in [df2, df3]:
        similar_result.append(get_similar_data(df1, df))
