""" excel点数据配合表信息json入库 """
import psycopg2
import pandas as pd
import requests
import json

# 获取属性中英文及类型
def getAttribute(fieldList):
    attribute={}
    for row in fieldList:
        attribute[row['fieldName']]={"name":row['fieldDescription'],"type":row['fieldType']}
    return attribute

# shp表建表语句（属性带空间字段）,建表时自动将字段转小写
def creatShpSQL(attributes,table_EN_Name):
    column2CreateTable=''
    for key, value in attributes.items():
        type=''
        if value['type']=="int":
            type='int4'
        elif value['type']=="decimal":
            type='float8'
        else:
            type='varchar(255)'
        column2CreateTable=column2CreateTable+key+" "+type+",\n"
    column2CreateTable=column2CreateTable+"geom geometry"
    createTableSQL=f'''
            CREATE TABLE {table_EN_Name} ({column2CreateTable});
            '''
    return createTableSQL

# 数据插入语句,excel与返回有出入
def creatInsertSQL(attributes,datas,tableName):
    result=''
    values=[]
    keyStr=''
    for key, value in attributes.items():
        values.append(value)
        keyStr=keyStr+'"'+to_lower_case(key)+'",'
    keyStr=keyStr+'"geom"'
    # print(values)
    for index, row in datas.iterrows():
        valueStr=''
        for value in values:
            value2table=''
            # 判断字段是否存在
            try:
                value2table=row[value['name']]
            except:
                value2table='NULL'

            if value['type']=='int' or value['type']=='decimal':
                if value2table=='':
                    value2table='NULL'
            else:
                if value2table=='NULL':
                    value2table="''"
                else:
                    value2table="'"+str(value2table)+"'"

            valueStr=valueStr+f"{value2table},"
        x=row['地理位置经度坐标']
        y=row['地理位置纬度坐标']
        # 坐标错误跳过该记录
        if x=='' or y==''or float(x)>180 or float(x)<-180 or float(y)>90 or float(y)<-90:
            continue
        valueStr=valueStr+createGeomStr(x,y)

        # if row['设计扬程']=='':
        #     print('空值是‘’')

        insertSQL=f'INSERT INTO {tableName} ({keyStr}) VALUES ({valueStr});\n'
        result=result+insertSQL
    return result

# 生成空间字段
def createGeomStr(longtitude,latitude):
    return f'ST_GeomFromText(\'point({longtitude} {latitude})\',4490)'

# 大写转小写
def to_lower_case(input_str):
    return input_str.lower()

if __name__=="__main__":
    postgre_connect = psycopg2.connect(
    database="test", 
    user="postgres", 
    password="Lvu123123",
    host="localhost",
    port="5432")
    cursor = postgre_connect.cursor()

    excel_path='F:/实习/超图(钱管局)实习/接口数据入库/excel/泵站基础信息.xlsx'
    res_path='F:/实习/超图(钱管局)实习/接口数据入库/fileinfo/泵站基础信息.json'
    table_data=pd.read_excel(excel_path,sheet_name='泵站基础信息数据',keep_default_na=False)

    # 属性中英文及类型
    attributes=None
    table_CN_Name=''
    table_EN_Name=''
    # 作为名称显示的字段
    name2Show=''

    with open(res_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
        table_CN_Name=data['data']['directoryName']
        table_EN_Name=data['data']['tableNameEn']
        attributes=getAttribute(data['data']['fieldList'])

        # createTableSQL=creatShpSQL(attributes,table_EN_Name)
        # cursor.execute(createTableSQL)
        # postgre_connect.commit()
        # print(f"建{table_EN_Name}成功")
    result=creatInsertSQL(attributes,table_data,table_EN_Name)
    cursor.execute(result)
    postgre_connect.commit()
    print("插入成功")
    cursor.close()
    postgre_connect.close()