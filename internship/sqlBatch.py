# 将要执行的sql放在一个文件夹里，批量执行sql
import psycopg2 
import pandas as pd
import os

postgre_connect = psycopg2.connect(
    database="LSPT_DATA", 
    user="sysadmin", 
    password="Supermap123!@#",
    host="10.33.13.194",
    port="5432")
# postgre_connect = psycopg2.connect(
#     database="test", 
#     user="postgres", 
#     password="Lvu123123",
#     host="localhost",
#     port="5432")
cursor = postgre_connect.cursor()

def traversal_files(path):
    dirs=[]#记录文件路径
    files=[]#记录文件夹路径
    for item in os.scandir(path):
        if item.is_dir():
          dirs.append(item.path)
        elif item.is_file():
          files.append(item.path)
    return dirs,files
# 修改路径为存放shp的文件夹
dirs,files=traversal_files('F:\实习\超图(钱管局)实习\shp2sql\SYDCsql')

for filepath in files: 
    try:
        with open(filepath, "r",encoding="UTF-8") as file:
                    # 从文件中读取.sql脚本内容
                    sql_script = file.read()

                    # 执行.sql脚本
                    cursor.execute(sql_script)
                    postgre_connect.commit()
                    print("完成 ",filepath)
    except Exception as e:
        postgre_connect.commit()
        print(filepath.split("\\")[-1])
        print(e)
cursor.close()
postgre_connect.close()