""" 接口数据入库 """
import psycopg2
import requests

if __name__=="__main__":
    postgre_connect = psycopg2.connect(
    database="supermap", 
    user="postgres", 
    password="Lvu123123",
    host="localhost",
    port="5432")
    cursor = postgre_connect.cursor()

    # headers={"accessToken":"b54690219d2347a7ab330d970842d29c","appKey":"dbccf7a460fc46918250aaac6a251235"}
    # resp = requests.post(f"http://10.33.13.182:7777/open/api/xMtAtVevE8Xn",headers=headers)
    # resp.raise_for_status()
    # result = resp.json()
    # print(result)

    