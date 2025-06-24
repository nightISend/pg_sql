import psycopg2
if __name__=="__main__":
    # postgre_connect = psycopg2.connect(
    # database="supermap", 
    # user="postgres", 
    # password="Lvu123123",
    # host="localhost",
    # port="5432")
    postgre_connect = psycopg2.connect(
    database="LSPT_DATA", 
    user="postgres", 
    password="Supermap.",
    host="10.33.13.194",
    port="5432")
    cursor = postgre_connect.cursor()

    cursor.execute("select hd_code,hd_name,uplcz_xd,downlcz_xd from hd_dth where hd_code like '%CSG%'")
    datas=cursor.fetchall()
    for data in datas:
        try:
            hd_code=data[0].replace("k","K")
            hd_name=data[1].replace("k","K")
            uplcz_xd=data[2].replace("k","K")
            downlcz_xd=data[3].replace("k","K")
        except:
            print(data[0])
            continue
        sql=f"update hd_dth set hd_code='{hd_code}',hd_name='{hd_name}',uplcz_xd='{uplcz_xd}',downlcz_xd='{downlcz_xd}' where hd_code='{data[0]}' "
        cursor.execute(sql)
        postgre_connect.commit()
    cursor.close()
    postgre_connect.close()