import psycopg2

if __name__=="__main__":
    postgre_connect = psycopg2.connect(
    database="supermap", 
    user="postgres", 
    password="Lvu123123",
    host="localhost",
    port="5432")
    cursor = postgre_connect.cursor()

    cursor.execute("select count(*) from hdone")
    num=cursor.fetchall()[0][0]

    for i in range(0,num):
        cursor.execute(f'''
                        select hdone.id, dm_buffer.user_key, dm_buffer.updm, dm_buffer.downdm
                        from hdone join dm_buffer on st_intersects(hdone.geom, dm_buffer.geom)
                       where hdone.id={i}
                       ''')
        hds=cursor.fetchall()
        print(hds)
        sql=''
        match len(hds):
            case 1:
                hd=hds[0]
                if hd[3]==None:
                    sql=f"update hdone set updm='{hd[1]}',downdm='{hd[3]}' "
                else:
                    sql=f"update hdone set updm='{hd[2]}',downdm='{hd[1]}' "
            case 2:
                hd1=hds[0]
                hd2=hds[1]
                if hd1[1]==hd2[2]:
                    sql=f"update hdone set updm='{hd1[1]}',downdm='{hd2[1]}' "
                else:
                    sql=f"update hdone set updm='{hd2[1]}',downdm='{hd1[1]}' "
            case 3:
                hd1=hds[0]
                hd2=hds[1]
                hd3=hds[2]
                if hd1[1]==hd2[3] and hd1[1]==hd3[3]:
                    sql=f"update hdone set updm='{hd1[2]}',downdm='{hd1[1]}' "
                elif hd2[1]==hd1[3] and hd2[1]==hd3[3]:
                    sql=f"update hdone set updm='{hd2[2]}',downdm='{hd2[1]}' "
                else :
                    sql=f"update hdone set updm='{hd3[2]}',downdm='{hd3[1]}' "
            case _:
                continue
        sql=sql+f" where id={i}"
        cursor.execute(sql)
        postgre_connect.commit()
        print(f"完成河道{i}")
