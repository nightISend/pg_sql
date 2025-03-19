"""
获取河道上下游断面的中点坐标
思路：由于河道数据有上游河道id，故遍历每一个断面获取与其相交的河道，如果河道的上游断面id与该断面id一致则是上游，否则下游
注意在arcgis里可以相交的面和线，在postGIS里可能不相交，可以做线的缓冲区，用缓冲区来和面相交，从而保证可以获取到面
"""
import psycopg2 

if __name__=="__main__":
    postgre_connect = psycopg2.connect(
        database="internship", 
        user="postgres", 
        password="Lvu123123",
        host="localhost",
        port="5432")
    cursor = postgre_connect.cursor()

    """ 获取所有断面的缓冲区,返回元组，顺序与下面字段顺序一致 """
    cursor.execute("select user_key,geog,centerx,centery from redmclipbuffer")
    dms=cursor.fetchall()

    cursor.execute("select gid,geog,upstreamna from riverclip")
    rivers=cursor.fetchall()
    """ 统计 """
    count=0
    """ 遍历断面 """
    for dm in dms:
        """ 遍历河流 """
        print('断面id',dm[0])
        for river in rivers:
            try:
                cursor.execute(f"select  ST_Intersects('{river[1]}','{dm[1]}')")
                if cursor.fetchall()[0][0]:
                    print("gid",river[0])
                    sql=''
                    """ 判断这个断面是河流的上游还是下游 """
                    if river[2]==dm[0]:
                        sql=f'''
                                UPDATE public.riverclip
                                    SET   shangx={dm[2]}, shangy={dm[3]}
                                    WHERE gid={river[0]};
                            '''
                    else:
                        sql=f'''
                                UPDATE public.riverclip
                                    SET   xiax={dm[2]}, xiay={dm[3]}
                                    WHERE gid={river[0]};
                            '''
                    cursor.execute(sql)
                    postgre_connect.commit()
            except Exception as e:
                print(e)
        count=count+1
        print(f"完成{count}条记录")

    postgre_connect.commit()
    cursor.close()  # 关闭游标
    postgre_connect.close()  # 关闭数据库