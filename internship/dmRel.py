""" 
根据距离确定
用于确定断面的上下游关系,通过分割的河道判断是不是真的上游,未完成
 """
import psycopg2 
import pandas as pd

if __name__=="__main__":
    postgre_connect = psycopg2.connect(
    database="supermap", 
    user="postgres", 
    password="Lvu123123",
    host="localhost",
    port="5432")
    cursor = postgre_connect.cursor()

    # 上次查询的断面
    lastdmid=''
    # 当前查询的断面
    dmid2search='QTJHK-210'
    while(1):
        cursor.execute(f"SELECT user_key,geom FROM dm WHERE user_key='{dmid2search}'")
        selectedDM=cursor.fetchall();
        selectedDMid=selectedDM[0][0]
        selectedDMgeom=selectedDM[0][1]

        cursor.execute("SELECT user_key,geom FROM dm WHERE updm is NOT NULL or dowmdm is NOT NULL")
        finishedDm=cursor.fetchall();

        cursor.execute("SELECT user_key,geom FROM dm WHERE updm is NULL AND dowmdm is NULL")
        unfinishedDm=cursor.fetchall();

        # 找上游断面
        mindistance=0.04
        upDmId=''
        for dm in unfinishedDm:
            if dm[0]==selectedDMid:
                continue
            
            cursor.execute(f"SELECT ST_Distance('{dm[1]}','{selectedDMgeom}')")
            distance=cursor.fetchall()[0][0]
            if mindistance>distance  :
                print("可能上断面"+dm[0])
                # 找与同时与两个断面相交的断面,判断该面是否与下游断面相接
                hdQuery=f'''
                            SELECT fddm.gid
                            FROM fddm
                            JOIN dm ON ST_Intersects(fddm.geom, dm.geom)
                            WHERE dm.user_key IN ('{dm[0]}', '{selectedDMid}')
                            GROUP BY fddm.gid
                            HAVING COUNT(DISTINCT dm.user_key) = 2;
                        '''
                cursor.execute(hdQuery)
                # hdGid=cursor.fetchall()[0][0]
                print(cursor.fetchall())
                # 看和下游断面有没有交集
        #         cursor.execute(f'''SELECT dm.*
        #                             FROM dm
        #                             JOIN fddm ON ST_Intersects(dm.geom, fddm.geom)
        #                             WHERE fddm.objectid = {hdGid} and (updm is NOT NULL or dowmdm is NOT NULL); 
        #                     ''')
        #         upHd=cursor.fetchall()[0][0]
        # print(upDmId)

        # # 找下游断面
        # mindistance=0.025
        # dowmDmId=''
        # for dm in finishedDm:
        #     cursor.execute(f"SELECT ST_Distance('{dm[1]}','{selectedDMgeom}')")
        #     distance=cursor.fetchall()[0][0]

        #     if mindistance>distance:
        #         upDmId=dm[0]
        #         mindistance=distance

        # print(upDmId,dowmDmId)
        # break
        break