""" 
用于确定断面的上下游关系,通过河道线确定顺序
postgis计算线与线的距离不是两个中心点的距离,
要先用arcgis计算线的中点再用中点来计算,
计算时用geom::geography把地理要素转成几何要素
 """
import psycopg2

if __name__=="__main__":
    postgre_connect = psycopg2.connect(
    database="supermap", 
    user="postgres", 
    password="Lvu123123",
    host="localhost",
    port="5432")
    cursor = postgre_connect.cursor()

    # 查询的河道线id
    hdx=49
    # 河道线的起始断面
    startdmid='MWX-1'
    # 起始断面的下游断面
    lastdmid='CSG-136'


    i=0
    while(1):
        cursor.execute(f"SELECT user_key,geom FROM dm WHERE user_key='{startdmid}'")
        selectedDM=cursor.fetchall();
        selectedDMgeom=selectedDM[0][1]

        query=f'''
            SELECT dm.user_key,dm.geom from dm 
            join hdx on st_intersects(hdx.geom, dm.geom) 
            WHERE hdx.userid={hdx} and (updm is  NULL and dowmdm is  NULL)'''
        cursor.execute(query)
        unfinishedDm=cursor.fetchall();

        if len(unfinishedDm)<=1:
            print("退出")
            cursor.execute(f"UPDATE dm SET updm = '', dowmdm= '{lastdmid}'WHERE user_key='{startdmid}'")
            postgre_connect.commit()
            break

        # 找上游断面
        mindistance=1
        upDmId=''
        for dm in unfinishedDm:
            if dm[0]==startdmid:
                continue
            cursor.execute(f"SELECT ST_Distance('{dm[1]}','{selectedDMgeom}')")
            distance=cursor.fetchall()[0][0]
            postgre_connect.commit()
            if mindistance>distance :
                upDmId=dm[0]
                mindistance=distance
        # print("上游"+upDmId)
        # print("下游"+lastdmid)
        print("当前"+startdmid)
        cursor.execute(f"UPDATE dm SET updm = '{upDmId}', dowmdm= '{lastdmid}'WHERE user_key='{startdmid}'")
        postgre_connect.commit()
        lastdmid=startdmid
        startdmid=upDmId
        # if i>5:
        #     break
        # i=i+1
        
