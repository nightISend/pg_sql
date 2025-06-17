""" 用于设置在河段的编号和名称 """
import psycopg2

if __name__=="__main__":
    postgre_connect = psycopg2.connect(
    database="supermap", 
    user="postgres", 
    password="Lvu123123",
    host="localhost",
    port="5432")
    cursor = postgre_connect.cursor()

    cursor.execute('select uplcz_xd,downlcz_xd,name,gid from hd_dth')
    datas=cursor.fetchall()
    static={}
    for data in datas:
        up_o=data[0]
        down_0=data[1]
        try:
            up=up_o.split('_')
            down=down_0.split('_')
        except:
            up_o=down_0.split('_')[0]+'_k00+000'
            up=up_o.split('_')
            down=down_0.split('_')
        id=''
        if up[0]==down[0]:
            id=up_o+'~'+down[1]
        else:
            id=up_o+'~'+down_0

        if id in static:
            static[id] += 1
        else:
            static[id] = 0
        if static[id] != 0:
            id=id+'^'+str(static[id])
        name=data[2]+'_'+id.split('_')[1]
        print(id,name,data[3])
        cursor.execute(f"update hd_dth set hd_code='{id}',hd_name='{name}' where gid={data[3]}")
        postgre_connect.commit()
    cursor.close()
    postgre_connect.close()