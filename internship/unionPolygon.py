""" 获取相同上下断面的记录中面积最大的河道的id,并赋值到各记录用于融合,并获取相应数据 """
import psycopg2

if __name__=="__main__":
    postgre_connect = psycopg2.connect(
    database="supermap", 
    user="postgres", 
    password="Lvu123123",
    host="localhost",
    port="5432")
    cursor = postgre_connect.cursor()

    cursor.execute('''
        SELECT 
            updm,
            downdm,
            COUNT(*) AS record_count
        FROM 
            hdfd
        GROUP BY 
            updm, 
            downdm
        ORDER BY 
            updm, 
            downdm;
    ''')

    groups=cursor.fetchall()

    for group in groups:
        print(group)
        cursor.execute(f'''
            select hd_code,st_area(geom::geography),geom from hdfd 
            where updm='{group[0]}' and downdm='{group[1]}'
        ''')
        records=cursor.fetchall()

        if group[0]=='None' and group[1]=='None':
            for record in records[1:]:
                cursor.execute(f"update hdfd set group_hd_code ='{record[0]}' where hd_code={record[0]}")
            continue

        if len(records)<2:
            cursor.execute(f"update hdfd set group_hd_code ='{records[0][0]}' where hd_code={records[0][0]}")
            postgre_connect.commit()
        else:
            maxArea=records[0][1]
            maxCode=records[0][0]
            for record in records[1:]:
                if record[1]>maxArea:
                    maxArea=record[1]
                    maxCode=record[0]
            for record in records:
                cursor.execute(f"update hdfd set group_hd_code ='{maxCode}' where hd_code={record[0]}")
            postgre_connect.commit()
    cursor.close()
    postgre_connect.close()