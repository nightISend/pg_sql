import psycopg2 

postgre_connect = psycopg2.connect(
    database="test", 
    user="postgres", 
    password="Lvu123123",
    host="localhost",
    port="5432")

cursor = postgre_connect.cursor()

tableNameSQL='''SELECT table_name
FROM information_schema.tables
WHERE table_name LIKE '%att%geo%'
  AND table_schema = 'public'
  AND table_name NOT LIKE '%history%'
  AND table_name NOT LIKE '%temp%'
ORDER BY table_name;
'''
cursor.execute(tableNameSQL)
tableNames=cursor.fetchall()
for tableName in tableNames:
    tableName=tableName[0][4:-4]
    createSQL=f'''
    CREATE TABLE c_{tableName}_shp as 
    SELECT 
      att_{tableName}_base.*,
      att_{tableName}_geo.obj_code,
      att_{tableName}_geo.obj_name,
      att_{tableName}_geo.center_x,
      att_{tableName}_geo.center_y,
      att_{tableName}_geo.element_type,
      att_{tableName}_geo.smgeometry
    FROM att_{tableName}_geo JOIN att_{tableName}_base on att_{tableName}_geo.version_id =att_{tableName}_base.version_id;
    '''
    print(createSQL)
    try:
      cursor.execute(createSQL)
    except:
       print(f"建{tableName}表失败")
    postgre_connect.commit()
cursor.close()
postgre_connect.close()