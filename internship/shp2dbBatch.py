import psycopg2 
import pandas as pd
if __name__=="__main__":    
    postgre_connect = psycopg2.connect(
        database="LSPT_DATA", 
        user="", 
        password="",
        host="10.33.13.194",
        port="5432")
    # postgre_connect = psycopg2.connect(
    #     database="test", 
    #     user="postgres", 
    #     password="Lvu123123",
    #     host="localhost",
    #     port="5432")
    cursor = postgre_connect.cursor()

    data=pd.read_excel("F:/实习/超图(钱管局)实习/shp2sql/QTJshpInfo去重无ck.xlsx",sheet_name="Sheet1",keep_default_na=False)
    for index, row in data.iterrows():

        # 要建的表的名称
        tableName=row['英文表名']

        # 要建的表的中文名称，如“水库”、“断面”
        tableZhName=row['中文表名']

        # shp表的名称
        shpTable=row['英文表名']

        # shp表里用来给code字段赋值的字段名
        shpCode=row['code']

        # shp表里用来给Name字段赋值的字段名
        shpName=row['name']

        # geo表注释
        geoNote=f"{tableZhName}空间信息表"

        # base表注释
        baseNote=f"{tableZhName}基本信息表"

        # 管理表注释
        connNote=f"{tableZhName}关联行政区划信息表"

        # base表字段即对应注释 [["gid","gid注释"],["f1","f1注释"],["id","id注释"]]
        columnNote=row['注释'][1:-1]

        """ 完整 """
        createTable=f'''
                    ALTER TABLE {tableName} ALTER COLUMN gid type VARCHAR;

                    CREATE TABLE att_{tableName}_geo(
                    smid serial,
                    smuserid int4 default 0, --赋值0就行
                    smgeometry geometry,
                    obj_code varchar(255),
                    obj_name varchar(255),
                    center_x float8,
                    center_y float8,
                    element_type varchar(255),
                    from_date timestamp default now(),
                    version_id uuid default uuid_generate_v4(),
                    PRIMARY KEY(version_id)
                    );

                    ALTER TABLE att_{tableName}_geo ALTER COLUMN version_id type VARCHAR;

                    INSERT INTO att_{tableName}_geo (obj_code,obj_name,center_x,center_y,smgeometry)
                    select {shpCode} ,{shpName} ,ST_X(ST_Centroid(geom)),ST_Y(ST_Centroid(geom)),geom from {shpTable};

                    create table table_temp as select {shpCode} as {tableName}_code,{shpName} as {tableName}_name from {shpTable};

                    create table att_{tableName}_base as select * from table_temp join {shpTable} on table_temp.{tableName}_code={shpTable}.{shpCode}
                    join (select obj_code,from_date,version_id from att_{tableName}_geo ) geo_temp on geo_temp.obj_code={shpTable}.{shpCode};

                    ALTER TABLE att_{tableName}_base DROP COLUMN geom;
                    ALTER TABLE att_{tableName}_base DROP COLUMN obj_code;
                    DROP TABLE IF EXISTS table_temp;

                    create table rel_{tableName}_ad as select {tableName}_code,att_{tableName}_geo.from_date,att_{tableName}_geo.version_id 
                    from att_{tableName}_base join att_{tableName}_geo on att_{tableName}_geo.version_id = att_{tableName}_base.version_id;
                    ALTER TABLE rel_{tableName}_ad ADD COLUMN ad_code VARCHAR default '330000';

                    COMMENT ON COLUMN att_{tableName}_geo.smid IS '主键id';
                    COMMENT ON COLUMN att_{tableName}_geo.smuserid IS 'sm用户id';
                    COMMENT ON COLUMN att_{tableName}_geo.smgeometry IS '空间信息';
                    COMMENT ON COLUMN att_{tableName}_geo.obj_code IS '要素编码';
                    COMMENT ON COLUMN att_{tableName}_geo.obj_name IS '要素名称';
                    COMMENT ON COLUMN att_{tableName}_geo.center_x IS '经度';
                    COMMENT ON COLUMN att_{tableName}_geo.center_y IS '纬度';
                    COMMENT ON COLUMN att_{tableName}_geo.element_type IS '要素类型';
                    COMMENT ON COLUMN att_{tableName}_geo.from_date IS '开始时间';
                    COMMENT ON COLUMN att_{tableName}_geo.version_id IS '版本id';
                    
                    COMMENT ON COLUMN att_{tableName}_base.{tableName}_code IS '{tableZhName}编码';
                    COMMENT ON COLUMN att_{tableName}_base.{tableName}_name IS '{tableZhName}名称';
                    COMMENT ON COLUMN att_{tableName}_base.from_date IS '记录生效时间';
                    COMMENT ON COLUMN att_{tableName}_base.version_id IS '版本id';

                    COMMENT ON COLUMN rel_{tableName}_ad.{tableName}_code IS '{tableZhName}编码';
                    COMMENT ON COLUMN rel_{tableName}_ad.ad_code IS '行政区划编码';
                    COMMENT ON COLUMN rel_{tableName}_ad.from_date IS '关系建立时间';
                    COMMENT ON COLUMN rel_{tableName}_ad.version_id IS '版本id';

                    comment on table att_{tableName}_geo is '{geoNote}';
                    comment on table att_{tableName}_base is '{baseNote}';
                    comment on table rel_{tableName}_ad is '{connNote}';
                '''
        cursor.execute(createTable)
        postgre_connect.commit()

        # 给base表的属性字段添加注释
        columnNot=columnNote.replace('[','').replace(']','').replace('"','').split(',')
        columnNotResult=[]
        i=0
        for num in range(0,len(columnNot),2):
            columnNotResult.append([columnNot[num],columnNot[num+1]])

        for row in columnNotResult:
            insertNote=f"COMMENT ON COLUMN att_{tableName}_base.{row[0]} IS '{row[1]}';"
            cursor.execute(insertNote)
            postgre_connect.commit()
        
        dropSQL=f" DROP TABLE IF EXISTS table_temp;DROP TABLE IF EXISTS {shpTable};"
        cursor.execute(dropSQL)
        postgre_connect.commit()
        print(f"{tableName}表创建完毕")
    cursor.close()
    postgre_connect.close()