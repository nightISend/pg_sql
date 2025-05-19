import psycopg2 
if __name__=="__main__":    
    # postgre_connect = psycopg2.connect(
    #     database="LSPT_DATA", 
    #     user="postgres", 
    #     password="Supermap.",
    #     host="10.33.13.194",
    #     port="5432")
    postgre_connect = psycopg2.connect(
        database="supermap", 
        user="postgres", 
        password="Lvu123123",
        host="localhost",
        port="5432")
    cursor = postgre_connect.cursor()

    # 要建的表的名称
    tableName="hpm"

    # 要建的表的中文名称，如“水库”、“断面”
    tableZhName="湖泊面"

    # shp表的名称
    shpTable=tableName

    # shp表里用来给code字段赋值的字段名
    shpCode="code"

    # shp表里用来给Name字段赋值的字段名
    shpName="name"

    # base表字段即对应注释 [["gid","gid注释"],["f1","f1注释"],["id","id注释"]]
    columnNote=[["gid","gid"],["smuserid","社交平台用户ID"],["objectid","对象ID"],["name","名称"],["code","编码"],["city","城市"],["county","县区"],["masl","海拔高度（米）"],["mu","面积（亩）"],["town","乡镇"],["trntype","流向类型"],["bas","所属流域"],["landform","地形地貌"],["area","面积"],["averdep","平均深度"],["vol","容积/体积"],["function","功能用途"],["type","类型"],["remark","备注信息"],["shape_leng","形状长度"],["shape_area","形状面积"],["contdiff","内容差异说明"],["spcl","特殊属性"],["imp","重要性等级"],["lchief","负责人"],["cf","重复"],["zys","重要水"],["sfh","是否合"],["lx2022","类型2022"]]

    # geo表注释
    geoNote=f"{tableZhName}空间信息表"

    # base表注释
    baseNote=f"{tableZhName}基本信息表"

    # 管理表注释
    connNote=f"{tableZhName}关联行政区划信息表"

    """ 完整 """
    createTable=f'''
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
            ALTER TABLE rel_{tableName}_ad ADD COLUMN ad_code VARCHAR default '420102';

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
    for row in columnNote:
        insertNote=f"COMMENT ON COLUMN att_{tableName}_base.{row[0]} IS '{row[1]}';"
        cursor.execute(insertNote)
        postgre_connect.commit()
    
    dropSQL=f" DROP TABLE IF EXISTS table_temp;DROP TABLE IF EXISTS {shpTable};"
    cursor.execute(dropSQL)
    postgre_connect.commit()

    cursor.close()
    postgre_connect.close()
    print(f"{tableName}表创建完毕")