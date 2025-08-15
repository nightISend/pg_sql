"""
excel点数据配合表信息json入库,
修改文件路径
入库前修改creatInsertSQL里坐标获取,修改用于显示的nameInList变量
"""
import psycopg2
import pandas as pd
import requests
import json

# 获取属性中英文及类型
def getAttribute(fieldList):
    attribute={}
    for row in fieldList:
        attribute[row['fieldName']]={"name":row['fieldDescription'],"type":row['fieldType']}
    return attribute

# shp表建表语句（属性带空间字段）,建表时自动将字段转小写
def creatShpSQL(attributes,table_EN_Name):
    column2CreateTable=''
    for key, value in attributes.items():
        type=''
        if value['type']=="int":
            type='int4'
        elif value['type']=="decimal":
            type='float8'
        elif value['type']=="string":
            type='text'
        else:
            type='varchar(255)'
        column2CreateTable=column2CreateTable+key+" "+type+",\n"
    column2CreateTable=column2CreateTable+"geom geometry"
    createTableSQL=f'''
            CREATE TABLE {table_EN_Name} (gid serial,{column2CreateTable});
            '''
    return createTableSQL

# 数据插入语句,excel与返回有出入
def creatInsertSQL(attributes,datas,tableName):
    result=''
    values=[]
    keyStr=''
    for key, value in attributes.items():
        values.append(value)
        keyStr=keyStr+'"'+to_lower_case(key)+'",'
    keyStr=keyStr+'"geom"'
    # print(values)
    for index, row in datas.iterrows():
        valueStr=''
        for value in values:
            value2table=''
            # 判断字段是否存在
            try:
                value2table=row[value['name']]
            except:
                value2table='NULL'

            if value['type']=='int' or value['type']=='decimal':
                if value2table=='':
                    value2table='NULL'
            else:
                if value2table=='NULL':
                    value2table="''"
                else:
                    # 半角引号转全角避免入库报错
                    value2table=str(value2table).replace("'","‘")
                    value2table="'"+str(value2table)+"'"

            valueStr=valueStr+f"{value2table},"

        """ 入库前根据excel选择坐标字段 """
        x=row['大坝地理位置经度坐标']
        y=row['大坝地理位置纬度坐标']

        # 坐标错误跳过该记录
        if x=='' or y==''or float(x)>180 or float(x)<-180 or float(y)>90 or float(y)<-90:
            continue
        valueStr=valueStr+createGeomStr(x,y)

        # if row['设计扬程']=='':
        #     print('空值是‘’')

        insertSQL=f'INSERT INTO {tableName} ({keyStr}) VALUES ({valueStr});\n'
        result=result+insertSQL
    return result

# 生成空间字段
def createGeomStr(longtitude,latitude):
    return f'ST_GeomFromText(\'point({longtitude} {latitude})\',4490)'

# 大写转小写
def to_lower_case(input_str):
    return input_str.lower()

if __name__=="__main__":
    postgre_connect = psycopg2.connect(
        database="test", 
        user="postgres", 
        password="Lvu123123",
        host="localhost",
        port="5432")
    # postgre_connect = psycopg2.connect(
    #     database="LSPT_DATA", 
    #     user="sysadmin", 
    #     password="Supermap123!@#",
    #     host="10.33.13.194",
    #     port="5432")
    cursor = postgre_connect.cursor()

    excel_path='F:/实习/超图(钱管局)实习/接口数据入库/excel/小型水库基础信息.xlsx'
    res_path='F:/实习/超图(钱管局)实习/接口数据入库/fileinfo/小型水库基础信息.json'
    table_data=pd.read_excel(excel_path,sheet_name='小型水库基础信息数据',keep_default_na=False)

    # 用于列表显示的字段，英文，小写，人工设置
    nameInList='res_name'

    # 属性中英文及类型,代码获取
    attributes=None
    table_CN_Name=''
    table_EN_Name=''

    with open(res_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
        table_CN_Name=data['data']['directoryName']
        table_EN_Name=data['data']['tableNameEn']
        attributes=getAttribute(data['data']['fieldList'])

        createTableSQL=creatShpSQL(attributes,table_EN_Name)
        cursor.execute(createTableSQL)
        postgre_connect.commit()
        print(f"建{table_EN_Name}成功")
    result=creatInsertSQL(attributes,table_data,table_EN_Name)
    cursor.execute(result)
    postgre_connect.commit()
    print("插入成功")


    tableName=table_EN_Name
    tableZhName=table_CN_Name
    # 入库SQL
    rkSQL=f'''
        ALTER TABLE {tableName} ALTER COLUMN gid type VARCHAR;

        CREATE TABLE att_{tableName}_geo(
        smid serial,
        smuserid int4 default 0, --赋值0就行
        smgeometry geometry,
        obj_code varchar(255),
        obj_name varchar(255),
        center_x float8,
        center_y float8,
        element_type varchar(255) default '{tableName}',
        from_date timestamp default now(),
        version_id uuid default uuid_generate_v4(),
        PRIMARY KEY(version_id)
        );

        ALTER TABLE att_{tableName}_geo ALTER COLUMN version_id type VARCHAR;

        INSERT INTO att_{tableName}_geo (obj_code,obj_name,center_x,center_y,smgeometry)
        select gid ,{nameInList},ST_X(ST_Centroid(geom)),ST_Y(ST_Centroid(geom)),geom from {tableName};

        create table table_temp as select gid as {tableName}_code,{nameInList} as {tableName}_name from {tableName};

        create table att_{tableName}_base as select * from table_temp join {tableName} on table_temp.{tableName}_code={tableName}.gid
        join (select obj_code,from_date,version_id from att_{tableName}_geo ) geo_temp on geo_temp.obj_code={tableName}.gid;

        ALTER TABLE att_{tableName}_base DROP COLUMN gid;
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

        comment on table att_{tableName}_geo is '{tableZhName}空间信息表';
        comment on table att_{tableName}_base is '{tableZhName}基本信息表';
        comment on table rel_{tableName}_ad is '{tableZhName}关联行政区划信息表';\n
                    '''
    for key, value in attributes.items():
        noteSQL=f"COMMENT ON COLUMN att_{tableName}_base.{key} IS '{value['name']}';\n"
        rkSQL=rkSQL+noteSQL
    rkSQL=rkSQL+f"DROP TABLE IF EXISTS table_temp;DROP TABLE IF EXISTS {tableName};"
    cursor.execute(rkSQL)
    postgre_connect.commit()
    print('入库完成')
    cursor.close()
    postgre_connect.close()