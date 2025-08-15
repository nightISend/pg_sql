""" 接口数据入库 """
import psycopg2
import requests
import json


'''
通过数据名称获取接口id
@param tableZNName:数据名称
@param header:请求头
'''
def getTableId(tableZNName,headers):
    # 负载，isPush=0为数仓共享接口，isPush=1为推送接口
    # data={
    # "pageNum": 1,
    # "pageSize": 24,
    # "apiName": tableZNName,
    # "status": "finish",
    # "izPush": 0
    # }
    # resp = requests.post(f"http://10.33.13.185:7777/webapi/market/api/all",headers=headers,data=data)
    # resp.raise_for_status()
    # res = resp.json()
    with open('F:/实习/超图(钱管局)实习/接口数据入库/fileinfo/接口清单.json', 'r', encoding='utf-8') as file:
        res = json.load(file)

    tableList=res['rows']
    for table in tableList:
        if table['apiName']==tableZNName:
            return table['id']
    return '没有找到表'

'''
通过接口id获取接口路径,表名,获取方式,表结构
@param id:接口id
@param header:请求头
'''
def getTableInfoById(id,headers):
    # resp = requests.post(f"http://10.33.13.185:7777/webapi/market/api/visit/{id}",headers=headers)
    # resp.raise_for_status()
    # res=resp.json()

    with open('F:/实习/超图(钱管局)实习/接口数据入库/fileinfo/测站基础信息接口信息.json', 'r', encoding='utf-8') as file:
        res = json.load(file)

    data=res['data']
    apiPath = data['apiPath']
    apiMethod = data['apiMethod']
    fieldList=data['requestParams']
    tableEnName=data['tableName']
    # 获取属性中英名及类型，英文名为键，中文名和类型为值
    attribute={}
    for row in fieldList:
        if row['example']==None:
            attribute[row['paramName']]={"name":row['paramDesc'],"type":row['paramType']}
    return apiPath,apiMethod,attribute,tableEnName

'''
shp表建表语句,建表时自动将字段转小写
@param attribute:表结构,由getTableInfoById生成
@param tableEnName:表名(英文),由getTableInfoById生成
'''
def creatShpSQL(attribute,tableEnName):
    column2CreateTable=''
    for key, value in attribute.items():
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
            CREATE TABLE {tableEnName} (gid serial,{column2CreateTable});
            '''
    return createTableSQL

'''
生成shp表插入语句
@param attribute:表结构,由getTableInfoById生成
@param tableEnName:表名(英文),由getTableInfoById生成
@param apiPath:接口路径,由getTableInfoById生成
@param apiMethod:请求方法,get或post,但好像都是post,暂不用
@param headers:请求头
'''
def creatInsertSQL(attribute,tableEnName,apiPath,apiMethod,headers):
    with open('F:/实习/超图(钱管局)实习/接口数据入库/fileinfo/测站基础信息数据样例.json', 'r', encoding='utf-8') as file:
        res = json.load(file)
    # resp = requests.post(f"http://10.33.13.182:7777/open/api/{apiPath}",headers=headers)
    # resp.raise_for_status()
    # res=resp.json()

    datas=res['data']
    result=''
    values=[]
    keyStr=''
    for key, value in attribute.items():
        values.append({"name":key,"type":value['type']})
        keyStr=keyStr+'"'+to_lower_case(key)+'",'
    keyStr=keyStr+'"geom"'
    for row in datas:
        valueStr=''
        for value in values:
            value2table=''
            # 判断字段是否存在
            try:
                value2table=row[value['name']]
            except:
                value2table='NULL'

            if value['type']=='int' or value['type']=='decimal':
                if value2table==None:
                    value2table='NULL'
            else:
                if value2table==None:
                    value2table="''"
                else:
                    # 半角引号转全角避免入库报错
                    value2table=str(value2table).replace("'","‘")
                    value2table="'"+str(value2table)+"'"

            valueStr=valueStr+f"{value2table},"

        """ 入库前选择坐标字段,下方代码用于生成空间数据 """
        x=row['lgtd']
        y=row['lttd']
        # 坐标错误跳过该记录
        if x=='' or y==''or float(x)>180 or float(x)<-180 or float(y)>90 or float(y)<-90:
            continue
        valueStr=valueStr+createGeomStr(x,y)

        insertSQL=f'INSERT INTO {tableEnName} ({keyStr}) VALUES ({valueStr});\n'
        result=result+insertSQL
    return result

'''
生成点要素，
@param longtitude:经度
@param latitude:纬度
'''
def createGeomStr(longtitude,latitude):
    return f'ST_GeomFromText(\'point({longtitude} {latitude})\',4490)'

'''
大写转小写
@param input_str:文本
'''
def to_lower_case(input_str):
    return input_str.lower()

'''
生成入库SQL
@param tableEnName:表名(英文)
@param tableZhName:表名(中文)
@param attribute:表结构,由getTableInfoById生成
@param attribute2show:用于列表显示的字段,小写
'''
def createRkSQL(tableEnName,tableZhName,attribute,attribute2show):
    rkSQL=f'''
        ALTER TABLE {tableEnName} ALTER COLUMN gid type VARCHAR;

        CREATE TABLE att_{tableEnName}_geo(
        smid serial,
        smuserid int4 default 0, --赋值0就行
        smgeometry geometry,
        obj_code varchar(255),
        obj_name varchar(255),
        center_x float8,
        center_y float8,
        element_type varchar(255) default '{tableEnName}',
        from_date timestamp default now(),
        version_id uuid default uuid_generate_v4(),
        PRIMARY KEY(version_id)
        );

        ALTER TABLE att_{tableEnName}_geo ALTER COLUMN version_id type VARCHAR;

        INSERT INTO att_{tableEnName}_geo (obj_code,obj_name,center_x,center_y,smgeometry)
        select gid ,{attribute2show},ST_X(ST_Centroid(geom)),ST_Y(ST_Centroid(geom)),geom from {tableEnName};

        create table table_temp as select gid as {tableEnName}_code,{attribute2show} as {tableEnName}_name from {tableEnName};

        create table att_{tableEnName}_base as select * from table_temp join {tableEnName} on table_temp.{tableEnName}_code={tableEnName}.gid
        join (select obj_code,from_date,version_id from att_{tableEnName}_geo ) geo_temp on geo_temp.obj_code={tableEnName}.gid;

        ALTER TABLE att_{tableEnName}_base DROP COLUMN gid;
        ALTER TABLE att_{tableEnName}_base DROP COLUMN geom;
        ALTER TABLE att_{tableEnName}_base DROP COLUMN obj_code;
        DROP TABLE IF EXISTS table_temp;

        create table rel_{tableEnName}_ad as select {tableEnName}_code,att_{tableEnName}_geo.from_date,att_{tableEnName}_geo.version_id 
        from att_{tableEnName}_base join att_{tableEnName}_geo on att_{tableEnName}_geo.version_id = att_{tableEnName}_base.version_id;
        ALTER TABLE rel_{tableEnName}_ad ADD COLUMN ad_code VARCHAR default '330000';

        COMMENT ON COLUMN att_{tableEnName}_geo.smid IS '主键id';
        COMMENT ON COLUMN att_{tableEnName}_geo.smuserid IS 'sm用户id';
        COMMENT ON COLUMN att_{tableEnName}_geo.smgeometry IS '空间信息';
        COMMENT ON COLUMN att_{tableEnName}_geo.obj_code IS '要素编码';
        COMMENT ON COLUMN att_{tableEnName}_geo.obj_name IS '要素名称';
        COMMENT ON COLUMN att_{tableEnName}_geo.center_x IS '经度';
        COMMENT ON COLUMN att_{tableEnName}_geo.center_y IS '纬度';
        COMMENT ON COLUMN att_{tableEnName}_geo.element_type IS '要素类型';
        COMMENT ON COLUMN att_{tableEnName}_geo.from_date IS '开始时间';
        COMMENT ON COLUMN att_{tableEnName}_geo.version_id IS '版本id';

        COMMENT ON COLUMN att_{tableEnName}_base.{tableEnName}_code IS '{tableZhName}编码';
        COMMENT ON COLUMN att_{tableEnName}_base.{tableEnName}_name IS '{tableZhName}名称';
        COMMENT ON COLUMN att_{tableEnName}_base.from_date IS '记录生效时间';
        COMMENT ON COLUMN att_{tableEnName}_base.version_id IS '版本id';

        COMMENT ON COLUMN rel_{tableEnName}_ad.{tableEnName}_code IS '{tableZhName}编码';
        COMMENT ON COLUMN rel_{tableEnName}_ad.ad_code IS '行政区划编码';
        COMMENT ON COLUMN rel_{tableEnName}_ad.from_date IS '关系建立时间';
        COMMENT ON COLUMN rel_{tableEnName}_ad.version_id IS '版本id';

        comment on table att_{tableEnName}_geo is '{tableZhName}空间信息表';
        comment on table att_{tableEnName}_base is '{tableZhName}基本信息表';
        comment on table rel_{tableEnName}_ad is '{tableZhName}关联行政区划信息表';\n
                    '''
    for key, value in attribute.items():
        noteSQL=f"COMMENT ON COLUMN att_{tableEnName}_base.{key} IS '{value['name']}';\n"
        rkSQL=rkSQL+noteSQL
    rkSQL=rkSQL+f"DROP TABLE IF EXISTS table_temp;DROP TABLE IF EXISTS {tableEnName};"
    return rkSQL

if __name__=="__main__":
    postgre_connect = psycopg2.connect(
    database="test", 
    user="postgres", 
    password="Lvu123123",
    host="localhost",
    port="5432")
    cursor = postgre_connect.cursor()


    tableZhName='测站基础信息'
    id=getTableId(tableZhName,{})
    apiPath,apiMethod,attribute,tableEnName=getTableInfoById(id,{})
    shpSQL=creatShpSQL(attribute,tableEnName)
    cursor.execute(shpSQL)
    postgre_connect.commit()
    print(f'shp建表')

    insertSQL=creatInsertSQL(attribute,tableEnName,apiPath,apiMethod,{})
    cursor.execute(insertSQL)
    postgre_connect.commit()
    print('shp表插入记录')

    attribute2show='stnm'
    rkSQL=createRkSQL(tableEnName,tableZhName,attribute,attribute2show)
    cursor.execute(rkSQL)
    postgre_connect.commit()
    print('入库完成')
    cursor.close()
    postgre_connect.close()