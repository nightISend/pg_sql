import pandas as pd
import json

if __name__=="__main__":
    i=0
    dicts={
        "id":[],
        "riverBank":[],
        "mileage":[],
        "machine":[],
        "pile":[],
        "holeSize":[],
        "holeNumber":[],
        "topHeight":[],
        "bottomHeight":[],
        "managePhone":[],
        "managePeople":[],
        "name":[],
        "x":[],
        "y":[],
        "type":[],
        "seawallId":[],
        "attachmentId":[],
        "docId":[]
    }
    data=pd.read_excel('F:\实习\超图(钱管局)实习\旱闸\省管海塘道口旱闸20250319备份.xlsx',sheet_name='备份')
    for index, row in data.iterrows():
        dict={
        "id":'',
        "riverBank":'',
        "mileage":'',
        "machine":'',
        "pile":'',
        "holeSize":'',
        "holeNumber":0,
        "topHeight":'',
        "bottomHeight":'',
        "managePhone":'',
        "managePeople":'',
        "name":'',
        "x":0,
        "y":0,
        "type":'',
        "seawallId":0,
        "attachmentId":None,
        "docId":None
        }
        infos=json.loads(row['基础信息'])

        for key,value in infos.items():
            if key == "geom":
                dict['x']=value['x']
                dict['y']=value['y']
            else:
                dict[key]=value
        for key,value in dict.items():
            dicts[key].append(value)
        i=i+1
        print(f"完成第{i}条记录")
    df = pd.DataFrame(dicts)
    df.to_excel('F:\实习\超图(钱管局)实习\旱闸\hanzha.xlsx')