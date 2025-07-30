""" 查看字典长度,避免shp吞属性名,不要读取.xls文件 """
import pandas as pd
if __name__=="__main__":
    infoExcel_path='F:/实习/超图(钱管局)实习/水库/rk.xlsx'
    data=pd.read_excel(infoExcel_path,sheet_name="Sheet2",keep_default_na=False)

    for index, row in data.iterrows():
        enName=row['英文属性名']
        chName=row['中文属性名']
        if len(enName)>10:
            print(chName,enName,len(enName))