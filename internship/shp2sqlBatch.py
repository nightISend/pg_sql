import os
def traversal_files(path):
    dirs=[]#记录文件路径
    files=[]#记录文件夹路径
    for item in os.scandir(path):
        if item.is_dir():
          dirs.append(item.path)
        elif item.is_file():
          files.append(item.path)
    return dirs,files
# 修改路径为存放shp的文件夹
dirs,files=traversal_files('F:\实习\超图(钱管局)实习\shp2sql\ARCGIS')
text=""
for file in files:
    if file[-3:]=='shp':
        data=file.split('\\')
        filename=data[-1].split('.')[0]
        # 修改F:\实习\超图(钱管局)实习\shp2sql\QTJshpSQL为存放shp的sql语句的位置
        text=text+f'shp2pgsql -s 4326 -c -W "UTF-8" {file}>F:\实习\超图(钱管局)实习\shp2sql\QTJshpSQL\{filename}.sql\n'
        # 修改F:\实习\超图(钱管局)实习\shp2sql\QTJshpSQL为存放shp的sql语句的位置
with open('F:\实习\超图(钱管局)实习\shp2sql\QTJshpSQL\shpSQL.txt', 'w') as f:

    f.write(text)