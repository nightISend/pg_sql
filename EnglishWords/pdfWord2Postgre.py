import psycopg2
from PyPDF2 import PdfReader

#将pdf里写有单词与其翻译的注记添加到数据库，并统计次数
#要求在写注记时翻译在上，原文在下，如“脱碳 /n decarbonisation”

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


#将pdf里写有单词与其翻译的注记添加到数据库，并统计次数
#要求在写注记时翻译在上，原文在下，如“脱碳 /n decarbonisation”
#注意不要在语句里加入单双引号，防止sql注入导致报错

if __name__=="__main__":
    conn=psycopg2.connect(database="work",user="postgres",password="Lvu123123",host="localhost",port="5433")#数据库连接属性要改
    cur=conn.cursor()#创建指针对象

    try:
        sql='''
        CREATE TABLE english_words (
            name character varying(100) NOT NULL,
            count integer DEFAULT 1,
            translation character varying(100)
        );
        '''
        cur.execute(sql)
    except:
        print("表已存在")

    dirs,files=traversal_files('F:\英语阅读')
    for row in files:
        print(row)
        pdf_file = open(row, 'rb')
        pdf_reader = PdfReader(pdf_file)#读取pdf
        num_pages = len(pdf_reader.pages)#获取页码

        #遍历每一页并获取注释
        for page_num in range(num_pages):
            page = pdf_reader.pages[page_num]
            annotations = page.get('/Annots')
            #如果注释存在，遍历注释，查看注记类型。如果是高光则获取注释内容
            if annotations:
                #annotations可能不是数组
                if str(type(annotations))=='<class \'PyPDF2.generic._base.IndirectObject\'>':
                    subtype = annotation.get_object()["/Subtype"]
                    if subtype == "/Highlight":
                            text=annotation.get_object()["/Contents"]
                            t_s=text.split("\n")#t_s[0]是英文单词，[1]是翻译

                            try:
                                translation=t_s[0]#获取翻译
                                word=t_s[1]#获取单词
                            except:
                                print("数据不完整"+translation)
                            else:
                                cur.execute("SELECT * FROM english_words where name ='"+word+"';")
                                records = cur.fetchall()

                                #将读取到的数据添加到数据库
                                insertRecord="INSERT INTO english_words(name, count, translation)VALUES ('"+word+"',default,'(1):"+translation+"');"
                                if(records==[]):
                                    cur.execute(insertRecord)
                                    # print("添加")
                                else:
                                    for row in records:
                                        count=row[1]+1;
                                        upCount="UPDATE english_words SET name='"+row[0]+"',count="+str(count)+",translation='"+row[2]+";("+str(count)+"):"+translation+"' where name='"+row[0]+"';";
                                        # print("修改")
                                        cur.execute(upCount)                
                else:
                    for annotation in annotations:
                        subtype = annotation.get_object()["/Subtype"]            
                        if subtype == "/Highlight":
                            text=annotation.get_object()["/Contents"]
                            t_s=text.split("\n")#t_s[0]是英文单词，[1]是翻译

                            try:
                                translation=t_s[0]#获取翻译
                                word=t_s[1]#获取单词
                            except:
                                print("数据不完整"+translation)
                            else:
                                try:
                                    cur.execute("SELECT * FROM english_words where name ='"+word+"';")
                                except:
                                    print(word)
                                records = cur.fetchall()

                                #将读取到的数据添加到数据库
                                insertRecord="INSERT INTO english_words(name, count, translation)VALUES ('"+word+"',default,'(1):"+translation+"');"
                                if(records==[]):
                                    cur.execute(insertRecord)
                                    # print("添加")
                                else:
                                    for row in records:
                                        count=row[1]+1;
                                        upCount="UPDATE english_words SET name='"+row[0]+"',count="+str(count)+",translation='"+row[2]+";("+str(count)+"):"+translation+"' where name='"+row[0]+"';";
                                        # print("修改")
                                        cur.execute(upCount)

conn.commit()
cur.close()
conn.close()