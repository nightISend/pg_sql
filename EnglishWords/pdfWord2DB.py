import psycopg2
from PyPDF2 import PdfReader

#将pdf里写有单词与其翻译的注记添加到数据库，并统计次数
#要求在写注记时翻译在上，原文在下，如“脱碳 /n decarbonisation”

conn=psycopg2.connect(database="work",user="postgres",password="Lvu123123",host="localhost",port="5433")#数据库连接属性要改
cur=conn.cursor()#创建指针对象

pdf_file = open('F:/英语阅读/3-5_Economist.pdf', 'rb')
pdf_reader = PdfReader(pdf_file)#读取pdf
num_pages = len(pdf_reader.pages)#获取页码

#遍历每一页并获取注释
for page_num in range(num_pages):
    page = pdf_reader.pages[page_num]
    annotations = page.get('/Annots')
    #如果注释存在，遍历注释，查看注记类型。如果是高光则获取注释内容
    if annotations:
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
                    cur.execute("SELECT * FROM english_words where name ='"+word+"';")
                    records = cur.fetchall()

                    #将读取到的数据添加到数据库
                    insertRecord="INSERT INTO public.english_words(name, count, translation)VALUES ('"+word+"',default,'(1):"+translation+"');"
                    if(records==[]):
                        cur.execute(insertRecord)
                        # print("添加")
                    else:
                        for row in records:
                            count=row[1]+1;
                            upCount="UPDATE public.english_words SET name='"+row[0]+"',count="+str(count)+",translation='"+row[2]+";("+str(count)+"):"+translation+"' where name='"+row[0]+"';";
                            # print("修改")
                            cur.execute(upCount)

conn.commit()
cur.close()
conn.close()