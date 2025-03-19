from PyPDF2 import PdfReader

pdf_file = open('F:\英语阅读\The_New_Yorker_-_March_4_2024.pdf', 'rb')
pdf_reader = PdfReader(pdf_file)#读取pdf
num_pages = len(pdf_reader.pages)#获取页码
page = pdf_reader.pages[50]

print(page)
# annotations = page.get('/Annots')
# # print(type(annotations))
# if str(type(annotations))=='<class \'PyPDF2.generic._base.IndirectObject\'>':
#     annotations=[annotations]
#     print(annotations)
# # print(str(type(annotations)))