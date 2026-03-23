'''excel转sql文件'''
import pandas as pd

# -------------------------- 配置参数（请修改这部分） --------------------------
EXCEL_FILE_PATH = "F:/校友林/结果/未认捐树完整表.xlsx"  # 你的Excel文件路径
OUTPUT_SQL_PATH = "F:/校友林/数据库备份/发布数据/未认捐树.sql" # 生成的SQL文件路径
TARGET_TABLE_NAME = "att_tree_geo"  # 数据库目标表名
SHEET_NAME = 0  # 读取Excel的第1个sheet（也可填sheet名称，如"Sheet1"）

# -------------------------- 核心转换函数 --------------------------
def excel_to_sql():
    # 1. 读取Excel数据（无多余检查，直接读取）
    df = pd.read_excel(EXCEL_FILE_PATH, sheet_name=SHEET_NAME)
    # 获取列名（即数据库字段名）
    columns = list(df.columns)
    columns.remove('x')
    columns.remove('y')
    columns.append('geom')
    # 字段名拼接（用于INSERT语句）
    columns_str = ", ".join(columns)

    # 2. 打开SQL文件，准备写入
    with open(OUTPUT_SQL_PATH, "w", encoding="utf-8") as f:
        # 逐行处理Excel数据，生成插入语句
        for _, row in df.iterrows():
            # 处理每行的数值：字符串加单引号，空值转NULL，数值保持原样
            values = []
            for col in columns:

                if col=='geom':
                    values.append(f"ST_GeomFromText(\'Point({str(row['x'])+' '+str(row['y'])})\', 4326)")
                    continue

                val = row[col]
                # 处理空值（NaN/None）
                if pd.isna(val):
                    values.append("NULL")
                # 处理字符串类型（含日期）
                elif isinstance(val, (str, pd.Timestamp)):
                    # 转字符串并处理内部单引号（避免SQL语法错误）
                    str_val = str(val).replace("'", "''")
                    values.append(f"'{str_val}'")
                # 处理数值类型（int/float）
                else:
                    values.append(str(val))
            
            # 拼接INSERT语句
            values_str = ", ".join(values)
            insert_sql = f"INSERT INTO {TARGET_TABLE_NAME} ({columns_str}) VALUES ({values_str});\n"
            # 写入SQL文件
            f.write(insert_sql)

    print(f"✅ 转换完成！SQL文件已生成：{OUTPUT_SQL_PATH}")
    print(f"📊 共生成 {len(df)} 条插入语句")

# -------------------------- 执行脚本 --------------------------
if __name__ == "__main__":
    excel_to_sql()