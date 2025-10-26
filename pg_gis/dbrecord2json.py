import psycopg2
import json
from datetime import datetime, date


def postgres_geom_to_json(db_config, table_name, output_file, geom_column='geom'):
    """
    读取PostgreSQL表数据（含geometry字段）并导出为JSON文件，将空间字段转为WKT格式
    
    参数:
        db_config: 数据库连接配置字典
        table_name: 要读取的表名
        output_file: 输出的JSON文件名
        geom_column: 空间字段名（默认'geom'）
    """
    conn = None
    cur = None
    try:
        # 连接数据库
        conn = psycopg2.connect(
            host=db_config['host'],
            port=db_config['port'],
            database=db_config['database'],
            user=db_config['user'],
            password=db_config['password']
        )
        print(f"成功连接到数据库: {db_config['database']}")

        # 创建游标
        cur = conn.cursor()

        # 1. 获取表的所有字段名（用于构造查询语句）
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = %s 
            ORDER BY ordinal_position
        """, (table_name,))
        columns = [row[0] for row in cur.fetchall()]
        if not columns:
            print(f"表 {table_name} 不存在或无字段")
            return

        # 2. 构造查询语句：对空间字段使用ST_AsText转换为WKT，其他字段正常查询
        select_clauses = []
        for col in columns:
            if col == geom_column:
                # 使用PostGIS函数将geometry转为WKT文本
                select_clauses.append(f"ST_AsText({col}) AS {col}")
            else:
                select_clauses.append(col)
        query_sql = f"SELECT {', '.join(select_clauses)} FROM {table_name};"
        print(f"执行查询: {query_sql}")

        # 3. 执行查询
        cur.execute(query_sql)
        
        # 4. 获取结果列名（与原表一致，空间字段已转为WKT）
        result_columns = [desc[0] for desc in cur.description]
        
        # 5. 获取所有行数据
        rows = cur.fetchall()
        print(f"从表 {table_name} 中读取到 {len(rows)} 条记录")

        # 6. 处理数据并转换为字典列表（空间字段已为WKT字符串，无需额外处理）
        result = []
        for row in rows:
            row_dict = {}
            for col, value in zip(result_columns, row):
                # 处理日期时间类型
                if isinstance(value, (datetime, date)):
                    row_dict[col] = value.isoformat()
                # 其他类型直接保留（包括已转为字符串的WKT）
                else:
                    row_dict[col] = value
            result.append(row_dict)

        # 7. 写入JSON文件
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"数据已成功导出到 {output_file}（空间字段已转为WKT）")

    except psycopg2.OperationalError as e:
        print(f"数据库连接失败: {e}")
    except psycopg2.ProgrammingError as e:
        if 'ST_AsText' in str(e):
            print(f"错误：未找到ST_AsText函数，请确保数据库已安装PostGIS扩展。详情：{e}")
        else:
            print(f"SQL执行错误（可能表或字段不存在）: {e}")
    except Exception as e:
        print(f"发生错误: {e}")
    finally:
        # 关闭游标和连接
        if cur:
            cur.close()
        if conn:
            conn.close()
        print("数据库连接已关闭")


if __name__ == "__main__":
    # 数据库配置（请根据实际情况修改）
    db_config = {
        'host': 'localhost',      # 数据库主机地址
        'port': '5432',           # 数据库端口（默认5432）
        'database': 'xyl',    # 数据库名
        'user': 'postgres',      # 用户名
        'password': 'Lvu123123'   # 密码
    }

    # 要读取的表名（请修改）
    table_name = 'trees'

    # 输出的JSON文件名（请修改）
    output_file = 'F:/Repo/xiaoYouLin/public/data/trees.json'

    # 执行导出
    postgres_geom_to_json(db_config, table_name, output_file)