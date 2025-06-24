import psycopg2

def update_chain(start_key):
    conn = psycopg2.connect(
    database="supermap", 
    user="postgres", 
    password="Lvu123123",
    host="localhost",
    port="5432")
    cursor = conn.cursor()

    current_key = start_key
    max_iterations = 1000
    iteration_count = 0

    while iteration_count < max_iterations:
        iteration_count += 1

        # 从下游向上游找,x是当前，y是下游
        cursor.execute("""
            SELECT x.updm, y.lc, y.distance,x."河道里"
            FROM dm_lcz x
            JOIN dm_lcz y ON y.user_key = x.dowmdm
            WHERE x.user_key = %s
        """, (current_key,))

        result = cursor.fetchone()

        # if result[3]!=None:
        #     print("结束",current_key)
        #     break

        # 终止条件：distance < 0
        if not result or result[2] < 0:
            break

        # 更新当前记录
        new_updm = result[0]

        print(result)
        new_lc = result[1] - result[2]
        print(new_lc)
        cursor.execute("""
            UPDATE dm_lcz
            SET lc = %s
            WHERE user_key = %s
        """, (new_lc, current_key))

        jd="K"+str(int(new_lc/1000))+"+"+str(int(new_lc%1000))
        print(jd)
        cursor.execute(f"""
            UPDATE dm_lcz
            SET "河道里" = '{jd}'
            WHERE user_key = '{current_key}'
        """)
        # 移动到上游记录
        if new_updm:
            current_key = new_updm
        else:
            break

        conn.commit()
        break
    cursor.close()
    conn.close()


if __name__ == "__main__":
    start_key = 'CSG-109-5'
    update_chain(start_key)