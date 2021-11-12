import psycopg2
from annotation import *
from preprocessing import *
from interface import *
import interface as gui


class Database:
    def __init__(self):
        self.conn = None

    def connect(self, host, port, database, user, pw):
        # Connect to your postgres DB
        print(host, port, database, user, pw)
        self.conn = psycopg2.connect(host=host, port=port, database=database, user=user, password=pw)

    def disconnect(self):
        self.conn.close()

    def execute_query(self, query, explain=True, analyze=False, json=False):
        if self.conn is None:
            print("Database not connected!")
            gui.query_text.insert(END, "ERROR: Database not connected!")
            return

        # Open a cursor to perform database operations
        cur = self.conn.cursor()

        # Execute a query
        header = ''
        if explain:
            header += 'explain '
        if analyze:
            header += 'analyze '
        if json:
            header += '(FORMAT JSON) '
        try:
            cur.execute(cur.mogrify(header + query))
        except:
            print("SQL query execution failed. Please check SQL query.")
            gui.query_text.insert(END, "ERROR: SQL query execution failed. Please check SQL query.")
            return None

        # Retrieve query results
        query_results = cur.fetchall()
        print("Query Results: ")
        print(query_results)
        print()

        cur.close()
        return query_results


db1 = Database()


def main():
    # clear text fields
    gui.query_text.delete("1.0", END)
    gui.step_text.delete("1.0", END)
    gui.annotation_text.delete("1.0", END)

    sql_query = gui.entry.get()
    if sql_query == "1":
        sql_query = "SELECT * from customer AS C, orders AS O where C.c_custkey = O.o_custkey"
    if sql_query == "test":
        sql_query = "SELECT s.s_acctbal, s.s_name, n.n_name, p.p_partkey, p.p_mfgr, s.s_address, s.s_phone, s.s_comment FROM part p, supplier s, partsupp ps, nation n, region r WHERE p.p_partkey != ps.ps_partkey AND s.s_suppkey = ps.ps_suppkey AND p.p_size = 15 AND p.p_type LIKE '%BRASS' AND s.s_nationkey = n.n_nationkey AND n.n_regionkey = r.r_regionkey AND r.r_name = 'EUROPE' AND ps.ps_supplycost = ( SELECT MIN(ps1.ps_supplycost) FROM partsupp ps1, supplier s1, nation n1, region r1 WHERE p.p_partkey = ps1.ps_partkey AND s1.s_suppkey = ps1.ps_suppkey AND s1.s_nationkey = n1.n_nationkey AND n1.n_regionkey = r1.r_regionkey AND r1.r_name = 'EUROPE') ORDER BY s.s_acctbal DESC, n.n_name, s.s_name, p.p_partkey "
    gui.entry.delete(0, END)

    # sql_query = "SELECT * from customer AS C, orders AS O where C.c_custkey = O.o_custkey"
    # sql_query = "SELECT PS.ps_availqty from partsupp PS, supplier S, customer C, orders O where S.s_suppkey = PS.ps_suppkey and C.c_custkey = O.o_custkey ORDER BY S.s_name"
    # sql_query = "SELECT C.c_custkey,C.c_name,N.n_name " \
    #             "from customer C, Nation N, Orders O " \
    #             "where C.c_nationkey = N.n_nationkey " \
    #             "and C.c_custkey = O.o_custkey " \
    #             "AND N.n_nationkey = ( " \
    #             "SELECT S.s_nationkey " \
    #             "FROM partsupp PS, Supplier S " \
    #             "WHERE S.s_suppkey = Ps.ps_suppkey " \
    #             "AND PS.ps_partkey = ( " \
    #             "SELECT MAX(P.p_retailprice) " \
    #             "FROM Part P " \
    #             "WHERE p.p_size > 10 " \
    #             ") " \
    #             ") "
    qep = db1.execute_query(query=sql_query, explain=True, analyze=False, json=True)
    if qep is None:
        return
    processed_qep = preprocess_json(qep)
    output_steps, annotated_query = annotate_json(processed_qep, sql_query)

    print('\n================================ STEPS ===================================')
    for step in output_steps:
        print(step)
    print('==========================================================================')
    print('\n=========================== ANNOTATED QUERY ==============================')
    for line in annotated_query:
        print(line)
    print('==========================================================================')

    # populate text fields
    gui.query_text.insert(END, "SQL Query: " + sql_query)
    gui.step_text.insert(END, "STEPS:" + "\n")
    for step in output_steps:
        gui.step_text.insert(END, step + "\n")
    gui.annotation_text.insert(END, "ANNOTATED QUERY:" + "\n")
    gui.annotation_text.tag_config("blue", foreground="blue")
    for line in annotated_query:
        gui.annotation_text.insert(END, line + "\n")
    # add blue
    blue()


def login():
    host = gui.host_entry.get()
    port = gui.port_entry.get()
    db_name = gui.db_entry.get()
    username = gui.user_entry.get()
    password = gui.pw_entry.get()
    gui.message.delete("1.0", END)

    print(host, port, db_name, username, password)
    gui.message.insert(END, "Connecting to database...")

    try:
        db1.connect(host=host, port=port, database=db_name, user=username, pw=password)
        gui.message.insert(END, "Login Success")
        gui.home_screen()
        print('DB Connected')

        gui.host_entry.delete("0", END)
        gui.port_entry.delete("0", END)
        gui.db_entry.delete("0", END)
        gui.user_entry.delete("0", END)
        gui.pw_entry.delete("0", END)
        gui.message.delete("1.0", END)

    except:
        print('Database connection failed. Please check database service or login info.')
        gui.message.delete("1.0", END)
        gui.message.insert(END, "Database connection failed. Please check database service or login info.")


def logout():
    print('DB Disconnected')
    db1.disconnect()


def blue():
    gui.annotation_text.tag_add("blue", "5.0", "5.100")
    gui.annotation_text.tag_add("blue", "7.0", "7.100")
    gui.annotation_text.tag_add("blue", "9.0", "9.100")
    gui.annotation_text.tag_add("blue", "11.0", "11.100")
    gui.annotation_text.tag_add("blue", "13.0", "13.100")
    gui.annotation_text.tag_add("blue", "15.0", "15.100")
    gui.annotation_text.tag_add("blue", "17.0", "17.100")
    gui.annotation_text.tag_add("blue", "19.0", "19.100")
    gui.annotation_text.tag_add("blue", "21.0", "21.100")
    gui.annotation_text.tag_add("blue", "23.0", "23.100")
    gui.annotation_text.tag_add("blue", "25.0", "25.100")
    gui.annotation_text.tag_add("blue", "27.0", "27.100")
    gui.annotation_text.tag_add("blue", "29.0", "29.100")
    gui.annotation_text.tag_add("blue", "31.0", "31.100")
    gui.annotation_text.tag_add("blue", "33.0", "33.100")
    gui.annotation_text.tag_add("blue", "35.0", "35.100")
    gui.annotation_text.tag_add("blue", "37.0", "37.100")
    gui.annotation_text.tag_add("blue", "39.0", "39.100")
    gui.annotation_text.tag_add("blue", "41.0", "41.100")
    gui.annotation_text.tag_add("blue", "43.0", "43.100")
    gui.annotation_text.tag_add("blue", "45.0", "45.100")
    gui.annotation_text.tag_add("blue", "47.0", "47.100")
    gui.annotation_text.tag_add("blue", "49.0", "49.100")


if __name__ == '__main__':
    account_login_screen()
