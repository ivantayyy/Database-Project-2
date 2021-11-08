import psycopg2
from annotation import *
from preprocessing import *
from interface import *


class Database:
    def __init__(self):
        self.conn = None

    def connect(self, host, port, database, user, pw):
        # Connect to your postgres DB
        self.conn = psycopg2.connect(host="localhost", port=5432, database="TPC-H", user=user, password=pw)

    def disconnect(self):
        self.conn.close()

    def execute_query(self, query, explain=True, analyze=False, json=False):
        if self.conn is None:
            print("Database not connected!")
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
        cur.execute(cur.mogrify(header + query))

        # Retrieve query results
        query_results = cur.fetchall()
        print(query_results)

        cur.close()
        return query_results


def main():
    db = Database()
    db.connect(host='localhost', port='5432', database='TPC-H', user='postgres', pw='1121')
    sql_query = "SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment FROM part, supplier, partsupp, nation, region WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size = 15 AND p_type LIKE '%BRASS' AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'EUROPE' AND ps_supplycost = ( SELECT MIN(ps_supplycost) FROM partsupp, supplier, nation, region WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'EUROPE') ORDER BY s_acctbal DESC, n_name, s_name, p_partkey "
    #sql_query = "SELECT PS.ps_availqty from partsupp PS, supplier S, customer C, orders O where S.s_suppkey = PS.ps_suppkey and C.c_custkey = O.o_custkey ORDER BY S.s_name"
    qep = db.execute_query(query=sql_query, explain=True, analyze=False, json=True)
    processed_qep = preprocess_json(qep)
    annotation = annotate_json(processed_qep, sql_query)
    db.disconnect()


if __name__ == '__main__':
    main()
    # root.mainloop()

