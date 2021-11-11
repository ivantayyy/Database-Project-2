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
    #sql_query = "SELECT * from customer AS C, orders AS O where C.c_custkey = O.o_custkey"
    sql_query = "SELECT s.s_acctbal, s.s_name, n.n_name, p.p_partkey, p.p_mfgr, s.s_address, s.s_phone, s.s_comment FROM part p, supplier s, partsupp ps, nation n, region r WHERE p.p_partkey != ps.ps_partkey AND s.s_suppkey = ps.ps_suppkey AND p.p_size = 15 AND p.p_type LIKE '%BRASS' AND s.s_nationkey = n.n_nationkey AND n.n_regionkey = r.r_regionkey AND r.r_name = 'EUROPE' AND ps.ps_supplycost = ( SELECT MIN(ps1.ps_supplycost) FROM partsupp ps1, supplier s1, nation n1, region r1 WHERE p.p_partkey = ps1.ps_partkey AND s1.s_suppkey = ps1.ps_suppkey AND s1.s_nationkey = n1.n_nationkey AND n1.n_regionkey = r1.r_regionkey AND r1.r_name = 'EUROPE') ORDER BY s.s_acctbal DESC, n.n_name, s.s_name, p.p_partkey "
    #sql_query = "SELECT PS.ps_availqty from partsupp PS, supplier S, customer C, orders O where S.s_suppkey = PS.ps_suppkey and C.c_custkey = O.o_custkey ORDER BY S.s_name"
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
    qep = db.execute_query(query=sql_query, explain=True, analyze=False, json=True)
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

    db.disconnect()


if __name__ == '__main__':
    main()
    # root.mainloop()

