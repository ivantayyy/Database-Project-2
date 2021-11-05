import psycopg2

from annotation import *
from preprocessing import *


class Database:
    def __init__(self):
        self.conn = None

    def connect(self, host, port, database, user, pw):
        # Connect to your postgres DB
        self.conn = psycopg2.connect(host="localhost", port=5432, database="TPC-H", user=user, password=pw)

    def disconnect(self):
        self.conn.close()

    def execute_query(self, query, explain=True, analyze=False):
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
        cur.execute(cur.mogrify(header + query))

        # Retrieve query results
        query_results = cur.fetchall()
        print(query_results)

        cur.close()
        return query_results


def main():
    db = Database()
    db.connect(host='localhost', port='5432', database='TPC-H', user='postgres', pw='1121')

    sql_query = "SELECT * from customer C, orders O where C.c_custkey = O.o_custkey"
    # sql_query = "SELECT PS.ps_availqty from partsupp PS, supplier S, customer C, orders O where S.s_suppkey = PS.ps_suppkey and C.c_custkey = O.o_custkey ORDER BY S.s_name"

    qep = db.execute_query(query=sql_query, explain=True, analyze=False)

    # qep = [('Hash Join  (cost=10330.00..108538.61 rows=1500000 width=266) (actual time=633.256..6622.343 rows=1500000 loops=1)',), ('  Hash Cond: (o.o_custkey = c.c_custkey)',), ('  ->  Seq Scan on orders o  (cost=0.00..41095.00 rows=1500000 width=107) (actual time=1.757..1270.994 rows=1500000 loops=1)',), ('  ->  Hash  (cost=5085.00..5085.00 rows=150000 width=159) (actual time=625.174..625.175 rows=150000 loops=1)',), ('        Buckets: 32768  Batches: 8  Memory Usage: 3858kB',), ('        ->  Seq Scan on customer c  (cost=0.00..5085.00 rows=150000 width=159) (actual time=0.849..300.105 rows=150000 loops=1)',), ('Planning Time: 25.436 ms',), ('Execution Time: 6715.488 ms',)]

    # qep = [('Gather Merge  (cost=174487386432.28..291162198935.89 rows=1000000000000 width=30)',),
    #        ('  Workers Planned: 2',),
    #        ('  ->  Sort  (cost=174487385432.26..175737385432.26 rows=500000000000 width=30)',),
    #        ('        Sort Key: s.s_name',),
    #        ('        ->  Parallel Hash Join  (cost=2817785304.56..5552198835.64 rows=500000000000 width=30)',),
    #        ('              Hash Cond: (o.o_custkey = c.c_custkey)',),
    #        ('              ->  Parallel Seq Scan on orders o  (cost=0.00..32345.00 rows=625000 width=4)',),
    #        ('              ->  Parallel Hash  (cost=1802160304.56..1802160304.56 rows=50000000000 width=34)',),
    #        ('                    ->  Nested Loop  (cost=447.42..1802160304.56 rows=50000000000 width=34)',),
    #        ('                          ->  Hash Join  (cost=447.00..22106.70 rows=333333 width=30)',),
    #        ('                                Hash Cond: (ps.ps_suppkey = s.s_suppkey)',), (
    #            '                                ->  Parallel Seq Scan on partsupp ps  (cost=0.00..20784.33 rows=333333 width=8)',),
    #        ('                                ->  Hash  (cost=322.00..322.00 rows=10000 width=30)',), (
    #            '                                      ->  Seq Scan on supplier s  (cost=0.00..322.00 rows=10000 width=30)',),
    #        (
    #            '                          ->  Index Only Scan using customer_pkey on customer c  (cost=0.42..3906.42 rows=150000 width=4)',)]
    query_steps = preprocessing(qep)
    annotate(query_steps)

    db.disconnect()


if __name__ == '__main__':
    main()
