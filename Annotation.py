import psycopg2

def Connect(user,pw):
    # Connect to your postgres DB
    conn = psycopg2.connect(host="localhost", port=5432, database="TPC-H", user=user, password=pw)

    # Open a cursor to perform database operations
    cur = conn.cursor()

    # Execute a query
    sql_query = "SELECT * from customer C, orders O where C.c_custkey = O.o_custkey"
    cur.execute(cur.mogrify('explain analyze ' + sql_query))

    # Retrieve query results
    query_results = cur.fetchall()
    print(query_results)
    for query in query_results:
        print(query)

    cur.close()
    conn.close()


def main():
    Connect("postgres", "123")

if __name__ == '__main__':
    main()