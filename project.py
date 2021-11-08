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


if __name__ == '__main__':
    root.mainloop()

