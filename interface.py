from tkinter import *
from annotation import *
from preprocessing import *
from project import *

HEIGHT = 1000
WIDTH = 1000

root = Tk()
root.geometry()

# canvas = Canvas(root, height=HEIGHT, width=WIDTH, bg="black")
# canvas.pack()


def qep_button():
    text.delete("1.0", END)
    sql_query = entry.get()
    if sql_query == "test":
        sql_query = "SELECT * from customer C, orders O where C.c_custkey = O.o_custkey"

    text.insert(END, "SQL Query:" + "\n")
    text.insert(END, sql_query + "\n\n")

    db = Database()
    db.connect(host='localhost', port='5432', database='TPC-H', user='postgres', pw='1121')

    qep = db.execute_query(query=sql_query, explain=True, analyze=False)
    query_steps = preprocessing(qep)
    output = annotate(query_steps)

    text.insert(END, "QEP:" + "\n")
    for x in qep:
        text.insert(END, str(x) + "\n")

    text.insert(END, "\n")

    text.insert(END, "Annotation:" + "\n")
    for x in output:
        text.insert(END, x + "\n")

    entry.delete(0, END)
    db.disconnect()


label = Label(root, text="SQL Query:")
label.grid(row=0, column=0)
entry = Entry(root, width=60)
entry.grid(row=0, column=1)
button = Button(root, text="QEP", command=qep_button)
button.grid(row=0, column=2, ipadx=50)
text = Text(root, width=100)
text.grid(row=2, columnspan=3)
