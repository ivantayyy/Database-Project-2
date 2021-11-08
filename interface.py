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
    query_label = Label(root, text="Query:")
    query_label.grid(row=1, column=0)

    sql_query = entry.get()
    if sql_query == "test":
        sql_query = "SELECT * from customer C, orders O where C.c_custkey = O.o_custkey"
    query_string = Label(root, text=sql_query)
    query_string.grid(row=1, column=1)

    db = Database()
    db.connect(host='localhost', port='5432', database='TPC-H', user='postgres', pw='1121')

    qep = db.execute_query(query=sql_query, explain=True, analyze=False)
    query_steps = preprocessing(qep)
    output = annotate(query_steps)

    count = 0
    for x in output:
        step_label = Label(root, text="Step " + str(count+1))
        step_label.grid(row=count+2, column=0)
        qep_step = Label(root, text=x)
        qep_step.grid(row=count+2, column=1)
        count += 1

    # qep_big = Label(root, text=qep)
    # qep_big.grid(row=8, column=0, columnspan =3)
    button = Button(root, text="Reset", command=reset)
    button.grid(row=9, column=1, ipadx=80)

    query_steps = preprocessing(qep)
    annotate(query_steps)

    entry.delete(0, END)
    db.disconnect()


def reset():
    pass


label = Label(root, text="SQL Query:")
label.grid(row=0, column=0)
entry = Entry(root, width=30)
entry.grid(row=0, column=1)
button = Button(root, text="QEP", command=qep_button)
button.grid(row=0, column=2, ipadx=50)

