from tkinter import *
from project import main

HEIGHT = 900
WIDTH = 1850

root = Tk()
root.geometry()

canvas = Canvas(root, height=HEIGHT*0.75, width=WIDTH*0.75)
canvas.pack()

frame = Frame(root, bg="#66b3ff")
frame.place(relheight=1, relwidth=1)
top_panel = Frame(frame, bg="#0080ff")
top_panel.place(relx=0.0125, rely=0.025, relheight=0.075, relwidth=0.975)
bottom_panel = Frame(frame, bg="#404040")
bottom_panel.place(relx=0.0125, rely=0.1, relheight=0.875, relwidth=0.975)
entry_label = Label(top_panel, text="Enter SQL:")
entry_label.place(relx=0.0125, rely=0.25, relheight=0.5, relwidth=0.05)
entry = Entry(top_panel)
entry.place(relx=0.0625, rely=0.25, relheight=0.5, relwidth=0.85)
button = Button(top_panel, text="Query", command=main)
button.place(relx=0.9125, rely=0.25, relheight=0.5, relwidth=0.075)
query_text = Text(bottom_panel)
query_text.place(relx=0.0125, rely=0.025, relheight=0.1, relwidth=0.975)
step_text = Text(bottom_panel)
step_text.place(relx=0.0125, rely=0.15, relheight=0.825, relwidth=0.48125)
annotation_text = Text(bottom_panel)
annotation_text.place(relx=0.50625, rely=0.15, relheight=0.825, relwidth=0.48125)

'''
COLOUR PALETTE

#3399ff light blue
#0066ff blue
#404040 gray
#4dff88 green

'''