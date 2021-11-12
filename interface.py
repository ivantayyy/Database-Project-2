from tkinter import *
from project import main
from project import login
from project import logout

HEIGHT = 900
WIDTH = 1850


def home_screen():
    global home
    global entry
    global query_text
    global step_text
    global annotation_text

    login_screen.withdraw()
    home = Toplevel(login_screen)
    home.title("Home")
    canvas = Canvas(home, height=HEIGHT * 0.75, width=WIDTH * 0.75)
    canvas.pack()
    frame = Frame(home, bg="#66b3ff")  # light blue
    frame.place(relheight=1, relwidth=1)
    top_panel = Frame(frame, bg="#0080ff")  # blue
    top_panel.place(relx=0.0125, rely=0.025, relheight=0.075, relwidth=0.975)
    bottom_panel = Frame(frame, bg="#404040")  # gray
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

    home.protocol("WM_DELETE_WINDOW", on_closing)


def account_login_screen():
    global login_screen
    global host_entry
    global port_entry
    global db_entry
    global user_entry
    global pw_entry
    global message
    login_screen = Tk()
    login_screen.geometry("400x450")
    login_screen.title("Database Login")
    Label(text="Database Login", bg="#66b3ff", width="300", height="2", font=("Calibri", 13)).pack()
    Label(text="").pack()
    host_label = Label(login_screen, text="Hostname")
    host_label.pack()
    host_entry = Entry(login_screen)
    host_entry.pack()
    port_label = Label(login_screen, text="Port")
    port_label.pack()
    port_entry = Entry(login_screen)
    port_entry.pack()
    db_label = Label(login_screen, text="Database")
    db_label.pack()
    db_entry = Entry(login_screen)
    db_entry.pack()
    username_label = Label(login_screen, text="Username")
    username_label.pack()
    user_entry = Entry(login_screen)
    user_entry.pack()
    password_label = Label(login_screen, text="Password")
    password_label.pack()
    pw_entry = Entry(login_screen)
    pw_entry.pack()
    Label(text="").pack()
    Button(text="Connect", height="1", width="30", command=login).pack()
    Label(text="").pack()
    message = Text(login_screen, height=6, width=35)
    message.pack()

    login_screen.mainloop()


def on_closing():
    logout()
    home.destroy()
    login_screen.deiconify()
