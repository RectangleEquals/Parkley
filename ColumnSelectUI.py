import tkinter as tk
from tkinter import ttk


class ColumnSelectUI:
    def __init__(self, master):
        self.master = master
        self.available_columns = None
        self.selected_columns = None

    def select_columns(self, dialog_title, available_columns):
        self.available_columns = available_columns
        self.selected_columns = None

        # create the dialog window
        dialog = tk.Toplevel(master=self.master, pady=1)
        dialog.title(dialog_title)
        dialog.config(borderwidth=10)

        # create the listbox with available columns
        lb_available_columns = tk.Listbox(dialog, selectmode="multiple")
        for col in self.available_columns:
            lb_available_columns.insert(tk.END, col)
        lb_available_columns.pack(side="left", fill="both", expand=True)

        def handle_select(dlg, lb):
            dlg.withdraw()
            self.selected_columns = lb.curselection()
            dlg.destroy()

        def handle_cancel(dlg, lb):
            dlg.withdraw()
            lb.selection_clear(0, tk.END)
            self.selected_columns = None
            dlg.destroy()

        # create the buttons for selecting and canceling the selection
        btn_select = ttk.Button(dialog, text="Select", command=lambda: handle_select(dialog, lb_available_columns))
        btn_cancel = ttk.Button(dialog, text="Cancel", command=lambda: handle_cancel(dialog, lb_available_columns))
        btn_select.pack(side="bottom", padx=5, pady=5)
        btn_cancel.pack(side="bottom", padx=5, pady=5)

        # run the dialog and return the selected columns
        dialog.grab_set()
        dialog.wait_window()

        if self.selected_columns:
            self.selected_columns = [self.available_columns[int(i)] for i in self.selected_columns]
            return self.selected_columns
        else:
            return None

