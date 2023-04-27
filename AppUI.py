import time

from tkinter import *
from tkinter import filedialog, ttk

from ColumnSelectUI import ColumnSelectUI
from numpy import array_equal
from UIEvent import UIEvent


class AppUI:
    def __init__(self, title, geometry, resizable, event_handler, event_callbacks):
        # Initialize Tkinter window
        self.master = Tk()
        self.master.title(title)
        self.master.geometry(geometry)
        if not resizable:
            self.master.resizable(False, False)
        else:
            self.master.resizable(True, True)

        self.column_ui = ColumnSelectUI(self.master)

        # Class variables
        self.input_folder = None
        self.output_folder = None
        self.columns = None

        # UI elements
        self.file_progress_label = None
        self.file_progress_bar = None
        self.overall_progress_label = None
        self.overall_progress_bar = None
        self.process_button = None
        self.output_folder_button = None
        self.output_folder_label = None
        self.input_folder_button = None
        self.input_folder_label = None
        self.event_handler = event_handler
        self.event_callbacks = event_callbacks

    # Function to "lazy" initialize UI Elements and then run the main UI loop
    def initialize(self):
        # Create UI elements
        self.input_folder_label = Label(self.master, text="Input Folder:", font=("Arial Bold", 12))
        self.input_folder_label.pack(pady=10)
        self.input_folder_button = Button(self.master, text="Select Input Folder",
                                          command=lambda: self.handle_ui_events(UIEvent(self.input_folder_button)))
        self.input_folder_button.pack(pady=10)
        self.output_folder_label = Label(self.master, text="Output Folder:", font=("Arial Bold", 12))
        self.output_folder_label.pack(pady=10)
        self.output_folder_button = Button(self.master, text="Select Output Folder",
                                           command=lambda: self.handle_ui_events(UIEvent(self.output_folder_button)))
        self.output_folder_button.pack(pady=10)
        self.process_button = Button(self.master, text="Process", bg="green", fg="white", font=("Arial Bold", 14),
                                     width=20, height=2, command=lambda: self.handle_ui_events(UIEvent(self.process_button)))
        self.process_button.pack(pady=10)

        self.file_progress_label = ttk.Label(self.master, text="Current File: 0% | 00:00:00 | 00:00:00", font=("Arial Bold", 8))
        self.file_progress_bar = ttk.Progressbar(self.master, orient="horizontal", length=400, mode="determinate")
        self.file_progress_label.pack(pady=5)
        self.file_progress_bar.pack(pady=5)

        self.overall_progress_label = ttk.Label(self.master, text="Overall: 0% | 00:00:00 | 00:00:00", font=("Arial Bold", 8))
        self.overall_progress_bar = ttk.Progressbar(self.master, orient="horizontal", length=400, mode="determinate")
        self.overall_progress_bar.pack(pady=5)
        self.overall_progress_label.pack(pady=5)

        self.master.mainloop()

    # Function to handle UI events
    def handle_ui_events(self, event):
        if event.event_type == self.input_folder_button:
            self.input_folder = filedialog.askdirectory()
            if self.input_folder and len(self.input_folder) > 0:
                self.input_folder_label.config(text=self.input_folder)
            else:
                self.input_folder_label.config(text="Input Folder:")
            self.event_handler(UIEvent("update_input_folder"))
        elif event.event_type == self.output_folder_button:
            self.output_folder = filedialog.askdirectory()
            if self.input_folder and len(self.input_folder) > 0:
                self.output_folder_label.config(text=self.output_folder)
            else:
                self.output_folder_label.config(text="Output Folder:")
            self.event_handler(UIEvent("update_output_folder"))
        elif event.event_type == self.process_button or event.event_type == "cancel_process":
            if event.event_type == "cancel_process":
                self.input_folder_button.config(state="normal")
                self.output_folder_button.config(state="normal")
                self.process_button.config(text="Process", bg="green", command=lambda: self.handle_ui_events(UIEvent(self.process_button)))
                zero_time = time.gmtime(0)
                self.update_progress_bars(0, zero_time, zero_time, 0, zero_time, zero_time)
            else:
                if not self.input_folder:
                    if not self.event_handler(UIEvent("warning", "Please select input folder!")):
                        return
                if not self.output_folder:
                    if not self.event_handler(UIEvent("warning", "Please select output folder!")):
                        return
                if not self.event_handler(UIEvent("check_processing")):
                    return

                # Update the UI
                self.process_button.config(text="Cancel", bg="red", command=self.event_callbacks.on_cancel_process)
                self.process_button.config(state=NORMAL)
                self.input_folder_button.config(state="disabled")
                self.output_folder_button.config(state="disabled")

                # Notify owner we are ready to begin processing
                self.event_handler(UIEvent("begin_processing"))

    def select_columns(self, dialog_title, columns):
        if not array_equal(columns, self.columns):
            # show column selection dialog
            self.columns = self.column_ui.select_columns(dialog_title, columns)
        return self.columns

    # Function to update progress bars
    def update_progress_bars(self, file_percent, file_elapsed_time, file_eta, overall_percent, overall_elapsed_time, overall_eta):
        # Current file progress
        self.file_progress_bar['value'] = file_percent
        file_elapsed_time_str = time.strftime("%H:%M:%S", file_elapsed_time)
        file_eta_str = time.strftime("%H:%M:%S", file_eta)

        file_percent_str = "N/A"
        if file_percent != 0:
            file_percent_str = format(file_percent, '.2f')
        self.file_progress_label.config(text=f"Current File: {file_percent_str}% | Elapsed: {file_elapsed_time_str} | Remaining: {file_eta_str}")

        # Overall progress
        self.overall_progress_bar['value'] = overall_percent
        overall_elapsed_time_str = time.strftime("%H:%M:%S", overall_elapsed_time)
        overall_eta_str = time.strftime("%H:%M:%S", overall_eta)
        if overall_eta_str == "00:00:00":
            overall_eta_str = file_eta_str

        overall_percent_str = "N/A"
        if overall_percent != 0:
            overall_percent_str = format(overall_percent, '.2f')
        self.overall_progress_label.config(text=f"Overall: {overall_percent_str}% | Elapsed: {overall_elapsed_time_str} | Remaining: {overall_eta_str}")
