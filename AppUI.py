import time

from tkinter import *
from tkinter import filedialog, ttk
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

        # Class variables
        self.input_folder = None
        self.output_folder = None

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
        self.input_folder_label = Label(self.master, text="Input Folder: ", font=("Arial Bold", 12))
        self.input_folder_label.pack(pady=10)
        self.input_folder_button = Button(self.master, text="Select Input Folder",
                                          command=lambda: self.handle_ui_events(self.input_folder_button))
        self.input_folder_button.pack(pady=10)
        self.output_folder_label = Label(self.master, text="Output Folder: ", font=("Arial Bold", 12))
        self.output_folder_label.pack(pady=10)
        self.output_folder_button = Button(self.master, text="Select Output Folder",
                                           command=lambda: self.handle_ui_events(self.output_folder_button))
        self.output_folder_button.pack(pady=10)
        self.process_button = Button(self.master, text="Process", bg="green", fg="white", font=("Arial Bold", 14),
                                     width=20, height=2, command=lambda: self.handle_ui_events(self.process_button))
        self.process_button.pack(pady=10)

        self.file_progress_label = ttk.Label(self.master, text="Current File: 0% | 00:00:00 | 00:00:00", font=("Arial Bold", 8))
        self.file_progress_bar = ttk.Progressbar(self.master, orient="horizontal", length=400, mode="determinate")
        self.file_progress_label = ttk.Label(self.master, text="Overall: 0% | 00:00:00 | 00:00:00", font=("Arial Bold", 8))
        self.overall_progress_bar = ttk.Progressbar(self.master, orient="horizontal", length=400, mode="determinate")

        self.master.mainloop()

    # Function to handle UI events
    def handle_ui_events(self, event):
        if event == self.input_folder_button:
            self.input_folder = filedialog.askdirectory()
            self.input_folder_label.config(text=self.input_folder)
            self.event_handler(UIEvent("update_input_folder"))
        elif event == self.output_folder_button:
            self.output_folder = filedialog.askdirectory()
            self.output_folder_label.config(text=self.output_folder)
            self.event_handler(UIEvent("update_output_folder"))
        elif event == self.process_button:
            if not self.input_folder:
                if not self.event_handler(UIEvent("warning", "Please select input folder!")):
                    return
            if not self.output_folder:
                if not self.event_handler(UIEvent("warning", "Please select output folder!")):
                    return
            if not self.event_handler(UIEvent("check_processing")):
                return

            # Update the UI
            self.overall_progress_bar.pack(pady=10)
            self.process_button.config(text="Cancel", bg="red", command=self.event_callbacks.on_cancel_process)
            self.process_button.config(state=NORMAL)
            self.input_folder_button.config(state="disabled")
            self.output_folder_button.config(state="disabled")

            # Notify owner we are ready to begin processing
            self.event_handler(UIEvent("begin_processing"))

    # Function to update progress bars
    def update_progress_bars(self, file_percent, file_elapsed_time, file_eta, overall_percent, overall_elapsed_time, overall_eta):
        # Current file progress
        self.file_progress_bar['value'] = file_percent
        file_elapsed_time_str = time.strftime("%H:%M:%S", file_elapsed_time)
        file_eta_str = time.strftime("%H:%M:%S", file_eta)
        self.file_progress_label['text'] = f"Current File: {file_percent}% | Elapsed: {file_elapsed_time_str} | Remaining: {file_eta_str}"

        # Overall progress
        self.overall_progress_bar['value'] = overall_percent
        overall_elapsed_time_str = time.strftime("%H:%M:%S", overall_elapsed_time)
        overall_eta_str = time.strftime("%H:%M:%S", overall_eta)
        self.overall_progress_label['text'] = f"Overall: {overall_percent}% | Elapsed: {overall_elapsed_time_str} | Remaining: {overall_eta_str}"
