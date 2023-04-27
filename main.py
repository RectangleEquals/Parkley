import csv
import duckdb
import math
import os
import pyarrow.parquet as pq
import time
import UIEvent

from AppUI import AppUI
from FunctionThread import FunctionThread
from tkinter import messagebox


class App:
    def __init__(self, title, geometry, resizable=None):
        # Class variables
        self.process_thread = None
        self.input_folder = None
        self.output_folder = None
        # self.columns = ['url', 'fetch_time', 'fetch_status', 'content_languages']
        self.columns = ['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count']
        time_now = time.time()
        self.file_start_time = time_now
        self.file_percent = 0
        self.file_elapsed_time = time_now
        self.file_eta = time_now
        self.overall_total_files = 1
        self.overall_file_index = 0
        self.overall_start_time = time_now
        self.overall_percent = 0
        self.overall_elapsed_time = time_now
        self.overall_eta = time_now
        self.row_steps = 100
        self.con = duckdb.connect(':memory:')

        # Initialize UI events and elements
        callbacks = UIEvent.UIEventCallbacks()
        callbacks.on_cancel_process = self.cancel_process
        self.app_ui = AppUI(title, geometry, resizable, self.handle_ui_event, callbacks)
        self.app_ui.initialize()

    # Function to handle info/warnings/errors dispatched from AppUI
    def handle_ui_event(self, e):
        if e.event_type == "info":
            messagebox.showerror(e.event_message)
        elif e.event_type == "warning":
            messagebox.showwarning(e.event_message)
        elif e.event_type == "error":
            messagebox.showerror(e.event_message)
            return False
        elif e.event_type == "update_input_folder":
            self.input_folder = self.app_ui.input_folder
        elif e.event_type == "update_output_folder":
            self.output_folder = self.app_ui.output_folder
        elif e.event_type == "check_processing":
            if self.process_thread and self.process_thread.is_running():
                self.handle_ui_event(UIEvent.UIEvent("error", "A process is already running!"))
                return False
        elif e.event_type == "begin_processing":
            self.process_thread = FunctionThread(target=self.process_parquet_files, onerror=self.check_last_exception)
            self.process_thread.start()
        return True

    # Function to cancel the current process
    def cancel_process(self):
        if self.process_thread:
            if self.process_thread.is_alive():
                self.process_thread.stop()
            else:
                self.check_last_exception()

        self.app_ui.input_folder_button.config(state="normal")
        self.app_ui.output_folder_button.config(state="normal")
        self.app_ui.file_progress_bar.pack_forget()
        self.app_ui.overall_progress_bar.pack_forget()
        self.app_ui.process_button.config(text="Process", bg="green", command=lambda: self.app_ui.handle_ui_events(self.app_ui.process_button))
        self.update_progress()

    # Function to update progress
    def update_progress(self):
        self.overall_percent = math.ceil(((self.overall_file_index + 1) / self.overall_total_files) * 100)
        self.overall_elapsed_time = time.time() - self.overall_start_time
        self.overall_eta = self.overall_elapsed_time * ((self.overall_total_files - (self.overall_file_index + 1)) / (self.overall_file_index + 1))
        self.app_ui.update_progress_bars(self.file_percent, self.file_elapsed_time, self.file_eta, self.overall_percent, self.overall_elapsed_time, self.overall_eta)

    # Function to search the input folder for parquet files
    def search_for_parquet_files(self):
        parquet_files = []
        for file in os.listdir(self.input_folder):
            if file.endswith(".parquet"):
                parquet_files.append(os.path.join(self.input_folder, file))
        return parquet_files

    # Function to read
    def convert_parquet_file_to_csv(self, input_file, output_file, columns):
        # Open the Parquet file as a table
        table = pq.read_table(input_file, columns=columns)

        # Register PyArrow Table as a DuckDB table
        self.con.register('parquet_table', table)

        # Define range of records to read
        total_rows = table.num_rows
        start_row = 0
        row_steps = self.row_steps
        rows_processed = 0
        total_iterations = math.ceil(total_rows / row_steps)

        # Create a CSV file
        with open(output_file, mode='w', newline='') as file:
            # Create a CSV writer object
            writer = csv.writer(file)

            # Write the header row to the CSV file
            writer.writerow(columns)

            # Start time
            start_time = time.time()

            # Iterate over each range of rows
            for i in range(total_iterations):
                if start_row + row_steps > total_rows:
                    row_steps = total_rows - start_row
                # stop_row = min(start_row + row_steps, total_rows)

                # Perform a table query using DuckDB
                select_query = f"SELECT {', '.join(columns)} FROM parquet_table LIMIT {row_steps} OFFSET {start_row}"
                result = self.con.execute(select_query)
                rows = result.fetchall()

                # Loop over the range of specified rows
                for j in range(len(rows)):
                    # Process the data for the current row as desired
                    row_data = rows[j]
                    writer.writerow(row_data)
                    rows_processed += 1

                # Calculate progress and print progress information
                self.file_percent = (rows_processed / total_rows) * 100
                elapsed_time = time.time() - start_time
                self.file_elapsed_time = time.gmtime(elapsed_time)
                eta = (elapsed_time / (i + 1)) * (total_iterations - i - 1)
                self.file_eta = time.gmtime(eta)
                self.update_progress()

                # Increment the row iteration variables
                start_row += row_steps
                if start_row > total_rows:
                    break

    def check_last_exception(self):
        if self.process_thread.has_exception:
            messagebox.showerror("Error", self.process_thread.get_last_exception())

    # Function to process the parquet files and update progress bar
    def process_parquet_files(self):
        parquet_files = self.search_for_parquet_files()
        self.overall_total_files = len(parquet_files)
        self.overall_start_time = time.time()

        for i, file in enumerate(parquet_files):
            if self.process_thread and not self.process_thread.is_running():
                self.check_last_exception()
                break
            self.overall_file_index = i
            input_file = file
            output_file = os.path.join(self.output_folder, os.path.splitext(os.path.basename(file))[0] + ".csv")
            self.convert_parquet_file_to_csv(input_file, output_file, self.columns)
            self.update_progress()

        # The process has finished, so restore the UI to it's original state
        self.cancel_process()


app = App("Parquet Parley", "500x400")
