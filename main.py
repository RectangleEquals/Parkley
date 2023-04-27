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
        self.columns = None
        self.row_steps = 100
        self.con = duckdb.connect(':memory:')

        # Progress variables
        time_now = time.time()
        self.file_start_time = time_now
        self.file_percent = 0
        self.file_elapsed_time = time_now
        self.file_eta = time_now
        self.overall_total_files = 0
        self.overall_file_index = 0
        self.overall_start_time = time_now
        self.overall_percent = 0
        self.overall_elapsed_time = time_now
        self.overall_eta = time_now

        # Initialize UI events and elements
        callbacks = UIEvent.UIEventCallbacks()
        callbacks.on_cancel_process = self.cancel_process
        self.app_ui = AppUI(title, geometry, resizable, self.handle_ui_event, callbacks)
        self.app_ui.initialize()

    # Function to handle info/warnings/errors dispatched from AppUI
    def handle_ui_event(self, e):
        if e.event_type == "info":
            messagebox.showerror(e.event_data)
        elif e.event_type == "warning":
            messagebox.showwarning(e.event_data)
        elif e.event_type == "error":
            messagebox.showerror(e.event_data)
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
            self.process_thread = FunctionThread(target=self.process_parquet_files, on_error=self.check_thread_error, on_done=self.check_thread_done)
            self.process_thread.start()
        return True

    # Function to cancel the current process
    def cancel_process(self):
        if self.is_processing():
            self.process_thread.stop()
            self.check_thread_error()
            #if not self.check_thread_done():
        self.app_ui.handle_ui_events(UIEvent.UIEvent("cancel_process"))
        self.update_progress()

    def is_processing(self):
        if self.process_thread:
            if self.process_thread.is_running():
                return True
        return False

    # Function to update progress
    def update_progress(self):
        self.overall_percent = math.ceil(((self.overall_file_index + 1) / (self.overall_total_files + 1)) * 100)
        overall_elapsed_time = time.time() - self.overall_start_time
        self.overall_elapsed_time = time.gmtime(overall_elapsed_time)
        overall_eta = overall_elapsed_time * ((self.overall_total_files - (self.overall_file_index + 1)) / (self.overall_file_index + 1))
        self.overall_eta = time.gmtime(overall_eta)
        self.app_ui.update_progress_bars(self.file_percent, self.file_elapsed_time, self.file_eta, self.overall_percent, self.overall_elapsed_time, self.overall_eta)

    # Function to search the input folder for parquet files
    def search_for_parquet_files(self):
        parquet_files = []
        for file in os.listdir(self.input_folder):
            if file.endswith(".parquet"):
                parquet_files.append(os.path.join(self.input_folder, file))
        return parquet_files

    def get_table_columns(self, input_file):
        table = pq.read_table(input_file)
        return table.column_names

    # Function to process the parquet files and update progress bar
    def process_parquet_files(self):
        parquet_files = self.search_for_parquet_files()
        self.overall_total_files = len(parquet_files)
        self.overall_start_time = time.time()

        for i, input_file in enumerate(parquet_files):
            if self.process_thread:
                if self.process_thread.is_stopping() or not self.process_thread.is_running():
                    self.check_thread_error()
                    break
            self.overall_file_index = i
            input_file_name = os.path.basename(input_file)
            output_file = os.path.join(self.output_folder, os.path.splitext(input_file_name)[0] + ".csv")

            # Allow the user to select which columns to export
            selected_columns = None
            while selected_columns is None:
                selected_columns = self.app_ui.select_columns(input_file_name, self.get_table_columns(input_file))
                if selected_columns is not None:
                    self.columns = selected_columns

            self.convert_parquet_file_to_csv(input_file, output_file, self.columns)
            self.update_progress()

        # The process has finished, so restore the UI to it's original state
        self.cancel_process()

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
            self.file_start_time = time.time()

            # Iterate over each range of rows
            for i in range(total_iterations):
                if self.process_thread:
                    if self.process_thread.is_stopping() or not self.process_thread.is_running():
                        self.check_thread_error()
                        break

                if start_row + row_steps > total_rows:
                    row_steps = total_rows - start_row
                # stop_row = min(start_row + row_steps, total_rows)

                # Perform a table query using DuckDB
                select_query = f"SELECT {', '.join(columns)} FROM parquet_table LIMIT {row_steps} OFFSET {start_row}"
                result = self.con.execute(select_query)
                rows = result.fetchall()

                # Loop over the range of specified rows
                for j in range(len(rows)):
                    if self.process_thread:
                        if self.process_thread.is_stopping() or not self.process_thread.is_running():
                            self.check_thread_error()
                            break

                    # Process the data for the current row as desired
                    row_data = rows[j]
                    writer.writerow(row_data)
                    rows_processed += 1

                # Calculate progress and print progress information
                self.file_percent = (rows_processed / total_rows) * 100
                elapsed_time = time.time() - self.file_start_time
                self.file_elapsed_time = time.gmtime(elapsed_time)
                eta = (elapsed_time / (i + 1)) * (total_iterations - i - 1)
                self.file_eta = time.gmtime(eta)
                self.update_progress()

                # Increment the row iteration variables
                start_row += row_steps
                if start_row > total_rows:
                    break

    def check_thread_error(self, error=None):
        if error or self.process_thread.has_exception():
            if error is not None:
                messagebox.showerror("Error", f"Exception in processing thread: {error}")
            else:
                messagebox.showerror("Error", f"Exception in processing thread: {self.process_thread.get_last_exception()}")

    def check_thread_done(self):
        if self.process_thread.is_done():
            messagebox.showinfo("Info", "The process has completed successfully")
            return True
        else:
            messagebox.showinfo("Info", "The process has been cancelled or interrupted")
        return False


app = App("Parquet Parley", "500x400")
