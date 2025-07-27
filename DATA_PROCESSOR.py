from ast import Module
import re
import os
from datetime import date


class DATA_PROCESSOR:
    def __init__(self ):
        self.data = []
        print("Initializing DATA_PROCESSOR...")
        current_dir = os.getcwd()
        print("current_dir:", current_dir)

        self.input_path = f"{current_dir}\data\input"
        self.processing_path =f"{current_dir}\data\processing"
        self.output_path = f"{current_dir}\data\output"

        self.list_of_files = []

    def get_files_to_process(self):
        # find the sources files that match the pattern
        # The pattern is: data_file_YYYYMMDDHHMMSS.csv
        # example of file name data_file_20210527182730.csv.
        files_to_load = []
        regex_pattern = r"data_file_\d{14}\.csv"

        # If you want to filter by current date, you can use the following:
        # current_day = date.today()
        # current_day_str = current_day.strftime("%Y%m%d")
        # regex_pattern = rf"data_file_{current_day_str}\d{{6}}\.csv"

        print(f"Looking for files in: {self.input_path}")


        for filename in os.listdir(self.input_path):
            print(f"Checking file: {filename}")

            if re.match(regex_pattern, filename):
                self.list_of_files.append(filename)
            else:
                print(f"File {filename} does not match the pattern. Moving to error folder.")
                 # Move the file to the error folder
                file_to_move = os.path.join(self.input_path, filename)
                os.rename(file_to_move, os.path.join(self.error_path, filename))



    def load_data_module_1(self):
        self.get_files_to_process()

        for file in self.list_of_files:
            # moved from input folder to processing folder
            file_to_move = os.path.join(self.input_path, file)

            if os.path.exists(file_to_move):
                # Check if the file is empty
                if not self.is_empty_file(file_to_move):
                    # Move the file to the processing folder
                    os.rename(file_to_move, os.path.join(self.processing_path, file))
                    print(f"Moved file: {file_to_move} to processing folder.")

                    file_to_process = os.path.join(self.processing_path, file)
                    print(f"Processing file: {file_to_process}")
                else:
                    print(f"File {file} is empty. Moving to error folder.")
                    # Move the file to the error folder
                    os.rename(file_to_move, os.path.join(self.error_path, file))


            

    # MODULE 1 :file_check_module
    def is_empty_file(self, file_name):
        # check if file is not empty
        return os.path.getsize(file_name) == 0
            
    def process_data(self):
        # Process the loaded data
        pass

    def save_data(self, destination):
        # Save the processed data to the specified destination
        pass

if __name__ == "__main__":
    processor = DATA_PROCESSOR()
    processor.load_data_module_1()
    #processor.process_data()
    #processor.save_data("processed_data.txt")



    


