from ast import Module
import re
import os
from datetime import date
import pandas as pd

class DATA_PROCESSOR:
    def __init__(self ):
        self.data = []
        print("Initializing DATA_PROCESSOR...")
        current_dir = os.getcwd()
        print("current_dir:", current_dir)

        self.input_path = f"{current_dir}\data\input"
        self.processing_path =f"{current_dir}\data\processing"
        self.error_path = f"{current_dir}\data\error"
        self.success_path = f"{current_dir}\data\success"
        self.processed_path = f"{current_dir}\data\processed"

        self.list_of_files = []

        self.files_to_be_processed = []

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
                    self.files_to_be_processed.append(file_to_process)

                else:
                    print(f"File {file} is empty. Moving to error folder.")
                    # Move the file to the error folder
                    os.rename(file_to_move, os.path.join(self.error_path, file))


            

    # MODULE 1 :file_check_module
    def is_empty_file(self, file_name):
        # check if file is not empty
        return os.path.getsize(file_name) == 0
            
    def data_quality_check_module_2(self ):
        
        for file_name in self.files_to_be_processed:
            # read the file into a pandas dataframe with header row
            print(f"Loading data from {file_name}...")
            df = pd.read_csv(file_name, header=0, encoding='utf-8')
            file_name_only = os.path.basename(file_name)
            print(f"Data loaded from {file_name_only}. Number of rows: {len(df)}")

            # print the first 5 rows of the dataframe
            print("First 5 rows of the dataframe:")
            print(df['phone'].head())


            # cast phone to string all the phone numbers
            df['phone'] = df['phone'].astype(str)
            
            # spliting into two columns contact_number_1 and contact_number_2
            # splited by \n
            df[['contact_number_1', 'contact_number_2']] = df['phone'].str.split('\n', n=1, expand=True)
            

            # remove "+" and spaces , \r and \n from the phone number
            df['contact_number_1'] = df['contact_number_1'].str.replace(r"[+ ]", "", regex=True)
            df['contact_number_1'] = df['contact_number_1'].str.replace(r"[\r\n]", "", regex=True)
            df['contact_number_2'] = df['contact_number_2'].str.replace(r"[+ ]", "", regex=True)
            df['contact_number_2'] = df['contact_number_2'].str.replace(r"[\r\n]", "", regex=True)

            # validate phone numbers
            df['phone_valid_1'] = df['contact_number_1'].apply(self.check_phone_file)
            df['phone_valid_2'] = df['contact_number_2'].apply(self.check_phone_file)

            #filter out invalid phone numbers
            valid_df_0 = df[(df['phone_valid_1'] == True) & (df['phone_valid_2'] == True)]
            print(f"Number of valid phone numbers: {len(valid_df_0)}")

            #filter out invalid phone numbers
            valid_df_1 = df[(df['phone_valid_1'] == True) & (df['phone_valid_2'] == False)]
            print(f"Number of valid phone numbers: {len(valid_df_1)}")
            # filter out invalid phone numbers for second phone number

            #filter out invalid phone numbers
            valid_df_2 = df[(df['phone_valid_1'] == False) & (df['phone_valid_2'] == True)]
            print(f"Number of valid phone numbers: {len(valid_df_1)}")
            # filter out invalid phone numbers for second phone number

            # join valid_df_0, valid_df_1 and valid_df_2
            valid_df = pd.concat([valid_df_0, valid_df_1, valid_df_2], ignore_index=True)

            # filter out invalid phone numbers for first phone number
            invalid_df_phone = df[(df['phone_valid_1'] == False) & (df['phone_valid_2'] == False)]
            print(f"Number of invalid phone numbers: {len(invalid_df_phone)}")

            # if phone number one or phone number two (or both) is ok I will keep the record
            # save invalid data to error folder 
            output_file = os.path.join(self.error_path, f"{file_name_only[0:-4]}_invalid_data_phone_number_{date.today()}.bad")
            invalid_df_phone.to_csv(output_file, index=False)
            print(f"Invalid data saved to {output_file}")

            # validate that the required fields are not null
            required_fields = ['name', 'contact_number_1', 'location']
            non_null_df = valid_df.dropna(subset=required_fields)
            print(f"Registros con todos los campos requeridos no nulos: {len(non_null_df)}")
            # print registros con todos los campos requeridos no nulos
            print(non_null_df.head())

            # get records where at least one of the required fields is null or empty or ""
            null_df = valid_df[valid_df[required_fields].isnull().any(axis=1) | (valid_df[required_fields] == "").any(axis=1)]
            print(f"Registros con al menos un campo requerido nulo: {len(null_df)}")
            # print registros con al menos un campo requerido nulo
            print(null_df.head())

            # save records with at least one required field null to error folder
            null_output_file = os.path.join(self.error_path, f"{file_name_only[0:-4]}_null_data_{date.today()}.bad")
            null_df.to_csv(null_output_file, index=False)
            print(f"Records with at least one required field null saved to {null_output_file}")
            

            # save valid data to success folder
            valid_output_file = os.path.join(self.success_path, f"{file_name_only[0:-4]}.out")
            valid_df.to_csv(valid_output_file, index=False)
            print(f"Valid data saved to {valid_output_file} total records: {len(valid_df)}")

            #moving file from processing folder to processed folder
            processing_file = os.path.join(self.processing_path, file_name_only)
            processed_file = os.path.join(self.processed_path, file_name_only)
            os.rename(processing_file, processed_file)

    def check_phone_file(self, phone: None):      
        response = False
        # Check if the file contains phone numbers in the correct format
        regex = r"^\d{10,15}$"

        if phone is None or phone == "":
            return False
        else:
            response = re.match(regex, phone) is not None
            if not response:
                print(f"Invalid phone number format: {phone}. Expected format: 10 to 15 digits.")
            return response


if __name__ == "__main__":
    processor = DATA_PROCESSOR()
    processor.load_data_module_1()
    processor.data_quality_check_module_2()
    #processor.save_data("processed_data.txt")



    


