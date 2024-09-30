import pandas as pd
import warnings
import mysql.connector as msql
import csv
warnings.filterwarnings("ignore")

#Task1
#Loading the data
def read_data_from_csv():
    #df =read the 'iot_telemetry_data.csv' file
    df = pd.read_csv('iot_telemetry_data.csv')
    return df


#Task 2: Renaming the Columns
def rename_columns():
    df=read_data_from_csv()
    #rename the columns according to the description
    df = df.rename(columns={"ts":"timestamp", "co":"carbon_monoxide", "device":"device_id", "lpg":"liquefied_petroleum_gas", "temp": "temperature"})
    return df

#Task 3: check for null values
def null_values_check():
    df=rename_columns()
    null_values = df.isnull().sum()
    return null_values


#Task4 :Removing Duplicates

def remove_duplicates():
    df=rename_columns()
    df = df.drop_duplicates()
    return df


#Task 5:Handling Missing Values:
def handle_missing_values():
    df=remove_duplicates()
    df = df.dropna()
    return df

#Task 6:Data Type Conversion:

def convert_data_types():
    df= handle_missing_values()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df[["humidity", "light", "motion", "temperature"]] = df[["humidity", "light", "motion", "temperature"]].apply(pd.to_numeric)
    return df

#Task 7: Export the cleaned dataset to "cleaned_environemnt.csv"
def export_the_dataset():
    df=convert_data_types()
    df = df.to_csv('cleaned_environment.csv', index=False)
    return df


#TASK 8: Load the Cleaned dataset 'cleaned_environment.csv' to the database provided.

conn = msql.connect(host="localhost", database="be0980f0", user="be0980f0", password="Cab#22se")

cursor=conn.cursor()


cursor.execute('DROP TABLE IF EXISTS cleaned_environment;')
cursor.execute("CREATE TABLE cleaned_environment(timestamp date, device_id varchar(255), carbon_monoxide float, humidity float, light varchar(255), liquefied_petroleum_gas float, motion varchar(255), smoke float, temperature float)")

with open('cleaned_environment.csv', mode='r') as csv_file:
    #read csv using reader class
    csv_reader = csv.reader(csv_file)
    #skip header
    header = next(csv_reader)
    for row in csv_reader:
        cursor.execute('INSERT INTO cleaned_environment VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)', row)

    conn.commit()

cursor.close()
conn.close()
#check if mysql table is created using "cleaned_environment"