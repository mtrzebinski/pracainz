# Load PySpark engine module
import findspark
findspark.init()
from pyspark.sql import SparkSession

# Load modules to transform data and save to Excel file
import pandas as pd
import xlsxwriter

# Load module to create folder structure
import os


def new_york_districts_view(spark, path):
    """Function that prepares a view of New York districts from a CSV file

    Parameters:
    spark: SparkSession object created
    path: Path to the location of the file

    Returns: DataFrame object
    """

    # Load CSV file with New York districts
    new_york_districts = spark.read \
        .options(header="true", interSchema="true") \
        .csv("{}/{}".format(path, "taxi+_zone_lookup.csv"))

    # Create temporary SQL view from file with New York districts
    new_york_districts.createOrReplaceTempView("new_york_districts")

    # Query that cleans data from unnecessary content
    query_new_york_districts = spark.sql("SELECT LocationID,\
                                        CONCAT(Borough, ' ', Zone) AS location\
                                        FROM new_york_districts")

    return query_new_york_districts


def querys(spark, new_york_districts, dataframe, file_name):
    """Function that returns DataFrames from queries

    Parameters:
    spark: SparkSession object created
    new_york_districts: DataFrames returned from the new_york_districts_view function
    dataframe: DataFrame with original Excel file
    file_name: The name of the original Excel file

    Retruns: DataFrames from queries
    """

    # Dictionary with the names of the months
    months = {"01": "Janauary", "02": "February", "03": "March",
              "04": "April", "05": "May", "06": "June", "07": "July",
              "08": "August", "09": "September", "10": "October",
              "11": "November", "12": "December"}

    # Set of steps for cutting elements from a filename like year or month
    list_from_file = file_name.split("_")
    month = months[list_from_file[-1][5:7]]

    # Create temporary SQL view from file with original data
    dataframe.createOrReplaceTempView("taxi_data")

    # Creating several SQL temporary views to get DataFrame with matched number of taxis in zones
    new_york_districts.createOrReplaceTempView("new_york_districts")

    temporary_query1 = spark.sql("SELECT PULocationID, COUNT(PULocationID) as amount_of_start_position\
                                FROM taxi_data GROUP BY PULocationID")

    temporary_query2 = spark.sql("SELECT DOLocationID, COUNT(DOLocationID) as amount_of_end_position\
                                    FROM taxi_data GROUP BY DOLocationID")

    temporary_query1.createOrReplaceTempView("temp1")
    temporary_query2.createOrReplaceTempView("temp2")

    temporary_query3 = spark.sql("SELECT temp1.PULocationID AS LocationID, temp1.amount_of_start_position,\
                                 temp2.amount_of_end_position from temp1\
                                 FULL OUTER JOIN temp2 ON temp1.PULocationID = temp2.DOLocationID")

    temporary_query3.createOrReplaceTempView("temp3")

    # Correct DataFrame with taxis in New York zones
    new_york_districts_correct = spark.sql("SELECT {} AS month, CONCAT('New York ',new_york_districts.location) AS new_york_districts, temp3.amount_of_start_position,\
                              temp3.amount_of_end_position\
                              FROM new_york_districts INNER JOIN temp3 ON new_york_districts.LocationID = temp3.LocationID\
                              WHERE new_york_districts.location NOT IN ('Unknown NV', 'Unknown NA')".format("'" + month + "'"))

    # DataFrame with the most detailed information
    main_information = spark.sql("SELECT {} AS month, SUBSTRING(tpep_pickup_datetime, 1, 10) AS date,\
                ROUND(SUM(passenger_count), 2) AS passenger_count, ROUND(AVG(passenger_count), 2) AS passenger_mean,\
                ROUND(SUM(trip_distance), 2) AS trip_distance, ROUND(AVG(trip_distance), 2) AS trip_distance_mean,\
                ROUND(SUM(tip_amount), 2) AS tip_amount, ROUND(AVG(tip_amount), 2) AS tip_amount_mean,\
                ROUND(SUM(total_amount), 2) AS total_amount, ROUND(AVG(total_amount), 2) AS total_amount_mean\
                from taxi_data GROUP BY SUBSTRING(tpep_pickup_datetime, 1, 10)".format("'" + month + "'"))

    # DataFrame with payment type
    payment_type = spark.sql("SELECT {} AS month, CASE payment_type\
                             WHEN 1 THEN 'Credit card'\
                             WHEN 2 THEN 'Cash'\
                             WHEN 3 THEN 'No charge'\
                             WHEN 4 THEN 'Dispute'\
                             WHEN 5 THEN 'Unknown'\
                             WHEN 6 THEN 'Voided trip'\
                             END AS payment_type, count(payment_type) AS payment_type\
                             FROM taxi_data GROUP BY payment_type\
                             HAVING payment_type IS NOT NULL".format("'" + month + "'"))

    return file_name, new_york_districts_correct, payment_type, main_information


def save_as_excel(file_name, dataframe1, dataframe2, dataframe3):
    """Function that saves DataFrames as Excel file

    Parameters:
    file_name: The name of the original Excel file
    dataframe1, dataframe2, dataframe3: DataFrames returned from the queries function

    Retruns: None
    """

    # Dictionary with the names of the months
    months = {"01": "Janauary", "02": "February", "03": "March",
              "04": "April", "05": "May", "06": "June", "07": "July",
              "08": "August", "09": "September", "10": "October",
              "11": "November", "12": "December"}

    # Set of steps for cutting elements from a filename like year or month
    list_from_file = file_name.split("_")

    year = list_from_file[-1][0:4]
    month = months[list_from_file[-1][5:7]]
    name = list_from_file[0]

    # Creating folder structure and Excel file with cleaned data
    sheet1 = dataframe1.toPandas()
    sheet2 = dataframe2.toPandas()
    sheet3 = dataframe3.toPandas()

    # Files divided on years and months with the same structure
    if not os.path.exists("ETLData/{}/{}".format(year, month)):
        os.makedirs("ETLData/{}/{}".format(year, month))

    # Files divided on months in one folder
    if not os.path.exists("ETLData/General"):
        os.makedirs("ETLData/General")

    writer = pd.ExcelWriter("ETLData/{}/{}/{}.xlsx".format(year, month, name+"_"+month), engine="xlsxwriter")
    sheet1.to_excel(writer, sheet_name="new_york_districts_correct")
    sheet2.to_excel(writer, sheet_name="payment_type")
    sheet3.to_excel(writer, sheet_name="main_information")

    writer.save()

    writer = pd.ExcelWriter("ETLData/General/{}.xlsx".format(year + "_" + month), engine="xlsxwriter")
    sheet1.to_excel(writer, sheet_name="new_york_districts_correct")
    sheet2.to_excel(writer, sheet_name="payment_type")
    sheet3.to_excel(writer, sheet_name="main_information")

    writer.save()


def main():
    # SparkSession object created
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("TaxisData") \
        .getOrCreate()

    # List of files to be cleared
    for file in os.listdir("BlobStorage"):

        # Skip file with New York districts
        if file == "taxi+_zone_lookup.csv":
            continue

        # DataFrame with original Excel file
        dataframe = spark.read \
            .options(header="true", interSchema="true") \
            .csv("BlobStorage/{}".format(file))

        # Run ETL process and save results into new Excel file
        save_as_excel(*querys(spark, new_york_districts_view(spark, "BlobStorage"), dataframe, file))

    # Creating one general Excel file to create dashboard
    one_excel_file_sheet1 = pd.DataFrame()
    one_excel_file_sheet2 = pd.DataFrame()
    one_excel_file_sheet3 = pd.DataFrame()

    for file in os.listdir("ETLData/General"):
        file = "ETLData/General/{}".format(file)
        one_excel_file_sheet1 = one_excel_file_sheet1.append(pd.read_excel(file, engine="openpyxl", sheet_name="new_york_districts_correct"), ignore_index=True)
        one_excel_file_sheet2 = one_excel_file_sheet2.append(pd.read_excel(file, engine="openpyxl", sheet_name="payment_type"), ignore_index=True)
        one_excel_file_sheet3 = one_excel_file_sheet3.append(pd.read_excel(file, engine="openpyxl", sheet_name="main_information"), ignore_index=True)

    # Delete duplicate column
    one_excel_file_sheet1.drop(columns=["Unnamed: 0"])
    one_excel_file_sheet2.drop(columns=["Unnamed: 0"])
    one_excel_file_sheet3.drop(columns=["Unnamed: 0"])

    writer = pd.ExcelWriter("ETLData/General/GeneralFile.xlsx", engine="xlsxwriter")
    one_excel_file_sheet1.to_excel(writer, sheet_name="new_york_districts_correct", index=False)
    one_excel_file_sheet2.to_excel(writer, sheet_name="payment_type", index=False)
    one_excel_file_sheet3.to_excel(writer, sheet_name="main_information", index=False)

    writer.save()

main()
