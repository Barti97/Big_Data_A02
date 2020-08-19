# --------------------------------------------------------
#
# PYTHON PROGRAM DEFINITION
#
# The knowledge a computer has of Python can be specified in 3 levels:
# (1) Prelude knowledge --> The computer has it by default.
# (2) Borrowed knowledge --> The computer gets this knowledge from 3rd party libraries defined by others
#                            (but imported by us in this program).
# (3) Generated knowledge --> The computer gets this knowledge from the new functions defined by us in this program.
#
# When launching in a terminal the command:
# user:~$ python3 this_file.py
# our computer first processes this PYTHON PROGRAM DEFINITION section of the file.
# On it, our computer enhances its Python knowledge from levels (2) and (3) with the imports and new functions
# defined in the program. However, it still does not execute anything.
#
# --------------------------------------------------------

# ------------------------------------------
# IMPORTS
# ------------------------------------------
import pyspark

import calendar
import datetime


def get_day_of_week(date):
    # 1. We create the output variable
    res = calendar.day_name[(datetime.datetime.strptime(date, "%d-%m-%Y")).weekday()]

    # 2. We return res
    return res

# ------------------------------------------
# FUNCTION process_line
# ------------------------------------------
def process_line(line):
    # 1. We create the output variable
    res = ()

    # 2. We remove the end of line character
    line = line.replace("\n", "")

    # 3. We split the line by tabulator characters
    params = line.split(";")

    # 4. We assign res
    if (len(params) == 7):
        res = tuple(params)

    # 5. We return res
    return res

# ------------------------------------------
# FUNCTION round_one
# ------------------------------------------
def round_one(input_data, station_name):
    
    total = 0
    
    resVAL = input_data.collect()
    
    for item in resVAL:
        total = total + 1
        
    return total


# ------------------------------------------
# FUNCTION round_two
# ------------------------------------------
def round_two(input_data, station_name, total):
    
    map2RDD = input_data.map(lambda item: (item[2] , 1))
    reducedRDD = map2RDD.reduceByKey(lambda lastNumber, nextNumber: lastNumber + nextNumber)
    sortedRDD = reducedRDD.sortBy(lambda item: item[1] * (-1))
    map3RDD = sortedRDD.map(lambda item: (item[0] , (item[1] , ((item[1]/ total)*100))))
    
    
    resVAL = map3RDD.collect()
    
    for item in resVAL:
        print(item)

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc, my_dataset_dir, station_name):
    
    inputRDD = sc.textFile(my_dataset_dir)
    allstationsRDD = inputRDD.map(process_line)
    mapRDD = allstationsRDD.map(lambda item: (int(item[0]), item[1], get_day_of_week(item[4].split(' ')[0]) +"_"+ item[4].split(' ')[1].split(':')[0], int(item[5])))
    filterRDD = mapRDD.filter(lambda item: item[0] == 0 and item[1] == station_name and item[3] == 0)
    
    totalRoundOne = round_one(filterRDD, station_name)
    round_two(filterRDD, station_name, totalRoundOne)
    
 
# --------------------------------------------------------
#
# PYTHON PROGRAM EXECUTION
#
# Once our computer has finished processing the PYTHON PROGRAM DEFINITION section its knowledge is set.
# Now its time to apply this knowledge.
#
# When launching in a terminal the command:
# user:~$ python3 this_file.py
# our computer finally processes this PYTHON PROGRAM EXECUTION section, which:
# (i) Specifies the function F to be executed.
# (ii) Define any input parameter such this function F has to be called with.
#
# --------------------------------------------------------
if __name__ == '__main__':
    # 1. We use as many input arguments as needed
    station_name = "Fitzgerald's Park"

    # 2. Local or Databricks
    local_False_databricks_True = False

    # 3. We set the path to my_dataset
    my_local_path = "/home/bartosz/Documents/Big Data & Analytics/A02"
    my_databricks_path = "/"

    my_dataset_dir = "/my_dataset/"
#    my_local_path = "/home/nacho/CIT/Tools/MyCode/Spark/"
#    my_databricks_path = "/"
#
#    my_dataset_dir = "FileStore/tables/3_Assignment/my_dataset/"

    if local_False_databricks_True == False:
        my_dataset_dir = my_local_path + my_dataset_dir
    else:
        my_dataset_dir = my_databricks_path + my_dataset_dir

    # 4. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We call to our main function
    my_main(sc, my_dataset_dir, station_name)
