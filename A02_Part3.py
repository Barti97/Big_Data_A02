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
import datetime

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
# FUNCTION get_ran_outs
# ------------------------------------------
def get_ran_outs(my_list, measurement_time):
    # 1. We create the output variable
    res = []
    # res = None

    # 2. We compute the auxiliary lists:
    # date_values (mapping dates to minutes passed from midnight)
    # indexes (indexes of actual ran-outs)
    date_values = []

    for item in my_list:
        date_values.append((int(item[0:2]) * 60) + int(item[3:5]))

    # 3. We get just the real ran-outs
    index = len(my_list) - 1
    measurements = 1

    # 3.1. We traverse the indexes
    while (index > 0):
        # 3.1.1. If it is inside a ran-out cycle, we increase the measurements
        if (date_values[index - 1] == (date_values[index] - measurement_time)):
            measurements = measurements + 1
        # 3.1.2. Otherwise, we append the actual ran-out and re-start the measurements
        else:
            res.insert(0, (my_list[index], measurements))
            measurements = 1

        # 3.1.3. We decrease the index
        index = index - 1

    # 3.2. We add the first position
    res.insert(0, (my_list[index], measurements))

    # 5. We return res
    return res

# ------------------------------------------
# FUNCTION checkByTime
# ------------------------------------------
def checkByTime(my_list, measurement_time):
    
    myList = list()
#    for i in data: 
    for item in my_list:
        date = item[0]
        time = item[1]
        time = datetime.datetime.strptime(time, "%H:%M:%S")
            
        result = date, time, 1
            
        if len(myList) == 0:
            myList.append(result)
            
        if myList[-1][1] == time[2]-datetime.timedelta(minutes=myList[-1][2]*measurement_time):
            myList[-1] = myList[-1][0], myList[-1][1], myList[-1][2] + 1
            
        myList.append(result)
     
#    for t in myList:
#        return (str(t[0]), str(t[1].strftime("%H:%M:%S")), str(t[2]))
    return myList



# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc, my_dataset_dir, station_name, measurement_time):
    
    inputRDD = sc.textFile(my_dataset_dir)
    allstationsRDD = inputRDD.map(process_line)
    mapRDD = allstationsRDD.map(lambda item: (int(item[0]), item[1], item[4].split(' ')[1], item[4].split(' ')[0].split("-")[2]+"-"+item[4].split(' ')[0].split("-")[1]+"-"+item[4].split(' ')[0].split("-")[0], int(item[5])))
    filterRDD = mapRDD.filter(lambda item: item[0] == 0 and item[1] == station_name and item[4] == 0)
    map2RDD = filterRDD.map(lambda item: (item[3], item[2]))
    
    groupedRDD = map2RDD.groupByKey().map(lambda item: (item[0], checkByTime(list(item[1]), measurement_time)))
    flatMapRDD = groupedRDD.flatMap(lambda item: [(item[0], value) for value in item[1]],False)

    sortedRDD = flatMapRDD.sortBy(lambda item: datetime.datetime.strptime(item[0], "%Y-%m-%d"))
    
    resVAL = sortedRDD.collect()
    
    for item in resVAL:
        print(item)

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
    # 1. We use as many input parameters as needed
    station_name = "Fitzgerald's Park"
    measurement_time = 5

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
    my_main(sc, my_dataset_dir, station_name, measurement_time)
