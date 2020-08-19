
# ------------------------------------------
# FUNCTION my_map
# ------------------------------------------
def my_map(my_input_stream, my_output_stream, my_mapper_input_parameters):
    
    projects = list()
    
    for i in my_input_stream:
        data = process_line(i)
        
        project = str(data[0])
        name = str(data[1])
        language = str(data[2])
        views = int(data[3])
        
        title = project+"_"+language
        
        project.append(title, name, views)
            
    for p in projects:
        my_output_stream.write(str(p[0]) + "\t(" + str(p[1]) + ","+ p[2] + ")\n")

# ------------------------------------------
# FUNCTION my_reduce
# ------------------------------------------
def my_reduce(my_input_stream, my_output_stream, my_reducer_input_parameters):
    
    projects = {}
    
    for i in my_input_stream:
        data = get_key_value(i)
        
        title = str(data[0])
        name = str(data[1])
        views = int(data[2])
        
        if title in projects:
            if (views > projects[title][1]):
                projects[title] = name , views
        else:
            projects[title] = name , views
            
    for p in sorted(projects.items(), key=lambda item: item[1][1], reverse=True):
        my_output_stream.write(p[0] + "\t(" + p[1][0] + ","+ p[1][1] + ")\n")


# ------------------------------------------
# FUNCTION check_highest_number
# ------------------------------------------
def check_highest_number(lastNumber, nextNumber):
    
    if(nextNumber[1] > lastNumber[1]):
        res = nextNumber
    else:
        res= lastNumber
    
    return res

# ------------------------------------------
# FUNCTION my_spark_core_model
# ------------------------------------------
def my_spark_core_model(sc, my_dataset_dir):
    
    inputRDD = sc.textFile(my_dataset_dir)
    allprojectsRDD = inputRDD.map(process_line)
    mapRDD = allprojectsRDD.map(lambda item: (str(item[0])+"_"+str(item[2]),(str(item[1], int(item[3])))))
    reducedRDD = mapRDD.reduceByKey(check_highest_number)
    sortedRDD = reducedRDD.sortBy(lambda item: item[2] * (-1))
    
    resVAL = sortedRDD.collect()

    print(len(resVAL))
    for item in resVAL:
        print(item)

# ------------------------------------------
# FUNCTION my_spark_streaming_model
# ------------------------------------------
def my_spark_streaming_model(ssc, monitoring_dir):
    inputDStream = ssc.textFileStream(monitoring_dir)
    allprojectsDStream = inputDStream.map(process_line)
    
    mapDStream = allprojectsDStream.map(lambda item: (str(item[0])+"_"+str(item[2]),(str(item[1], int(item[3])))))
    reducedDStream = mapDStream.reduceByKey(check_highest_number)
    sortedDStream = reducedDStream.transform( lambda item: item.sortBy(lambda item: item[2], False) )
    sortedDStream.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
    sortedDStream.pprint()
