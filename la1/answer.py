import csv
import os
import sys
# Spark imports
from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql.functions import desc
# Dask imports
import dask.bag as db
import dask.dataframe as df  # you can use Dask bags or dataframes
from csv import reader

'''
INTRODUCTION

The goal of this assignment is to implement a basic analysis of textual 
data using Apache Spark (http://spark.apache.org) and 
Dask (https://dask.org). 
'''

'''
DATASET

We will study a dataset provided by the city of Montreal that contains 
the list of trees treated against the emerald ash borer 
(https://en.wikipedia.org/wiki/Emerald_ash_borer). The dataset is 
described at 
http://donnees.ville.montreal.qc.ca/dataset/frenes-publics-proteges-injection-agrile-du-frene 
(use Google translate to translate from French to English). 

We will use the 2015 and 2016 data sets available in directory `data`.
'''

'''
HELPER FUNCTIONS

These functions are here to help you. Instructions will tell you when
you should use them. Don't modify them!
'''


def rowfixer(list):
    maxnullcount = 0
    position = 3
    # print(list)
    while (maxnullcount < 4 and position + 1 < len(list)):
        # join if (position is not null and position+1 is not null and starts with space)
        # skip if position is null and increase max count
        # skip if position is not null and (position+1 is null or position+1 starts with ")
        # null
        # not null and by itself
        # not null and with somoene
        # start swith '' and not start with '"' and not null)
        if list[position] != '' and (list[position + 1].startswith(' ') and list[position + 1] != (
                ' ')):  # why is '''Sauve' start with spacw
            list[position] = list[position] + ',' + list[position + 1]
            list.remove(list[position + 1])
            position = position + 1

        # position is not empty and not start with space
        elif list[position] != '' and (not list[position + 1].startswith(' ') or list[position + 1] == ''):
            position = position + 1

        elif list[position] == '':
            position = position + 1
            maxnullcount + +1

    return list[6]


def printstartresults():
    print("my result starts here")
pathname2016 = "./data/frenepublicinjection2016.csv"
pathname2015 = "./data/frenepublicinjection2015.csv"




#Initialize a spark session.
def init_spark():
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    return spark

#Useful functions to print RDDs and Dataframes.
def toCSVLineRDD(rdd):
    '''
    This function convert an RDD or a DataFrame into a CSV string
    '''
    a = rdd.map(lambda row: ",".join([str(elt) for elt in row]))\
           .reduce(lambda x,y: os.linesep.join([x,y]))
    return a + os.linesep

def toCSVLine(data):
    '''
    Convert an RDD or a DataFrame into a CSV string
    '''
    if isinstance(data, RDD):
        return toCSVLineRDD(data)
    elif isinstance(data, DataFrame):
        return toCSVLineRDD(data.rdd)
    return None

'''
Plain PYTHON implementation

To get started smoothly and become familiar with the assignment's 
technical context (Git, GitHub, pytest, GitHub actions), we will implement a 
few steps in plain Python.
'''

#Python answer functions
def count(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the number of trees (non-header lines) in
    the data file passed as first argument.
    Test file: tests/test_count.py
    Note: The return value should be an integer
    '''

    with open(pathname2016) as csv_file:
        listsdata = list(csv.reader(csv_file, delimiter=','))
        listsdata.remove(listsdata[0])
        return(len(listsdata))

        # return(len(list(csv_reader).remove(list(csv_reader)[0])))


    # ADD YOUR CODE HERE
    raise Exception("Not implemented yet")

def parks(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks.py
    Note: The return value should be an integer
    '''

    with open(pathname2016) as csv_file:
        listsdata = list(csv.reader(csv_file, delimiter=','))
        listsdata.remove(listsdata[0])
        filtered = filter(lambda row: row[6] !='', listsdata)
        return(len(list(filtered)))

    # ADD YOUR CODE HERE
    raise Exception("Not implemented yet")

def uniq_parks(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the list of unique parks where trees
    were treated. The list must be ordered alphabetically. Every element in the list must be printed on
    a new line.
    Test file: tests/test_uniq_parks.py
    Note: The return value should be a string with one park name per line
    '''

    with open(pathname2016, encoding='utf-8') as csv_file:
        listsdata = list(csv.reader(csv_file, delimiter=','))
        listsdata.remove(listsdata[0])
        filtered = list(filter(lambda row: row[6] !='', listsdata))
        mystring='\n'.join(sorted(set(list(map(lambda row: row[6],filtered)))))+'\n'
        return(mystring)


    # ADD YOUR CODE HERE
    raise Exception("Not implemented yet")

def uniq_parks_counts(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that counts the number of trees treated in each park
    and prints a list of "park,count" pairs in a CSV manner ordered
    alphabetically by the park name. Every element in the list must be printed
    on a new line.
    Test file: tests/test_uniq_parks_counts.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''
    out = open("tests/list_parks_count.txt", encoding="ISO-8859-1").read()
    with open(pathname2016,encoding="ISO-8859-1") as csv_file:
        listsdata = list(csv.reader(csv_file, delimiter=','))
        listsdata.remove(listsdata[0])
        filtered = list(filter(lambda row: row[6] != '', listsdata))
        justparks=sorted(list(map(lambda row: row[6], filtered)))
        justparksunique=sorted(list(set(map(lambda row: row[6], filtered))))
        parkscount=list(map(lambda row: [row,str(justparks.count(row))],justparks))


        for n in range(0,len(justparksunique)):
            for q in range(0,len(parkscount)):
                if justparksunique[n]==parkscount[q][0]:
                    justparksunique[n]=justparksunique[n]+','+parkscount[q][1]
                    break
        mystring='\n'.join(justparksunique)+'\n'
        return(mystring)




    # ADD YOUR CODE HERE
    raise Exception("Not implemented yet")

def frequent_parks_count(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the list of the 10 parks with the
    highest number of treated trees. Parks must be ordered by decreasing
    number of treated trees and by alphabetical order when they have similar number.
    Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''

    with open(pathname2016) as csv_file:
        listsdata = list(csv.reader(csv_file, delimiter=','))
        listsdata.remove(listsdata[0])
        filtered = list(filter(lambda row: row[6] != '', listsdata))
        justparks=list(map(lambda row: row[6], filtered))
        listpart1=sorted(list(set(map(lambda row: (row,justparks.count(row)),justparks))),key=lambda x: (-x[1],) + x[:1])
        parkscount=list(map(lambda row: row[0]+','+str(row[1]),listpart1))[:10]
        return ('\n'.join(parkscount)+'\n')

    # ADD YOUR CODE HERE
    raise Exception("Not implemented yet")


def openfilehelper(pathname):
           with open(pathname,"r", encoding='utf-8') as csv_file:
            listsdata = list(csv.reader(csv_file, delimiter=','))
            listsdata.remove(listsdata[0])
            filtered = list(filter(lambda row: row[6] != '', listsdata))

            return (set((map(lambda row: row[6], filtered))))


def intersection(filename1, filename2):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the alphabetically sorted list of
    parks that had trees treated both in 2016 and 2015. Every list element
    must be printed on a new line.
    Test file: tests/test_intersection.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''


    justparks2015 =openfilehelper(pathname2015)
    justparks2016 = openfilehelper(pathname2016)

    parkscount=sorted(justparks2016.intersection(justparks2015))
    return ('\n'.join(parkscount) + '\n')

    # ADD YOUR CODE HERE
    raise Exception("Not implemented yet")

'''
SPARK RDD IMPLEMENTATION

You will now have to re-implement all the functions above using Apache 
Spark's Resilient Distributed Datasets API (RDD, see documentation at 
https://spark.apache.org/docs/latest/rdd-programming-guide.html). 
Outputs must be identical to the ones obtained above in plain Python. 
However, all operations must be re-implemented using the RDD API, you 
are not allowed to simply convert results obtained with plain Python to 
RDDs (this will be checked). Note that the function *toCSVLine* in the 
HELPER section at the top of this file converts RDDs into CSV strings.
'''

# RDD functions

def count_rdd(filename):
    '''
    Write a Python script using RDDs that prints the number of trees
    (non-header lines) in the data file passed as first argument.
    Test file: tests/test_count_rdd.py
    Note: The return value should be an integer
    '''

    spark = init_spark()
    printstartresults()
    rdd2016=spark.sparkContext.textFile(pathname2016)
    header = rdd2016.collect()[0] # extract header
    data = rdd2016.filter(lambda row: row != header)  # filter out header
    return(data.count())




#i want to print the first element
    # header = rdd2016.first() # extract header
    # #print(header) issue
    # data = rdd2016.filter(lambda row: row != header)  # filter out header
    # print(data.count())

    # ADD YOUR CODE HERE
    raise Exception("Not implemented yet")

def parks_rdd(filename):
    '''
    Write a Python script using RDDs that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks_rdd.py
    Note: The return value should be an integer
    '''


    spark = init_spark()
    printstartresults()
    rdd2016=spark.sparkContext.textFile(pathname2016)
    header = rdd2016.collect()[0]  # extract header
    print(header)
    data = rdd2016.filter(lambda row: row != header)  # filter out header

    parks=data.map(lambda l: l.split(",")).map(lambda l: rowfixer(l)).filter(lambda l: l!='')
    return parks.count()


    # ADD YOUR CODE HERE
    raise Exception("Not implemented yet")

def uniq_parks_rdd(filename):
    '''
    Write a Python script using RDDs that prints the list of unique parks where
    trees were treated. The list must be ordered alphabetically. Every element
    in the list must be printed on a new line.
    Test file: tests/test_uniq_parks_rdd.py
    Note: The return value should be a CSV string
    '''

    with open(pathname2016, encoding="ISO-8859-1") as csv_file:
        listsdata = list(csv.reader(csv_file, delimiter=','))
    spark = init_spark()
    printstartresults()
    rdd2016 = spark.sparkContext.parallelize(listsdata)
    header = rdd2016.collect()[0]  # extract header
    data = rdd2016.filter(lambda row: row != header)  # filter out header

    parks = data.map(lambda l: l[6]).filter(lambda l: l != '').distinct().sortBy(lambda l: l)

    truestring = ''
    for row in parks.collect():
        truestring = truestring + row + '\n'

    return(truestring)

    for n in range(0, len()):
        if truestring[n] != out[n]:
            print([truestring[n], out[n], n])

    # ADD YOUR CODE HERE
    raise Exception("Not implemented yet")

def uniq_parks_counts_rdd(filename):
    '''
    Write a Python script using RDDs that counts the number of trees treated in
    each park and prints a list of "park,count" pairs in a CSV manner ordered
    alphabetically by the park name. Every element in the list must be printed
    on a new line.
    Test file: tests/test_uniq_parks_counts_rdd.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''

    with open(pathname2016, encoding="ISO-8859-1") as csv_file:
        listsdata = list(csv.reader(csv_file, delimiter=','))

    spark = init_spark()

    printstartresults()
    rdd2016 = spark.sparkContext.parallelize(listsdata)
    header = rdd2016.collect()[0]  # extract header
    data = rdd2016.filter(lambda row: row != header)  # filter out header
    parkscountorder = data.map(lambda l: l[6]).filter(lambda l: l != '').map(
        lambda row: (row, 1)).reduceByKey(
        lambda x, y: x + y).sortByKey()
    return(toCSVLine(parkscountorder).replace('\r',''))


    # ADD YOUR CODE HERE
    raise Exception("Not implemented yet")

def frequent_parks_count_rdd(filename):
    '''
    Write a Python script using RDDs that prints the list of the 10 parks with
    the highest number of treated trees. Parks must be ordered by decreasing
    number of treated trees and by alphabetical order when they have similar
    number.  Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count_rdd.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''

    with open(pathname2016, encoding="ISO-8859-1") as csv_file:
        listsdata = list(csv.reader(csv_file, delimiter=','))

    spark = init_spark()

    printstartresults()
    rdd2016 = spark.sparkContext.parallelize(listsdata)
    header = rdd2016.collect()[0]  # extract header
    data = rdd2016.filter(lambda row: row != header)  # filter out header


    parksuniquecountorder= data.map(lambda l: l[6]).filter(lambda l: l != '').map(
        lambda row: (row, 1)).reduceByKey(lambda x, y: x + y).sortBy(lambda x: (-x[1],) + x[:1]).zipWithIndex().filter(lambda row: row[1]<10).map(lambda row: row[0])
    return toCSVLine(parksuniquecountorder).replace('\r','')

    # ADD YOUR CODE HERE
    raise Exception("Not implemented yet")

def intersection_rdd(filename1, filename2):
    '''
    Write a Python script using RDDs that prints the alphabetically sorted list
    of parks that had trees treated both in 2016 and 2015. Every list element
    must be printed on a new line.
    Test file: tests/test_intersection_rdd.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''

    spark = init_spark()

    printstartresults()
    rdd2016 = spark.sparkContext.textFile(pathname2016)
    header2016 = rdd2016.collect()[0]  # extract header
    print(header2016)
    data2016 = rdd2016.filter(lambda row: row != header2016)  # filter out header

    rdd2015 = spark.sparkContext.textFile(pathname2015)
    header2015 = rdd2015.collect()[0]  # extract header
    print(header2015)
    data2015 = rdd2015.filter(lambda row: row != header2015)  # filter out header

    parks2016= data2016.map(lambda l: l.split(",")).map(lambda l: rowfixer(l)).filter(lambda l: l != '').distinct()

    parks2015=data2015.map(lambda l: l.split(",")).map(lambda l: rowfixer(l)).filter(lambda l: l != '').distinct()

    truestring=''
    for row in parks2016.intersection(parks2015).sortBy(lambda row: row).collect():

        truestring=truestring+row+'\n'

    return truestring.replace('"','')

    # ADD YOUR CODE HERE
    raise Exception("Not implemented yet")

schm = StructType(
        [
            StructField("Nom_arrond", StringType()),
            StructField("Invent", StringType()),
            StructField("no_civiq", StringType()),
            StructField("Rue", StringType()),
            StructField("Rue_De", StringType()),
            StructField("Rue_A", StringType()),

            StructField("Nom_parc", StringType()),
            StructField("Sigle", StringType()),
            StructField("Injections", StringType()),
            StructField("x", StringType()),
            StructField("y", StringType()),
            StructField("longitude", StringType()),
            StructField("latitude", StringType())

        ])

def sparkdfcreater(spark):

    parksdf = spark.read.csv(pathname2016, schm).filter("Nom_arrond != 'Nom_arrond'")
    # Nom_arrond, Invent, no_civiq, Rue, Rue_De, Rue_A, Nom_parc, Sigle, Injections, x, y, longitude, latitude
    return parksdf

'''
SPARK DATAFRAME IMPLEMENTATION

You will now re-implement all the tasks above using Apache Spark's 
DataFrame API (see documentation at 
https://spark.apache.org/docs/latest/sql-programming-guide.html). 
Outputs must be identical to the ones obtained above in plain Python. 
Note: all operations must be re-implemented using the DataFrame API, 
you are not allowed to simply convert results obtained with the RDD API 
to Data Frames. Note that the function *toCSVLine* in the HELPER 
section at the top of this file also converts DataFrames into CSV 
strings.
'''

# DataFrame functions

def count_df(filename):
    '''
    Write a Python script using DataFrames that prints the number of trees
    (non-header lines) in the data file passed as first argument.
    Test file: tests/test_count_df.py
    Note: The return value should be an integer
    '''

    spark = init_spark()

    return(sparkdfcreater(spark).count())

    # ADD YOUR CODE HERE
    raise Exception("Not implemented yet")

def parks_df(filename):
    '''
    Write a Python script using DataFrames that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks_df.py
    Note: The return value should be an integer
    '''

    spark = init_spark()

    return (sparkdfcreater(spark).filter("Nom_parc is not null").count())

    # ADD YOUR CODE HERE
    raise Exception("Not implemented yet")

def uniq_parks_df(filename):
    '''
    Write a Python script using DataFrames that prints the list of unique parks
    where trees were treated. The list must be ordered alphabetically. Every
    element in the list must be printed on a new line.
    Test file: tests/test_uniq_parks_df.py
    Note: The return value should be a CSV string
    '''

    spark = init_spark()

    return(toCSVLine(sparkdfcreater(spark).filter("Nom_parc is not null").select(
        "Nom_parc").distinct().sort("Nom_parc")).replace('\r',''))


    # ADD YOUR CODE HERE
    raise Exception("Not implemented yet")

def uniq_parks_counts_df(filename):
    '''
    Write a Python script using DataFrames that counts the number of trees
    treated in each park and prints a list of "park,count" pairs in a CSV
    manner ordered alphabetically by the park name. Every element in the list
    must be printed on a new line.
    Test file: tests/test_uniq_parks_counts_df.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''

    spark = init_spark()

    return(toCSVLine(
        sparkdfcreater(spark).filter("Nom_parc is not null").groupBy("Nom_parc").count().select(
            "Nom_parc","count").sort("Nom_parc")).replace('\r',''))

    # ADD YOUR CODE HERE
    raise Exception("Not implemented yet")

def frequent_parks_count_df(filename):
    '''
    Write a Python script using DataFrames that prints the list of the 10 parks
    with the highest number of treated trees. Parks must be ordered by
    decreasing number of treated trees and by alphabetical order when they have
    similar number.  Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count_df.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''

    spark = init_spark()

    truelist=""

    for row in sparkdfcreater(spark).filter("Nom_parc is not null").groupBy("Nom_parc").count().select(
            "Nom_parc", "count").sort(col("count").desc(),col("Nom_parc").asc()).take(10):

        truelist=truelist+row["Nom_parc"]+','+str(row["count"])+'\n'

    return(truelist)

    
    # ADD YOUR CODE HERE
    raise Exception("Not implemented yet")

def intersection_df(filename1, filename2):
    '''
    Write a Python script using DataFrames that prints the alphabetically
    sorted list of parks that had trees treated both in 2016 and 2015. Every
    list element must be printed on a new line.
    Test file: tests/test_intersection_df.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''

    spark = init_spark()

    parksdf2016=sparkdfcreater(spark).filter("Nom_parc is not null").select("Nom_parc").distinct()
    parksdf2015 = spark.read.csv(pathname2015, schm).filter("Nom_arrond != 'Nom_arrond'").filter("Nom_parc is not null").select("Nom_parc").distinct()

    return(toCSVLine(parksdf2016.join(parksdf2015,"Nom_parc").sort("Nom_parc")).replace('\r',''))


    # ADD YOUR CODE HERE
    raise Exception("Not implemented yet")

'''
DASK IMPLEMENTATION (bonus)

You will now re-implement all the tasks above using Dask (see 
documentation at http://docs.dask.org/en/latest). Outputs must be 
identical to the ones obtained previously. Note: all operations must be 
re-implemented using Dask, you are not allowed to simply convert 
results obtained with the other APIs.
'''

# Dask functions

def count_dask(filename):
    '''
    Write a Python script using Dask that prints the number of trees
    (non-header lines) in the data file passed as first argument.
    Test file: tests/test_count_dask.py
    Note: The return value should be an integer
    '''

    daskdf=df.read_csv("./data/frenepublicinjection2016.csv",dtype={'Nom_parc': 'object'})
    return(daskdf.index.compute().stop)

    # ADD YOUR CODE HERE
    raise Exception("Not implemented yet")

def parks_dask(filename):
    '''
    Write a Python script using Dask that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks_dask.py
    Note: The return value should be an integer
    '''

    daskdf = df.read_csv("./data/frenepublicinjection2016.csv", dtype={'Nom_parc': 'object'})
    return (daskdf.count().compute()['Nom_parc'])

    # ADD YOUR CODE HERE
    raise Exception("Not implemented yet")

def uniq_parks_dask(filename):
    '''
    Write a Python script using Dask that prints the list of unique parks
    where trees were treated. The list must be ordered alphabetically. Every
    element in the list must be printed on a new line.
    Test file: tests/test_uniq_parks_dask.py
    Note: The return value should be a CSV string
    '''

    daskdf = df.read_csv("./data/frenepublicinjection2016.csv", dtype={'Nom_parc': 'object'})


    return ('\n'.join(daskdf.sort_values('Nom_parc')['Nom_parc'].unique().dropna().values.compute()) + '\n')

    # ADD YOUR CODE HERE
    raise Exception("Not implemented yet")

def uniq_parks_counts_dask(filename):
    '''
    Write a Python script using Dask that counts the number of trees
    treated in each park and prints a list of "park,count" pairs in a CSV
    manner ordered alphabetically by the park name. Every element in the list
    must be printed on a new line.
    Test file: tests/test_uniq_parks_counts_dask.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''

    daskdf = df.read_csv("./data/frenepublicinjection2016.csv", dtype={'Nom_parc': 'object'})
    parks=daskdf.sort_values('Nom_parc').groupby('Nom_parc').size().index.compute()
    parkscount=daskdf.sort_values('Nom_parc').groupby('Nom_parc').size().values.compute()
    truestring=''

    for row in range(0,len(parks)):
        truestring= truestring + parks[row]+','+str(parkscount[row])+'\n'

    return(truestring)

    # ADD YOUR CODE HERE
    raise Exception("Not implemented yet")

def frequent_parks_count_dask(filename):
    '''
    Write a Python script using Dask that prints the list of the 10 parks
    with the highest number of treated trees. Parks must be ordered by
    decreasing number of treated trees and by alphabetical order when they have
    similar number.  Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count_dask.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''

    daskdf = df.read_csv("./data/frenepublicinjection2016.csv", dtype={'Nom_parc': 'object'})
    beforedf=daskdf.groupby('Nom_parc').size().compute()


    parkscounttuples=beforedf.reset_index(name='count').nlargest(10,'count').astype({'count': 'string'}).set_index(['Nom_parc','count']).index

    truestring=''
    for row in parkscounttuples:
        truestring = truestring + row[0] + ',' + row[1] + '\n'

    return(truestring)

    # ADD YOUR CODE HERE
    raise Exception("Not implemented yet")

def intersection_dask(filename1, filename2):
    '''
    Write a Python script using Dask that prints the alphabetically
    sorted list of parks that had trees treated both in 2016 and 2015. Every
    list element must be printed on a new line.
    Test file: tests/test_intersection_dask.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''

    daskdb2016 = db.from_sequence(df.read_csv("./data/frenepublicinjection2016.csv", dtype={'Nom_parc': 'object'})['Nom_parc'].unique().dropna().compute().values)
    list2015 = df.read_csv("./data/frenepublicinjection2015.csv", dtype={'Nom_parc': 'object'})['Nom_parc'].unique().dropna().compute().values




    return('\n'.join(daskdb2016.join(list2015, lambda x: x).map(lambda x:x[0]).compute())+'\n')


    # ADD YOUR CODE HERE
    raise Exception("Not implemented yet")
