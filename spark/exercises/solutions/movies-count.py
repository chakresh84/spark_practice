# Load pyspark module
from pyspark import SparkContext
sc=SparkContext()

#Load the movies.dat file in memory
movies=sc.textFile("mydata/movies.txt")

#Split the RDD based on '::' as delimiter
movies_split=movies.map(lambda line:line.split("::"))

#Take only the movies fields containing movie names
movies_names=movies_split.map(lambda fields: fields[1])

#Find the number of movies released in 1993
movies_release=movies_names.filter(lambda word: "1993" in word)

#Count the number of movies released in 1993
print "##### Number of movies release in 1993 = "+ str(movies_release.count())

 
