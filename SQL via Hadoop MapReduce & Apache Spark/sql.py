#@author: jeffyjacob
#Spark Version              : 2.4.0
#Python Version             : Python 2.7.14

from pyspark import SparkContext
import sys

def print_output(x):
    for i in range(len(x)):
        return "\t".join([str(x[0]),str(x[1])])


sc = SparkContext(appName='inf553')
city_data = sc.textFile(sys.argv[1])
country_data = sc.textFile(sys.argv[2])

#country.data w[0] - code
#country.data w[1] - name
country = country_data.map(lambda s:s.split("\t")).map(lambda w:(str(w[0]),("Country",w[1])))
#city.data w[1] - name
#city.data w[2] - code
#city.data w[4] - population
city = city_data.map(lambda s: s.split('\t')).filter(lambda x: int(x[4])>1000000).map(lambda w: (str(w[2]),("City",w[1])))

x =  city.join(country)

output = x.map(lambda x:(x[1][1][1],1)).reduceByKey(lambda u,x: u+x).filter(lambda x:x[1]>=3).map(print_output)
output.coalesce(1).saveAsTextFile(sys.argv[3])
