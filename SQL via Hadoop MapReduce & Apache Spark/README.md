On Amazon AWS EC2
## HADOOP MAP REDUCE:
    Hadoop Version          : 2.7.5
	  Java (openjdk) Version  : 1.8.0_191

### EXECUTION (Compile and run):
  hadoop com.sun.tools.javac.Main sql.java
	jar cf sqlc.jar sql*.class
	hadoop jar sqlc.jar sql <city_dir> <country_dir> <output_dir>

## SPARK:
	Spark Version              : 2.4.0
	Python Version             : Python 2.7.14

### EXECUTION:
	bin/spark-submit SQLCount.py <city_dir> <country_dir> <output_dir>
