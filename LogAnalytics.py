import sys
from operator import  add
from pyspark import SparkContext

# Check for number of inputs passed from command line
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: access_log.py <file>"
        exit(-1)

sc = SparkContext(appName="Log Analytics")

access_log = sc.textFile(sys.argv[1], 4)

error_log = access_log.filter(lambda x: "ERROR" in x)

cached_log = error_log.cache()

print ("Total number of error records are %s " % (cached_log.count())

print ("Number of product pages visited that have Errors is %s" % (cached_log.filter(lambda x: "product" in x).count()) 
