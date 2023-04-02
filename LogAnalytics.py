# Get the lines from the textfile, create 4 partitions
# diganti nama file
access_log = sc.textFile("file:///home/cloudera/logs.txt", 4)
# access_log = sc.textFile(sys.argv[1], 4)

#Filter Lines with ERROR only
error_log = access_log.filter(lambda x: "ERROR" in x)

# Cache error log in memory
cached_log = error_log.cache()

# Now perform an action -  count
print("Total number of error records are %s " % cached_log.count())

# Now find the number of lines with 
print("Number of product pages visited that have Errors is %s" % cached_log.filter(lambda x: "product" in x).count())