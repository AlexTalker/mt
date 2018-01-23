from pyspark import SparkContext, Row
from pyspark.sql import SQLContext
import re
import sys

# Sufficient pattern:
PATTERN     = '^(?P<host>\S+) - - \[(?P<datetime>.+)\] "((?P<method>\w+)\s+)?(?P<request>.+)" (?P<code>\d+) (?P<bytes>[\d\-]+)$'
log_pattern = re.compile(PATTERN)
def _extractor(line):
    match = log_pattern.match(line)
    if match is None:
        raise Exception(line)
    return Row(**match.groupdict())

# Time series
def date_request(r):
    return Row((r.datetime[:11], "%s %s" % (r.method, r.code)), 1)

logFile   = sys.argv[1]
datesFile = sys.argv[2]

with SparkContext() as ctx:
    logs  = ctx.textFile(logFile).map(_extractor)
    dated = logs.map(date_request)\
        .groupByKey()\
        .mapValues(len)\
        .filter(lambda v: v[1] >= 10)\
        .map(lambda v: Row(v[0][0], (v[0][1], v[1])))\
        .groupByKey()\
        .mapValues(list)
    SQLContext(ctx).createDataFrame(dated).write.mode('overwrite').json(datesFile)
