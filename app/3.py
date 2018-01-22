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
    return Row(r.datetime[:11], "%s %s" % (r.method, r.code))

def request_counter(requests):
    result = {}
    for r in requests:
        if r not in result:
            result[r] = 0
        result[r] += 1
    return result

def filter_requests(requests):
    result = {}
    for k in requests:
        if requests[k] >= 10:
            result[k] = requests[k]
    return result

logFile   = sys.argv[1]
datesFile = sys.argv[2]

with SparkContext() as ctx:
    logs  = ctx.textFile(logFile).map(_extractor)
    dated = logs.map(date_request)\
        .groupByKey()\
        .mapValues(list)\
        .mapValues(request_counter)\
        .mapValues(filter_requests)
    SQLContext(ctx).createDataFrame(dated).write.mode('overwrite').json(datesFile)
