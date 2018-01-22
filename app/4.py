from pyspark import SparkContext, Row
from pyspark.sql import SQLContext, Window
from pyspark.sql.functions import first, sum

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

def date_request(r):
     return Row(r.datetime[:11], 1)

def error_codes(v):
    return int(v.code) in range(400, 600)

logFile    = sys.argv[1]
framesFile = sys.argv[2]

with SparkContext() as ctx:
    logs   = ctx.textFile(logFile).map(_extractor)
    errors = logs.filter(error_codes).map(date_request).groupByKey().mapValues(len)
    win    = Window.partitionBy("_1").orderBy("_1").rowsBetween(0, 6)
    frame  = SQLContext(ctx).createDataFrame(errors).select(
            first('_1').over(win).alias("_1"),
            sum('_2').over(win).alias('_2')
    )
    frame.write.mode('overwrite').json(framesFile)
