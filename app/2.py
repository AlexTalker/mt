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

logFile    = sys.argv[1]
errorsFile = sys.argv[2]


with SparkContext() as ctx:
    # Errors
    logs  = ctx.textFile(logFile).map(_extractor)
    error = logs.filter(lambda v: int(v.code) in range(500,600))
    SQLContext(ctx).createDataFrame(error).write.mode('overwrite').json(errorsFile)
