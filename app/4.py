from utils import logs, create_frame, save_frames
from pyspark import Row
from pyspark.sql import Window
from pyspark.sql.functions import first, sum

def date_request(r):
     return Row(r.datetime[:11], 1)

def error_codes(v):
    return int(v.code) in range(400, 600)

errors = logs().filter(error_codes).map(date_request).groupByKey().mapValues(len)
win    = Window.partitionBy("_1").orderBy("_1").rowsBetween(0, 6)
frame  = create_frame(errors).select(
        first('_1').over(win).alias("_1"),
        sum('_2').over(win).alias('_2')
)

save_frames(frames=frame)
