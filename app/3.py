from utils import logs, create_frame, save_frames
from pyspark import Row

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

dated = logs().map(date_request)\
    .groupByKey()\
    .mapValues(list)\
    .mapValues(request_counter)\
    .mapValues(filter_requests)

save_frames(dates=create_frame(dated))
