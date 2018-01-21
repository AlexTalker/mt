from utils import logs, create_frame, save_frames
from pyspark.sql import SQLContext

# Errors
error = logs().filter(lambda v: int(v.code) in range(500,600))

save_frames(errors=create_frame(error))
