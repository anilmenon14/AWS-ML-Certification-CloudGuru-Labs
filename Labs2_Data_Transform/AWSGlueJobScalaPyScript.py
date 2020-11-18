# This script was used to be able to transform data being crawled into table awsrandomuserdbdemo to an S3 output as csv

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "sampledb", table_name = "awsrandomuserdbdemo", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "sampledb", table_name = "awsrandomuserdbdemo", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("first", "string", "first", "string"), ("last", "string", "last", "string"), ("age", "int", "age", "int"), ("gender", "string", "gender", "string"), ("latitude", "string", "latitude", "string"), ("longitude", "string", "longitude", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("first", "string", "first", "string"), ("last", "string", "last", "string"), ("age", "int", "age", "int"), ("gender", "string", "gender", "string"), ("latitude", "string", "latitude", "string"), ("longitude", "string", "longitude", "string")], transformation_ctx = "applymapping1")
## @type: Map
## @args: [f = <function>, transformation_ctx = "<transformation_ctx>"]
## @return: <output>
## @inputs: [frame = <frame>]
def genderNum(rec):
    if rec["gender"] == "male":
        rec["gender"] = 1
    else:
        rec["gender"] = 0
    return rec
mappeddf = Map.apply(frame = applymapping1, f = genderNum)
mappeddf.toDF().show()
## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://mypersonalizedbucket/AthenaRandomUserResults/"}, format = "csv", transformation_ctx = "datasink2"]
## @return: datasink2
## @inputs: [frame = applymapping1]
datasink2 = glueContext.write_dynamic_frame.from_options(frame = mappeddf, connection_type = "s3", connection_options = {"path": "s3://mypersonalizedbucket/AthenaRandomUserResults/"}, format = "csv", transformation_ctx = "datasink2")
job.commit()
