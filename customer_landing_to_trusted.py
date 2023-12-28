import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

class CusLand:
    def __init__(self):

        args = getResolvedOptions(sys.argv, ["JOB_NAME"])
        sc = SparkContext()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session
        job = Job(glueContext)
        job.init(args["JOB_NAME"], args)
        s3_node11 = self.s3_fun4(glueContext)
        filter_node = self.filt(s3_node11)
        s3_node13 = self.s3_fun5(glueContext, filter_node)
        job.commit()

def s3_fun4(glueContext):
    return glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-project-udacity/customer_landing/"],
        "recurse": True,
    },
    transformation_ctx="s3_node11",
)

def filt(s3_node11):
    return Filter.apply(
    frame=s3_node11,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="filter_node",
)

def s3_fun5(glueContext, filter_node):
    return glueContext.write_dynamic_frame.from_options(
    frame=filter_node,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-project-udacity/customer_trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="s3_node13",
)

cus1 = CusLand()
