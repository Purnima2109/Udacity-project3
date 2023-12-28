import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

class Acc_land:
    def __init__(self):
        args = getResolvedOptions(sys.argv, ["JOB_NAME"])
        sc = SparkContext()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session
        job = Job(glueContext)
        job.init(args["JOB_NAME"], args)
        s3_node11 = self.s3_fun(glueContext)
        am_node1 = self.am_fun(glueContext)
        join1 = self.join12(s3_node11, am_node1)
        drop_node = self.drop1(join1)
        s3_node13 = self.s3_fun3(glueContext,drop_node)
        job.commit()

def s3_fun(glueContext):
    return glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://stedi-project-udacity/customer_trusted/"],
            "recurse": True,
        },
        transformation_ctx="s3_node11",
    )

def am_fun(glueContext):
    return glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-project-udacity/accelerometer_landing/"],
        "recurse": True,
    },
    transformation_ctx="am_node1",
)

def join12(s3_node11, am_node1):
    return Join.apply(
    frame1=s3_node11,
    frame2=am_node1,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="join1",
)

def drop1(join1):
    return DropFields.apply(
    frame=join1,
    paths=[
        "customerName",
        "email",
        "phone",
        "birthDay",
        "serialNumber",
        "registrationDate",
        "lastUpdateDate",
        "shareWithResearchAsOfDate",
        "shareWithPublicAsOfDate",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="drop_node",
)

def s3_fun3(glueContext,drop_node):
    return glueContext.write_dynamic_frame.from_options(
    frame=drop_node,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-project-udacity/accelerometer_trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="s3_node13",
)

acc = Acc_land()

