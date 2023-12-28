import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

class Machine:
    def __init__(self):
        args = getResolvedOptions(sys.argv, ["JOB_NAME"])
        sc = SparkContext()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session
        job = Job(glueContext)
        job.init(args["JOB_NAME"], args)
        s3_node11 = self.s3_fun11(glueContext)
        am_node1 = self.am_fun11(glueContext)
        join_node = self.join3(s3_node11, am_node1)
        drop_node2 = self.drop3(join_node)
        s3_node13 = self.s3_fun12(glueContext,drop_node2)
        job.commit()

def s3_fun11(glueContext):
    return glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-project-udacity/accelerometer_trusted/"],
        "recurse": True,
    },
    transformation_ctx="s3_node11",
)

def am_fun11(glueContext):
    return glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-project-udacity/step_trainer_trusted/"],
        "recurse": True,
    },
    transformation_ctx="am_node1",
)

def join3(s3_node11, am_node1):
    return Join.apply(
    frame1=s3_node11,
    frame2=am_node1,
    keys1=["timeStamp"],
    keys2=["sensorReadingTime"],
    transformation_ctx="join_node",
)

def drop3(join_node):
    return DropFields.apply(
    frame=join_node,
    paths=["user"],
    transformation_ctx="drop_node2",
)

def s3_fun12(glueContext,drop_node2):
    return glueContext.write_dynamic_frame.from_options(
    frame=drop_node2,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-project-udacity/machine_learning_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="s3_node13",
)

mac = Machine()
