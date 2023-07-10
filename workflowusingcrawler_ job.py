import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="destinationdb", table_name="04072023", transformation_ctx="S3bucket_node1"
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("rank", "long", "rank", "int"),
        ("title", "string", "title", "string"),
        ("director", "string", "director", "string"),
        ("actors", "string", "actors", "string"),
        ("year", "long", "year", "int"),
        ("rating", "double", "rating", "double"),
        ("votes", "long", "votes", "int"),
        ("`revenue (millions)`", "double", "`revenue (millions)`", "double"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Filter
Filter_node1688969673535 = Filter.apply(
    frame=ApplyMapping_node2,
    f=lambda row: (row["revenue (millions)"] > 300),
    transformation_ctx="Filter_node1688969673535",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Filter_node1688969673535,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": "s3://04072023/output/", "partitionKeys": []},
    transformation_ctx="S3bucket_node3",
)

job.commit()
