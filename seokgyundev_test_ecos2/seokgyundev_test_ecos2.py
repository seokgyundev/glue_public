import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

inputs = ['JOB_NAME', 'parquet_table_name', 'rawdata_s3_path', 'parquet_s3_path']
args = getResolvedOptions(sys.argv, inputs)
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

print('============== input log ==============')
print(str(args))
print('============== data define ==============')
database_name = "datalake-sample001"
parquet_table_name = args["parquet_table_name"]
rawdata_s3_path = args["rawdata_s3_path"]
parquet_s3_path = args["parquet_s3_path"]

print('database_name : '+database_name)
print('parquet_table_name : '+parquet_table_name)
print('rawdata_s3_path : '+rawdata_s3_path)
print('parquet_s3_path : '+parquet_s3_path)

# print('============== get rawdata from S3 ==============')
# rawdata_frame = glueContext.create_dynamic_frame.from_options(
#     format_options={"jsonPath": "$.StatisticSearch.row[*]", "multiline": False},
#     connection_type="s3",
#     format="json",
#     connection_options={
#         "paths": [rawdata_s3_path],
#         "recurse": True,
#     },
#     transformation_ctx="rawdata_frame",
# )

# print('============== define transform info ==============')
# transform_info = ApplyMapping.apply(
#     frame=rawdata_frame,
#     mappings=[
#         ("ITEM_NAME1", "string", "item_name1", "string"),
#         ("TIME", "string", "time", "string"),
#         ("DATA_VALUE", "string", "data_value", "float"),
#         ("UNIT_NAME", "string", "unit_name", "string"),
#     ],
#     transformation_ctx="transform_info",
# )

# print('============== define parquet info ==============')
# parquet_frame = glueContext.getSink(
#     path=parquet_s3_path,
#     connection_type="s3",
#     updateBehavior="UPDATE_IN_DATABASE",
#     #partitionKeys=["year", "month", "day"],
#     compression="uncompressed",
#     #enableUpdateCatalog=True,
#     transformation_ctx="parquet_frame",
# )

# # print('============== define parquet catalog info ==============')
# # parquet_frame.setCatalogInfo(
# #     catalogDatabase=database_name,
# #     catalogTableName=parquet_table_name,
# # )

# parquet_frame.setFormat("glueparquet")
# parquet_frame.writeFrame(transform_info)
# job.commit()
