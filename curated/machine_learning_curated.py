import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1726589490877 = glueContext.create_dynamic_frame.from_catalog(database="mike", table_name="trusted_step_trainer", transformation_ctx="StepTrainerTrusted_node1726589490877")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1726589490348 = glueContext.create_dynamic_frame.from_catalog(database="mike", table_name="trusted_accelerometer", transformation_ctx="AccelerometerTrusted_node1726589490348")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from tst
join ta on tst.sensorReadingTime = ta.timeStamp
'''
SQLQuery_node1726589493111 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"tst":StepTrainerTrusted_node1726589490877, "ta":AccelerometerTrusted_node1726589490348}, transformation_ctx = "SQLQuery_node1726589493111")

# Script generated for node Drop Fields
DropFields_node1726592613899 = DropFields.apply(frame=SQLQuery_node1726589493111, paths=["customerName", "user", "email", "phone", "birthDay", "sensorReadingTime"], transformation_ctx="DropFields_node1726592613899")

# Script generated for node Amazon S3
AmazonS3_node1726589495664 = glueContext.getSink(path="s3://stedi-project/curated/machine_learning/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1726589495664")
AmazonS3_node1726589495664.setCatalogInfo(catalogDatabase="mike",catalogTableName="curated_machine_learning")
AmazonS3_node1726589495664.setFormat("json")
AmazonS3_node1726589495664.writeFrame(DropFields_node1726592613899)
job.commit()