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

# Script generated for node Customer Trusted
CustomerTrusted_node1726580155756 = glueContext.create_dynamic_frame.from_catalog(database="mike", table_name="trusted_customer", transformation_ctx="CustomerTrusted_node1726580155756")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1726579331599 = glueContext.create_dynamic_frame.from_catalog(database="mike", table_name="landing_accelerometer", transformation_ctx="AccelerometerLanding_node1726579331599")

# Script generated for node Transform
SqlQuery0 = '''
select a.user, a.timeStamp, a.x,a.y,a.z from a
join c on a.user = c.email;
'''
Transform_node1726579334767 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"a":AccelerometerLanding_node1726579331599, "c":CustomerTrusted_node1726580155756}, transformation_ctx = "Transform_node1726579334767")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1726579340632 = glueContext.getSink(path="s3://stedi-project/trusted/accelerometer/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1726579340632")
AccelerometerTrusted_node1726579340632.setCatalogInfo(catalogDatabase="mike",catalogTableName="trusted_accelerometer")
AccelerometerTrusted_node1726579340632.setFormat("json")
AccelerometerTrusted_node1726579340632.writeFrame(Transform_node1726579334767)
job.commit()