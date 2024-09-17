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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1726579331599 = glueContext.create_dynamic_frame.from_catalog(database="mike", table_name="trusted_accelerometer", transformation_ctx="AccelerometerTrusted_node1726579331599")

# Script generated for node Join
Join_node1726582867181 = Join.apply(frame1=CustomerTrusted_node1726580155756, frame2=AccelerometerTrusted_node1726579331599, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1726582867181")

# Script generated for node Drop Fields
DropFields_node1726582923533 = DropFields.apply(frame=Join_node1726582867181, paths=["z", "y", "timestamp", "user", "x"], transformation_ctx="DropFields_node1726582923533")

# Script generated for node Transform
SqlQuery1793 = '''
select distinct customername, email, phone, birthday, 
serialnumber, registrationdate, lastupdatedate, 
sharewithresearchasofdate, sharewithpublicasofdate,
sharewithfriendsasofdate from myDataSource
'''
Transform_node1726579334767 = sparkSqlQuery(glueContext, query = SqlQuery1793, mapping = {"myDataSource":DropFields_node1726582923533}, transformation_ctx = "Transform_node1726579334767")

# Script generated for node Customer Curated
CustomerCurated_node1726579340632 = glueContext.getSink(path="s3://stedi-project/curated/customer/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1726579340632")
CustomerCurated_node1726579340632.setCatalogInfo(catalogDatabase="mike",catalogTableName="curated_customer")
CustomerCurated_node1726579340632.setFormat("json")
CustomerCurated_node1726579340632.writeFrame(Transform_node1726579334767)
job.commit()