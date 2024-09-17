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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1726580497584 = glueContext.create_dynamic_frame.from_catalog(database="mike", table_name="landing_step_trainer", transformation_ctx="StepTrainerLanding_node1726580497584")

# Script generated for node Customer Curated
CustomerCurated_node1726580496611 = glueContext.create_dynamic_frame.from_catalog(database="mike", table_name="curated_customer", transformation_ctx="CustomerCurated_node1726580496611")

# Script generated for node SQL Query
SqlQuery0 = '''
select s.sensorReadingTime,s.serialNumber,s.distanceFromObject from s
join c on s.serialNumber= c.serialNumber
'''
SQLQuery_node1726580500030 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"s":StepTrainerLanding_node1726580497584, "c":CustomerCurated_node1726580496611}, transformation_ctx = "SQLQuery_node1726580500030")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1726580502631 = glueContext.getSink(path="s3://stedi-project/trusted/step_trainer/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1726580502631")
StepTrainerTrusted_node1726580502631.setCatalogInfo(catalogDatabase="mike",catalogTableName="trusted_step_trainer")
StepTrainerTrusted_node1726580502631.setFormat("json")
StepTrainerTrusted_node1726580502631.writeFrame(SQLQuery_node1726580500030)
job.commit()