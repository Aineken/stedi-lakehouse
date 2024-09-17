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

# Script generated for node Customer Landing
CustomerLanding_node1726578179064 = glueContext.create_dynamic_frame.from_catalog(database="mike", table_name="landing_customer", transformation_ctx="CustomerLanding_node1726578179064")

# Script generated for node Landing to Trusted
SqlQuery1794 = '''
select * from myDataSource
where shareWithResearchasOfDate is not null


'''
LandingtoTrusted_node1726578183489 = sparkSqlQuery(glueContext, query = SqlQuery1794, mapping = {"myDataSource":CustomerLanding_node1726578179064}, transformation_ctx = "LandingtoTrusted_node1726578183489")

# Script generated for node Customer Trusted
CustomerTrusted_node1726578190015 = glueContext.getSink(path="s3://stedi-project/trusted/customer/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1726578190015")
CustomerTrusted_node1726578190015.setCatalogInfo(catalogDatabase="mike",catalogTableName="trusted_customer")
CustomerTrusted_node1726578190015.setFormat("json")
CustomerTrusted_node1726578190015.writeFrame(LandingtoTrusted_node1726578183489)
job.commit()