CREATE EXTERNAL TABLE `landing_accelerometer`(
  `user` string COMMENT 'from deserializer',
  `timestamp` timestamp COMMENT 'from deserializer',
  `x` float COMMENT 'from deserializer',
  `y` float COMMENT 'from deserializer',
  `z` float COMMENT 'from deserializer')
ROW FORMAT SERDE
  'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'paths'='timestamp,user,x,y,z')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://stedi-project/landing/accelerometer/'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0',
  'CrawlerSchemaSerializerVersion'='1.0',
  'UPDATED_BY_CRAWLER'='accelerometer_crawler',
  'averageRecordSize'='761',
  'classification'='json',
  'compressionType'='none',
  'objectCount'='9',
  'recordCount'='9007',
  'sizeKey'='6871328',
  'typeOfData'='file')