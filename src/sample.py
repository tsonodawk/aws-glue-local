import sys
from awsglue.transforms import ApplyMapping
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

DYNAMODB_INPUT_TABLE_NAME = 'aws-glue-local-test-table'
S3_OUTPUT_BUCKET_NAME = 'aws-glue-local-test-bucket'


args = getResolvedOptions(sys.argv, [
    'JOB_NAME'
])


JOB_NAME = args['JOB_NAME']

print('aaa========================================================')

sc = SparkContext()
print('bbb========================================================')

glueContext = GlueContext(sc)
print('ccc========================================================')

job = Job(glueContext)
print('ddd========================================================')

job.init(JOB_NAME, args)

print('eee========================================================')

datasource = glueContext.create_dynamic_frame.from_options(
    connection_type='dynamodb',
    connection_options={
        'dynamodb.input.tableName': DYNAMODB_INPUT_TABLE_NAME
    }
)


print('fff========================================================')

applymapping = ApplyMapping.apply(
    frame=datasource,
    mappings=[
        ('Id', 'string', 'Id', 'string'),
        ('Column1', 'string', 'Column1', 'string'),
        ('Column2', 'string', 'Column2', 'string'),
        ('Column3', 'string', 'Column3', 'string')
    ]
)

# datasource.toDF().show()
#datasource.select_fields(['Id']).toDF().distinct().show()

print('ggg========================================================')

dataskin2 = glueContext.write_dynamic_frame.from_options(
    frame=datasource,
    connection_type='s3',
    connection_options={
        'path': 's3://' + S3_OUTPUT_BUCKET_NAME
    },
    format='csv',
    transformation_ctx = "datasink2",
)

print('hhh========================================================')

job.commit()
