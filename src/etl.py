from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext
import sys

def load_dynamic_frame_from_csv(glueContext: GlueContext, spark, bucket: str, path: str) -> DynamicFrame:
    p = "s3://{}/{}".format(bucket, path)
    return glueContext.create_dynamic_frame_from_options(
        connection_type="s3",
        connection_options={"paths": [p]},
        format="csv",
        format_options={"withHeader": True, "separator": ","},
    )

