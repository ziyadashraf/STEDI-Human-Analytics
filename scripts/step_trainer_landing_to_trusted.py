import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Curated
CustomerCurated_node1676576584401 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="CustomerCurated_node1676576584401",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://ziyad-stedi-lake/project/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1",
)

# Script generated for node Join Customer and Step Trainer
JoinCustomerandStepTrainer_node1676402624789 = Join.apply(
    frame1=StepTrainerLanding_node1,
    frame2=CustomerCurated_node1676576584401,
    keys1=["serialNumber"],
    keys2=["serialnumber"],
    transformation_ctx="JoinCustomerandStepTrainer_node1676402624789",
)

# Script generated for node Drop Fields
DropFields_node1676402768073 = DropFields.apply(
    frame=JoinCustomerandStepTrainer_node1676402624789,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithfriendsasofdate",
        "sharewithpublicasofdate",
    ],
    transformation_ctx="DropFields_node1676402768073",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1676576584339 = glueContext.write_dynamic_frame.from_catalog(
    frame=DropFields_node1676402768073,
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node1676576584339",
)

job.commit()
