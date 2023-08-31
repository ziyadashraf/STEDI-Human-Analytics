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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1676576584339 = glueContext.create_dynamic_frame.from_catalog(
    database="project",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node1676576584339",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1",
)

# Script generated for node Join Customer and Step Trainer
JoinCustomerandStepTrainer_node1676402624834 = Join.apply(
    frame1=AccelerometerTrusted_node1,
    frame2=StepTrainerTrusted_node1676576584339,
    keys1=["timestamp"],
    keys2=["sensorreadingtime"],
    transformation_ctx="JoinCustomerandStepTrainer_node1676402624834",
)

# Script generated for node Drop Fields
DropFields_node1676402768067 = DropFields.apply(
    frame=JoinCustomerandStepTrainer_node1676402624834,
    paths=["user"],
    transformation_ctx="DropFields_node1676402768067",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1676578616395 = glueContext.write_dynamic_frame.from_catalog(
    frame=DropFields_node1676402768067,
    database="stedi",
    table_name="machine_learning_curated",
    transformation_ctx="StepTrainerTrusted_node1676578616395",
)

job.commit()
