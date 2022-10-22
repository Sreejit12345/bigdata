from pyspark.sql import SparkSession
from pyspark import SparkConf

import yaml

path="E:\\sparkpractice\\pyspark_production_code_env\\app_properties_in_YAML.yml"

with open(path) as file:
    data=yaml.load(file,yaml.SafeLoader)
    print(data)
