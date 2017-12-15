
spark = SparkSession.builder.master("local").getOrCreate()

print spark.sqlContext.getAllConfs

spark.read.options(header='true').csv('data/*/dag_run.csv').show()

spark.read.options(header='true').csv('data/*/logs.csv').show()
