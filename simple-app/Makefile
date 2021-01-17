
submit:
	${SPARK_HOME}/bin/spark-submit \
	--class SimpleApp --master yarn --deploy-mode cluster \
	--driver-memory 4g  --executor-memory 2g --executor-cores 1  \
	--queue default target/scala-2.11/spark-simple-app_2.11-1.0.jar
