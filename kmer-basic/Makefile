
#  export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
#  export SPARK_HOME=/home/opt/spark-3.0.1-bin-hadoop2.7
submit:
	${SPARK_HOME}/bin/spark-submit \
	--class KmerRDDSampleAppWithTuple \
	--master yarn \
	--deploy-mode cluster \
	--driver-memory 4g \
	--executor-memory 2g \
	--executor-cores 1  \
	--queue default \
	--conf spark.input.skiplineinputformat.linesperrecord=4 \
	--conf spark.input.skiplineinputformat.linesfilter=0,0,1,0 \
	--conf spark.mapreduce.input.lineinputformat.linespermap=10000 \
	./target/scala-2.11/kmer-basic_2.11-1.0.jar
