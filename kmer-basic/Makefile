
submit:
	/home/opt/spark-2.4.7-bin-without-hadoop/bin/spark-submit \
	--class KmerApp --master yarn --deploy-mode cluster \
	--driver-memory 4g  --executor-memory 2g --executor-cores 1  \
	--queue default \
	--conf "mapreduce.input.skiplineinputformat.linesperrecord=4" \
	--conf "mapreduce.input.skiplineinputformat.linesfilter=0,0,1,0" \
	--conf "mapreduce.input.lineinputformat.linespermap=10000" \
	./target/scala-2.11/kmer-basic_2.11-1.0.jar
