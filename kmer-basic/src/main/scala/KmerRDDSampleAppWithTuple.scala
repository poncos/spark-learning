import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.hadoop.mapreduce.lib.input.{KeyValueTextInputFormat, NLineInputFormat}
import org.slf4j.LoggerFactory

object KmerRDDSampleAppWithTuple {

  val LOG = LoggerFactory.getLogger("MAIN")

  def main(args: Array[String]) {

    val rawData = "/user/poncos/kmers/input/ERR188245_1_piece.fastq"
    val conf = new SparkConf().setAppName("Simple Kmer Application")
      .set("mapreduce.input.lineinputformat.linespermap","10000")
    val sc = new SparkContext(conf)

    LOG.info(sc.getConf.toDebugString)
    LOG.info(s"reading file with ...")
    val reads: RDD[(LongWritable, Text)] = sc.newAPIHadoopFile(rawData, classOf[SkipLinesInputFormat],
      classOf[LongWritable], classOf[Text])

/*
    LOG.info(s"Detecting kmers ...")
    val kmers: RDD[(String, Long)] = reads.flatMap(KmerFunctions.fastqReadToKmerTuple)
    printf(s"Counting kmers ...")
    val kmersFrequency: RDD[(String, Long)] = kmers.reduceByKey( (c1, c2) => {
      LOG.info(s"adding up ${c1} + ${c2}")
      c1 + c2
    })
    */
    val count = reads.count()
    LOG.info(s"Saving file [${count}]")


    reads.saveAsTextFile(f"/user/poncos/kmers/output/${java.util.UUID.randomUUID()}/")

    sc.stop()
  }

}
