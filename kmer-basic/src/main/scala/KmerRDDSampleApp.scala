import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object KmerRDDSampleApp {

  def main(args: Array[String]) {

    val rawData   = "/user/poncos/kmers/input/ERR188245_1_piece.fastq"
    val conf      = new SparkConf().setAppName("Simple Kmer Application")
    val sc        = new SparkContext(conf)

    val reads: RDD[(LongWritable, Text)] = sc.newAPIHadoopFile(rawData, classOf[SkipLinesInputFormat],
      classOf[LongWritable], classOf[Text])

    val kmers: RDD[(String, Long)] = reads.flatMap(KmerFunctions.fastqReadToKmerTuple)
    val kmersFrequency: RDD[(String, Long)] = kmers.reduceByKey( (c1, c2) => c1 + c2)

    kmersFrequency.saveAsTextFile("/user/poncos/kmers/output/")

    sc.stop()
  }

}
