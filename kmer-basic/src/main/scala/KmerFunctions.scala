import org.apache.hadoop.io.{LongWritable, Text}

object KmerFunctions {

  def kmerExtract(read: (LongWritable, Text)): Seq[String] = {
    val appConfig = new AppConfig
    splitString(read._2.toString, appConfig.kmerLength, 0)
  }

  def fastqReadToKmerTuple(read: (LongWritable, Text)): Seq[(String, Long)] = {
    val kmers = splitString(read._2.toString, 32, 0)
    kmers.map( kmer => (kmer, 1L))
  }

  def splitString(xs: String, size: Int, offset: Int) : List[String] = {
    if (xs.isEmpty || offset > xs.length - size)  Nil
    else {
      val substring = xs.slice(offset, offset + size)
      substring :: splitString(xs, size, offset+1)
    }
  }
}
