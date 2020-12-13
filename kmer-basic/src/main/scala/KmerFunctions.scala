import org.apache.hadoop.io.{LongWritable, Text}

object KmerFunctions {

  def kmerExtract(read: (LongWritable, Text)): Seq[String] = {
    splitString(read._2.toString, 32, 0)
  }

  def splitString(xs: String, size: Int, offset: Int) : List[String] = {
    if (xs.isEmpty || offset > xs.length - size)  Nil
    else {
      val substring = xs.slice(offset, offset + size)
      substring :: splitString(xs, size, offset+1)
    }
  }
}
