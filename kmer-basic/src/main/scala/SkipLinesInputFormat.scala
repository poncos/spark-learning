import java.util

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, NLineInputFormat}
import scala.collection.JavaConversions._

class SkipLinesInputFormat extends NLineInputFormat {

  val LINES_PER_RECORD = "spark.input.skiplineinputformat.linesperrecord"

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[LongWritable, Text] = {
    println(s"createRecordReader ...")
    new SkipLinesRecordReader(split.asInstanceOf[FileSplit], context)
  }

  override def getSplits(job: JobContext): util.List[InputSplit] = {
    val recordsPerSplit = job.getConfiguration.getInt(NLineInputFormat.LINES_PER_MAP, 10000)
    val linesPerRecord = job.getConfiguration.getInt(LINES_PER_RECORD, 4)

    println(s"getSplits: recordsPerSplit [$recordsPerSplit], linesPerRecord [$linesPerRecord]")

    val splits = new util.ArrayList[InputSplit]
    for (status <- listStatus(job)) {
      println(s"Listing status: ${status.getPath}")
      for (split <- NLineInputFormat.getSplitsForFile(status, job.getConfiguration, recordsPerSplit * linesPerRecord)) {
        println(s"adding split with length: [${split.getLength}] and start [${split.getStart}]")
        splits.add(split)
      }
    }
    println(f"getSplits has finished with ${splits.length} splits.")
    splits
  }
}
