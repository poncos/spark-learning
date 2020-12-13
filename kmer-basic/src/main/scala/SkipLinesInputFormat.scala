import java.util

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, NLineInputFormat}

class SkipLinesInputFormat extends NLineInputFormat {

  val LINES_PER_RECORD = "mapreduce.input.skiplineinputformat.linesperrecord"

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[LongWritable, Text] = {
    new SkipLinesRecordReader(split.asInstanceOf[FileSplit], context)
  }

  override def getSplits(job: JobContext): util.List[InputSplit] = {
    val recordsPerSplit = job.getConfiguration.getInt(NLineInputFormat.LINES_PER_MAP, 10000)
    val linesPerRecord = job.getConfiguration.getInt(LINES_PER_RECORD, 4)

    println(s"getSplits: recordsPerSplit [$recordsPerSplit], linesPerRecord [$linesPerRecord]")

    val splits = new util.ArrayList[InputSplit]
    import scala.collection.JavaConversions._
    for (status <- listStatus(job)) {
      import scala.collection.JavaConversions._
      for (split <- NLineInputFormat.getSplitsForFile(status, job.getConfiguration, recordsPerSplit * linesPerRecord)) {
        splits.add(split)
      }
    }
    splits
  }
}
