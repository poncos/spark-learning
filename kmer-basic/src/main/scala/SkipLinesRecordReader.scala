import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, SplitLineReader, UncompressedSplitLineReader}
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}

class SkipLinesRecordReader(fileSplit: FileSplit, context: TaskAttemptContext)
  extends RecordReader[LongWritable, Text] {

  val SKIP_LINES_FILTER                   = "spark.input.skiplineinputformat.linesfilter"
  val LINES_PER_RECORD                    = "spark.input.skiplineinputformat.linesperrecord"
  val LINE_MAX_LENTH                      = "spark.input.linerecordreader.line.maxlength"

  val DEFAULT_MAX_LINE_LENGTH             = 1024
  val DEFAULT_LINES_FILTER : Array[Int]   = Array(0,0,1,0)

  var jobConf : Configuration             = context.getConfiguration
  var linesPerRecord                      = this.jobConf.getInt(LINES_PER_RECORD, 1)

  val linesFilter : Array[Int]            = DEFAULT_LINES_FILTER
  var maxLineLength                       = this.jobConf.getInt(LINE_MAX_LENTH, DEFAULT_MAX_LINE_LENGTH)

  var fileIn : FSDataInputStream          = null
  var start                               = fileSplit.getStart
  var end                                 = start + fileSplit.getLength
  var pos                                 = 0L

  var in : SplitLineReader                = null
  var currentKey : LongWritable           = null
  var currentValue : Text                 = null
  var currentRecordLine : Int             = 0

  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
    val file = this.fileSplit.getPath
    // open the file and seek to the start of the split
    val fs = file.getFileSystem(this.jobConf)
    this.fileIn = fs.open(file)

    this.fileIn.seek(start)
    this.in = new UncompressedSplitLineReader(fileIn, this.jobConf, null, this.fileSplit.getLength)

    if (this.start > 0) start += in.readLine(new Text, 0, maxBytesToConsume(start))

    this.pos = start

    println(
      s"SkipLinesRecordReader with [${this.linesPerRecord}] lines per record, and [${this.linesFilter}] filter.")
  }

  override def nextKeyValue(): Boolean = {
    println(s"nextKeyValue ...")
    if (this.currentKey == null) this.currentKey = new LongWritable

    if (this.currentValue == null) this.currentValue = new Text

    // We always read one extra line, which lies outside the upper
    // split limit i.e. (end - 1)
    while ( {
      this.pos <= this.end || this.in.needAdditionalRecordAfterSplit
    }) {

      this.currentKey.set(this.pos)

      val bytesRead = this.in.readLine(this.currentValue, this.maxLineLength, maxBytesToConsume(this.pos))
      pos += bytesRead

      if (bytesRead == 0) return false

      if (bytesRead <= this.maxLineLength) {

        if (this.currentRecordLine >= this.linesPerRecord) this.currentRecordLine = 0
        this.currentRecordLine+=1
        if (this.linesFilter(this.currentRecordLine-1) == 1) {
          println(s"Value: ${this.currentValue.toString}")
          return true
        }
      }
    }

    false
  }

  override def getCurrentKey: LongWritable = {
    println(s"getCurrentKey ${this.currentKey}")
    this.currentKey;
  }

  override def getCurrentValue: Text = this.currentValue

  override def getProgress: Float = {
    println("getProgress")
    if (this.start == this.end) 0.0f
    else Math.min(1.0f, (this.pos - this.start) / (this.end - this.start).toFloat)
  }

  override def close(): Unit = {
    try if (in != null) in.close()
    catch {
      case e: Exception =>
    }
  }

  private def maxBytesToConsume(pos: Long): Int =
    Math.max(Math.min(Integer.MAX_VALUE, end - pos), maxLineLength).toInt
}
