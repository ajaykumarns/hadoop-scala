/**
 * Created with IntelliJ IDEA.
 * User: anadathur
 * Date: 4/8/13
 * Time: 12:04 AM
 * To change this template use File | Settings | File Templates.
 */
package com.anadathur.hadoop

import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, SequenceFileOutputFormat}
import org.apache.hadoop.io.compress.SnappyCodec
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.mapreduce.lib.input.{SequenceFileInputFormat, FileInputFormat}


class ImgCounterMapper extends SimpleMapper[HLong, Text, Text, HInt]{
  val regex = """(?i)(jpg|gif|jpeg)""".r
  override def map{
    println(s"Processing line: $value")
    for(word <- regex.findAllIn(value)){
      incrementCounter(word.toLowerCase)
    }
  }
}

object Counter extends SimpleJobDriver{
  override def process(job: Job) {
    job.withOnlyMapper(classOf[ImgCounterMapper])
      .deduceInputOutputTypes()
  }
}

class SequenceFileMapper extends SimpleMapper[HLong, Text, HLong, Text]{
  override def map{
    write(key, value)
  }
}

object UncompressedSeqFile extends SimpleJobDriver{
  override def process(job: Job){
    job.withOnlyMapper(classOf[SequenceFileMapper])
      .deduceInputOutputTypes()
      .enableCompression()
      .withOutputFormat(classOf[SequenceFileOutputFormat[_,_]])
      .withCompressionType(CompressionType.BLOCK)
      .withCompressor(classOf[SnappyCodec])
  }
}

object CompressedFileReader extends SimpleJobDriver(classOf[SequenceFileMapper], null){
  override def process(job: Job) {
    job.withOnlyMapper(classOf[SequenceFileMapper])
      .deduceInputOutputTypes()
      .withInputFormat(classOf[SequenceFileInputFormat[_,_]])
  }
}