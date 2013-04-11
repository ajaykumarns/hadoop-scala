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

object Counter extends SimpleJobDriver(classOf[ImgCounterMapper], null){
  override def process(job: Job) {
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[HInt])
    job.setNumReduceTasks(0)
  }
}

class SequenceFileMapper extends SimpleMapper[HLong, Text, HLong, Text]{
  override def map{
    write(key, value)
  }
}

object UncompressedSeqFile extends SimpleJobDriver(classOf[SequenceFileMapper], null){
  override def process(job: Job){
    job.setOutputKeyClass(classOf[HLong])
    job.setOutputValueClass(classOf[Text])
    job.setNumReduceTasks(0)
    FileOutputFormat.setCompressOutput(job, true)
    FileOutputFormat.setOutputCompressorClass(job, classOf[SnappyCodec])
    SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK)
    job.setOutputFormatClass(classOf[SequenceFileOutputFormat[_,_]])
  }
}

object CompressedFileReader extends SimpleJobDriver(classOf[SequenceFileMapper], null){
  override def process(job: Job) {
    job.setOutputKeyClass(classOf[HLong])
    job.setOutputValueClass(classOf[Text])
    job.setNumReduceTasks(0)
    job.setInputFormatClass(classOf[SequenceFileInputFormat[_,_]])
  }
}