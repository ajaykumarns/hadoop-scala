/**
 * Created with IntelliJ IDEA.
 * User: anadathur
 * Date: 4/3/13
 * Time: 7:59 PM
 * To change this template use File | Settings | File Templates.
 */
package com.anadathur

import org.apache.hadoop.mapreduce.{OutputFormat, Reducer, Mapper, Job}
import java.lang.reflect.ParameterizedType
import org.apache.hadoop.io.{LongWritable, FloatWritable, IntWritable}
import java.lang
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.output.{SequenceFileOutputFormat, FileOutputFormat}
import org.apache.hadoop.conf.{Configuration, Configurable}
import org.apache.hadoop.util.{ToolRunner, Tool}
import org.apache.hadoop.io.compress.{SnappyCodec, CompressionCodec}
import org.apache.hadoop.io.SequenceFile.CompressionType


package object hadoop{
  type HInt = IntWritable
  type HFloat = FloatWritable
  type HLong = LongWritable
  type Text = org.apache.hadoop.io.Text


  def setKeyValueTypes(job: Job){
    def types(x: Class[_]) = {
      val args = x.getGenericSuperclass.asInstanceOf[ParameterizedType].getActualTypeArguments
      (args(2).asInstanceOf[Class[_]], args(3).asInstanceOf[Class[_]])
    }

    val clazz = job.getReducerClass
    if (clazz != null && job.getMapperClass != null){
      val result = types(clazz)
      job.setOutputKeyClass(result._1)
      job.setOutputValueClass(result._2)

      val m = types(job.getMapperClass)
      job.setMapOutputKeyClass(m._1)
      job.setMapOutputValueClass(m._2)
    } else if(job.getMapperClass != null){
      val result = types(job.getMapperClass)
      job.setOutputKeyClass(result._1)
      job.setOutputValueClass(result._2)
      job.setNumReduceTasks(0)
    }
  }

  val intWritableZero = new HInt(0)
  val intWritableOne = new HInt(1)

  implicit def longToLongWritable(l: Long): HLong = new HLong(l)
  implicit def hLongtoLong(hl: HLong) = hl.get
  implicit def floatWritableUnbox(v: HFloat): Float = v.get
  implicit def floatWritableBox  (v: Float) = new HFloat(v)

  implicit def intToIntWritable(x: Int): HInt = x match {
    case 0 => intWritableZero
    case 1 => intWritableOne
    case _ => new HInt(x)
  }

  def println(x: String) = System.out.println(x)

  implicit def strToText(s: String): Text = new Text(s)
  implicit def txtToStr(t: Text) = t.toString
  implicit def charToText(s: Char): Text = new Text(s.toString)
  implicit def enrichedJob(j: Job) = new EnrichedJob(j)
  implicit def strToPath(s: String) = new Path(s)



  class EnrichedJob(job: Job){

    private[this] def setKeyValueTypes(){
      def types(x: Class[_]) = {
        val args = x.getGenericSuperclass.asInstanceOf[ParameterizedType].getActualTypeArguments
        (args(2).asInstanceOf[Class[_]], args(3).asInstanceOf[Class[_]])
      }

      require(job.getMapperClass != null, "Mapper class cannot be null.")
      //both mapper & reducer present,
      if (job.getReducerClass != null && job.getMapperClass != null){
        val result = types(job.getReducerClass)
        val m = types(job.getMapperClass)
        this.withMapOutputType(result._1, result._2)
            .withOutputType(m._1, m._2)
      } else if(job.getMapperClass != null && job.getReducerClass == null){
        //reducer may be absent
        val result = types(job.getMapperClass)
        this.withOutputType(result._1, result._2)
      }
    }

    def withMapOutputType(keyType: Class[_], valueType: Class[_]) = {
      require(keyType != null); require(valueType != null)
      job.setMapOutputKeyClass(keyType)
      job.setMapOutputValueClass(valueType)
      this
    }

    def withOutputType(keyType: Class[_], valueType: Class[_]) = {
      require(keyType != null); require(valueType != null)
      job.setOutputKeyClass(keyType)
      job.setOutputValueClass(valueType)
      this
    }

    def withInputPaths(paths: Path*) = {
      for (p <- paths){
        require(p != null)
        FileInputFormat.addInputPath(job, p)
      }
      this
    }

    def withOutputPath(path: Path) = {
      require(path != null)
      FileOutputFormat.setOutputPath(job, path)
      this
    }

    def withMapperReducer(mapper: Class[_ <: Mapper[_,_, _, _]], reducer: Class[_ <: Reducer[_, _, _,_]]) = {
      require(mapper != null); require(reducer != null)
      job.setMapperClass(mapper)
      job.setReducerClass(reducer)
      this
    }

    def withOnlyMapper(mapper: Class[_ <: Mapper[_,_, _, _]]) = {
      require(mapper != null)
      job.setMapperClass(mapper)
      job.setNumReduceTasks(0)
      this
    }

    def deduceInputOutputTypes() = {
      setKeyValueTypes()
      this
    }

    def withMainClass(clazz: Class[_]) = {
      job.setJarByClass(clazz)
      this
    }

    def enableCompression() = {
      FileOutputFormat.setCompressOutput(job, true)
      this
    }

    def withInputFormat(clazz: Class[_ <: org.apache.hadoop.mapreduce.InputFormat[_,_]]) = {
      job.setInputFormatClass(clazz)
      this
    }

    def withOutputFormat(clazz: Class[_ <: OutputFormat[_, _]]) = {
      job.setOutputFormatClass(clazz)
      this
    }

    def withCompressor(compressor: Class[_ <: CompressionCodec]) = {
      FileOutputFormat.setOutputCompressorClass(job, compressor)
      this
    }

    def withCompressionType(ctype: CompressionType) = {
      SequenceFileOutputFormat.setOutputCompressionType(job, ctype)
      this
    }

  }



  trait Configured extends Configurable{
    private[this] var conf: Configuration = _
    def getConf: Configuration = conf
    def setConf(conf: Configuration) {
      this.conf = conf
    }
  }

  trait MapReduceHelper[KI, VI, KO, VO] extends Configured{
    import org.apache.hadoop.mapreduce.TaskInputOutputContext
    type ContextType <: TaskInputOutputContext[KI, VI, KO, VO]

    protected[this] var context: ContextType = _
    def key = context.getCurrentKey
    def value = context.getCurrentValue

    def write(key: KO, value: VO){
      context.write(key, value)
    }

    def write(values: Iterable[(KO, VO)]){
      for ((k, v) <- values) {
        context.write(k, v)
      }
    }

    def incrementCounter(name: String): Unit = context.getCounter(getClass.getName, name).increment(1)
    def getCounter(name: String) = context.getCounter(getClass.getName, name).getValue
  }

  abstract class SimpleMapper[KI, VI, KO, VO]
    extends Mapper[KI, VI, KO, VO]
    with MapReduceHelper[KI, VI, KO, VO]{

    type ContextType = Mapper[KI, VI, KO, VO]#Context
    def map: Unit

    override protected[this] def setup(ctx: ContextType){
      this.context = ctx
    }

    override def map(key: KI, value: VI, context: ContextType) {
      map
    }
  }

  abstract class SimpleReducer[KI, VI, KO, VO]
    extends Reducer[KI, VI, KO, VO]
    with MapReduceHelper[KI, VI, KO, VO]{

    import scala.collection.JavaConversions.iterableAsScalaIterable
    import java.lang

    type ContextType = Reducer[KI, VI, KO, VO]#Context
    def reduce: Unit
    def values: Iterable[VI] = this.context.getValues

    override protected[this] def setup(ctx: ContextType){
      this.context = ctx
    }

    override def reduce(key: KI, values: lang.Iterable[VI], context: ContextType) {
      reduce
    }
  }

  abstract class SimpleJobDriver(val mapper: Class[_ <: Mapper[_, _, _, _]] = null,
                                 val reducer: Class[_ <: Reducer[_, _, _, _]] = null)
    extends Configured with App with Tool{

    def process(job: Job){
      job.deduceInputOutputTypes()
    }

    final override def run(args: Array[String]): Int = {
      if (args.length != 2){
        println("Usage: %s <input_dir> <output_dir>".format(getClass.getSimpleName))
        System.exit(-1)
      }

      val job = new Job(getConf)
      job.withInputPaths(args(0))
        .withOutputPath(args(1))
        .withMainClass(getClass)

      job.setJobName("Map reduce written using scala for: %s".format(getClass.getSimpleName))
      if(mapper != null)
        job.setMapperClass(mapper)
      if (reducer != null)
        job.setReducerClass(reducer)

      process(job)
      val success = job.waitForCompletion(true)
      return if(success) 0 else 1
    }

    val exitCode = ToolRunner.run(this, args)
    System.exit(exitCode)
  }
}