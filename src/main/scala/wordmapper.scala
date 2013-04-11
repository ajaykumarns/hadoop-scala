/**
 * Created with IntelliJ IDEA.
 * User: anadathur
 * Date: 4/3/13
 * Time: 5:06 PM
 * To change this template use File | Settings | File Templates.
 */
package com.anadathur.hadoop
import scala.collection.JavaConversions.iterableAsScalaIterable
import org.apache.hadoop.mapreduce.{Partitioner, Job}
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat
import org.apache.hadoop.io.{WritableComparable, WritableComparator}


class WordMapper extends SimpleMapper[HLong, Text, Text, HInt]{
  override def map =
    this.value.split("\\W+").filter(_.length > 0).foreach { str: String =>
      write(new Text(str), 1)
  }
}

class WordReducer extends SimpleReducer[Text, HInt, Text, HInt]{
  override def reduce = write(key, values.map(_.get).sum)
}

object WordCount extends SimpleJobDriver(classOf[WordMapper], classOf[WordReducer])

class AvgWordLenMapper extends SimpleMapper[HLong, Text, Text, HLong]{
  override def map {
    value.split("\\W+")
      .filter(_.length > 0)
      .foreach { str: String => write(str(0).toUpper, new HLong(str.length))}
  }
}

class AvgWordLenReducer extends SimpleReducer[Text, HInt, Text, HFloat]{
  override def reduce {
    def avg(iter : Iterator[HInt], acc: Float = 0, count: Int = 0): HFloat = {
      if (!iter.hasNext){
        if (count != 0) acc/count
        else 0
      } else {
        avg(iter, acc + iter.next.get, count + 1)
      }
    }
    write(key, avg(values.iterator))
  }
}

object WordLenJob extends SimpleJobDriver(classOf[AvgWordLenMapper], classOf[AvgWordLenReducer])



class SSMapper extends SimpleMapper[Text, Text, Text, HInt]{
  override def map = write(key + "#" + value, Integer.parseInt(value.trim))
}

class SSReducer extends SimpleReducer[Text, HInt, Text, HInt]{
  override def reduce = write(key/*.split("#")(0)*/, values.head)
}

class SSComparator extends WritableComparator(classOf[Text], true){
  import SS.asTuple
  override def compare(a: WritableComparable[_], b: WritableComparable[_]): Int = {
    val (txt1, txt2) = (a.asInstanceOf[Text], b.asInstanceOf[Text])
    val (s1, i1) = asTuple(txt1)
    val (s2, i2) = asTuple(txt2)
    if(s1 == s2){
      if (i1 == i2) 0
      else if (i1 > i2) -1
      else 1
    } else {
      s1.compareTo(s2)
    }
  }
}

class SSGrouper extends WritableComparator(classOf[Text], true){
  import SS.asTuple
  override def compare(a: WritableComparable[_], b: WritableComparable[_]): Int = {
    val (txt1, txt2) = (a.asInstanceOf[Text], b.asInstanceOf[Text])
    val (s1, i1) = asTuple(txt1)
    val (s2, i2) = asTuple(txt2)
    s1.compareTo(s2)
  }
}

class SSPartitioner extends Partitioner[Text, HInt]{
  override def getPartition(key: Text, value: HInt, numOfParts: Int): Int =
    Math.abs(key.split("#")(0)(0).toInt) % numOfParts
}

object SS extends SimpleJobDriver(classOf[SSMapper], classOf[SSReducer]){
  def asTuple(txt: String) = {
    val parts = txt.split("#")
    (parts(0).trim, parts(1).trim.toInt)
  }

  override def process(job: Job) {
    super.process(job)
    job.getConfiguration.set("key.value.separator.in.input.line", " ")
    job.setPartitionerClass(classOf[SSPartitioner])
    job.setInputFormatClass(classOf[KeyValueTextInputFormat])
    job.setSortComparatorClass(classOf[SSComparator])
    job.setGroupingComparatorClass(classOf[SSGrouper])
    job.setNumReduceTasks(2)
  }
}
