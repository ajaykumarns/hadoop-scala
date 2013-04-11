/**
 * Created with IntelliJ IDEA.
 * User: anadathur
 * Date: 4/7/13
 * Time: 11:03 PM
 * To change this template use File | Settings | File Templates.
 */
package com.anadathur.hadoop
import org.apache.hadoop.mrunit.mapreduce._
import org.junit.Test
import scala.collection.JavaConversions.seqAsJavaList

class WordMapper extends SimpleMapper[HLong, Text, Text, HInt]{
  override def map{
    for( word <- value.split("\\W+").filter(_.length > 0)){
      println(s"Writing word: $word")
      write(word, 1)
    }
  }
}

class SumReducer extends SimpleReducer[Text, HInt, Text, HInt]{
  override def reduce{
    println(s"Writing reduce: $key")
    write(key, values.map(_.get).sum)
  }
}

@Test
class MrUnitTest{
  import org.apache.hadoop.mrunit.mapreduce.MapDriver
  import org.junit.Before

  private[this] val driver = new MapDriver[HLong, Text, Text, HInt]
  val reducerDriver = new ReduceDriver[Text, HInt, Text, HInt]
  val mrDriver = new MapReduceDriver[HLong, Text, Text, HInt, Text, HInt]()

  @Before
  def setUp{
    driver.setMapper(new WordMapper)
    reducerDriver.setReducer(new SumReducer)
    mrDriver.setMapper(new WordMapper)
    mrDriver.setReducer(new SumReducer)
  }

  @Test
  def testMapper{
    driver.withInput(0, "cat cat dog")
      .withOutput("cat", 1)
      .withOutput("cat", 1)
      .withOutput("dog", 1)
      .runTest()
  }

  @Test
  def testReducer{
    reducerDriver.withInput("cat", (1 to 3).map { i => new HInt(1) })
      .withOutput("cat", 3)
      .runTest()
  }

  @Test
  def testMapperAndReducer{
    mrDriver.withInput(0, "cat pounced")
      .withOutput("cat", 1)
      .withOutput("pounced", 1)
      .runTest()
  }
}