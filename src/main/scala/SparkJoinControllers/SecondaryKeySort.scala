package SparkJoinControllers

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.rdd.RDD


/** Secondary Sort
  *
  * customPartitioner := we define our custom partitioner that partitions by the first key's hash value
  * sortedWithinPartitions := we ge
  */
object SecondarySort {

  def groupByKeyAndSortBySecondaryKey[K : Ordering : ClassTag, S : Ordering : ClassTag, V : ClassTag]
                                                (doubleKeyRDD : RDD[((K, S), V)], partitions : Int):
  RDD[(K, List[(S, V)])] = {
    //Create an instance of our custom partitioner
    val customPartitioner = new PrimaryCustomKeyPartitioner[Int, Int](partitions)

    //define an implicit ordering, to order by the second key the ordering will
    //be used even though not explicitly called
    implicit val ordering: Ordering[(K, S)] = Ordering.Tuple2

    val sortedWithinPartitions = doubleKeyRDD.repartitionAndSortWithinPartitions(customPartitioner)
    sortedWithinPartitions.mapPartitions( iter => groupSorted[K, S, V](iter) )
  }

  def groupSorted[K,S,V](it: Iterator[((K, S), V)]): Iterator[(K, List[(S, V)])] = {
    val res = List[(K, ArrayBuffer[(S, V)])]()
    it.foldLeft(res)((list, next) => list match {
      case Nil =>{
        val ((firstKey, secondKey), value) = next
        List((firstKey, ArrayBuffer((secondKey, value))))

      }

      case head :: rest =>
        val (curKey, valueBuf) = head
        val ((firstKey, secondKey), value) = next
        if (!firstKey.equals(curKey) ) {
          (firstKey, ArrayBuffer((secondKey, value))) :: list
        } else {
          valueBuf.append((secondKey, value))
          list
        }

    }).map { case (key, buf) => (key, buf.toList) }.iterator
  }
}

class PrimaryCustomKeyPartitioner[K, S](partitions: Int) extends Partitioner {
  /**
    * We create a hash partitioner and use it with the first set of keys.
    */
  val delegatePartitioner = new HashPartitioner(partitions)

  override def numPartitions = delegatePartitioner.numPartitions

  /** Partition by the hash value of the first key */
  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[(K, S)]
    delegatePartitioner.getPartition(k._1)
  }


}