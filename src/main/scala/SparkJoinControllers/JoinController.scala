package SparkJoinControllers

import org.apache.spark.{SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import scala.reflect.ClassTag
import scala.collection.Map
import collection.mutable._
import DAO.FileWritter

/** Class that controls the flow of joining operations
  * Communicates with FileWritter to write the results to files
  * Keep track of timers */
class JoinController() extends Serializable {
  val fileWritter = new FileWritter()

  def removeUnwantedTuples(x: Int, smallRDD:  Map[Int, String]) = Some(x).filter(smallRDD.contains)


  /** simpleJoin is a method that gets as input the unprocessed RDD from input File, and the name of the output File.
    * Prepossess of RDDs : First we create tuples of form (key,value) where =>
    * key := second column of file, and the joining factor in the join process,
    * value := FirstColumn ( Third column values ), where =>
    * FirstColumn is the identifier of Rows := R or S,
    * Third column values are all the values of the tuple's key for the specific
    *     identify R or S, for example := (Z, X) "small file's example for S identifier"
    *
    * Full example of Tuple's form : (2,(S(Z, X)), meaning for key 2 and identifier S we have grouped values Z and X.
    *
    * Full example of Simple join's tuple : (2,(S(Z, X),R(a, g)))
    *
    * The final output is a groupByKey result of the above tuples
    *
    * S(Z, X),R(a, g) in an uncompressed form are 2x2=4 rows of tuples,
    * *) (2,((S,Z),(R,a))) & (2,((S,Z),(R,g))) & (2,((S,X),(R,a))) & (2,((S,X),(R,g)))
    *    : (4 tuples in the full unprocessed join form)
    *
    * Our way of reducing the values size in our tuples is important for when the shuffles occurs.
    * One important think that needs to be said is that in our examples both of our files are not large,
    *   especially after our changes in the values, so using groupByKey doesn't bring that much of a shuffle over
    *
    *
    * For the exercise's purpose we wont sort by values, that will be fully be
    *    covered in question 5 by implementing repartitionAndSortWithinPartitions
    *
    *
    * Also we choose the smaller RDD, get it's distinct keys and filter out
    *    all keys from larger RDD that will not end up in the joining result
    *    (meaning they are different from the distinct keys of smaller RDD)
    * */

  def simpleJoin(inputFileName : RDD[String], outputFileName : String, inputFileNameString : String)= {

    val smallRDD_S = inputFileName.map(x => (x.split(",")(1).toInt,(x.split(",")(0),x.split(",")(2)).toString())).
      filter(e => e._2.contains("S")).
      map(x => (x._1,x._2.toString.replace("S,","").replace("(","").replace(")",""))).
      sortByKey(ascending = true).
      groupByKey().
      map(x => (x._1,"S"+x._2.toString.replace("List","").replace("CompactBuffer","")))
      .cache()

    val smallRDD_S_map: Map[Int, (String)] = smallRDD_S.collectAsMap()

    val smallRDD_R = inputFileName.map(x => (x.split(",")(1).toInt,(x.split(",")(0),x.split(",")(2)))).
      filter(e => e._2.toString().contains("R")).
      map(x => (x._1,x._2.toString.replace("R,","").replace("(","").replace(")",""))).
      filter(e => removeUnwantedTuples(e._1,smallRDD_S_map) != None).
      sortByKey(ascending = true).
      groupByKey().
      map(x => (x._1,"R"+x._2.toString.replace("List","").replace("CompactBuffer","")))
      .cache()

    val runRDDS2 = smallRDD_S.take(1)
    val runRDDR2 = smallRDD_R.take(1)

    println("Time for Simple Join of RDD_S with RDD_R (Question 1) of file "+inputFileNameString+" is : \n")

    val simpleJoin: RDD[(Int, (String, String))] = time{smallRDD_S.join(smallRDD_R)}

    println("Count for Simple Join of RDD_S with RDD_R (Question 1) of file "+simpleJoin.count())

    val finalSimpleJoinResult : RDD[(Int, (String, String))] =  simpleJoin.sortByKey(ascending = true)

    fileWritter.saveToOutputFile(finalSimpleJoinResult,outputFileName)
    fileWritter.appendFile("src/main/resources/times.txt","for Simple join RDD result count : "+finalSimpleJoinResult.count().toString+".\n")

    smallRDD_S.unpersist()
    smallRDD_R.unpersist()
    finalSimpleJoinResult.unpersist()
  }

  /** simpleJoinDataset is a method that creates R_Ds and S_Ds Datasets with columns Key and Value_R & Value_S respectively
    * and uses the simple join method with joining key column name the "Key"
    *
    * The preprocess of RDDs is the same as the question's one.
    * */
  def simpleJoinDataset(inputFileName : RDD[String], outputFileName : String, sc : SparkContext, inputFileNameString : String): Unit ={

    val smallRDD_S_BcJoin = inputFileName.map(x => (x.split(",")(1).toInt,(x.split(",")(0),x.split(",")(2)).toString())).
      filter(e => e._2.toString().contains("S")).
      map(x => (x._1,x._2.toString.replace("S,","").replace("(","").replace(")",""))).
      sortByKey(ascending = true).
      groupByKey().
      map(x => (x._1,"S"+x._2.toString.replace("List","").replace("CompactBuffer","")))
      .cache()

    val smallRDD_S_BcJoinMap: Map[Int, String] = smallRDD_S_BcJoin.collectAsMap()

    val smallRDD_R_BcJoin = inputFileName.map(x => (x.split(",")(1).toInt,(x.split(",")(0),x.split(",")(2)).toString())).
      filter(e => e._2.toString().contains("R")).
      map(x => (x._1,x._2.toString.replace("R,","").replace("(","").replace(")",""))).
      filter(e => removeUnwantedTuples(e._1, smallRDD_S_BcJoinMap).isDefined).
      sortByKey(ascending = true).
      groupByKey().
      map(x => (x._1,"R"+x._2.toString.replace("List","").replace("CompactBuffer","")))
      .cache()

    val ss = SparkSession.builder().master("local[*]").appName("Scala Programming").getOrCreate()

    import ss.implicits._

    val R_Ds = ss.createDataset(smallRDD_R_BcJoin).selectExpr("_1 as Key","_2 as Value_R")
    val S_Ds = ss.createDataset(smallRDD_S_BcJoin).selectExpr("_1 as Key","_2 as Value_S")

    println("Time for Simple Join Datasets of Dataset S with Dataset R (Question 2) of file "+inputFileNameString+" is : \n")
//    val Ds_Join = time{ S_Ds.join(broadcast(R_Ds),"Key") }
    val Ds_Join = time{ S_Ds.join(R_Ds,"Key") }

    println("Count Simple Join Datasets of Dataset S with Dataset R (Question 2) of file "+Ds_Join.count())

    val sortedDsJoin = Ds_Join.sort("Key")

    val repartitioned = sortedDsJoin.repartition(1)
    repartitioned.write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").option("header", "true").save("src/main/resources/"+outputFileName)

    fileWritter.appendFile("src/main/resources/times.txt","for Simple join Dataset result count : "+sortedDsJoin.count().toString+".\n")

    smallRDD_S_BcJoin.unpersist()
    smallRDD_R_BcJoin.unpersist()
    R_Ds.unpersist()
    S_Ds.unpersist()
    ss.sqlContext.clearCache()
  }

  /** sqlJoinDataFrames is a method that creates R_Df and S_Df Dataframes with columns Key and Value_R & Value_S respectively
    * Register those Dataframes as SQL temporary view, and then join them with SQL query, with joining factor the equality
    *   of R_Df.key and S_Df.key
    *
    * The preprocess of RDDs is the same as the question's one.
    * */

  def sqlJoinDataFrames(inputFileName : RDD[String], outputFileName : String, sc : SparkContext, inputFileNameString : String): Unit ={

    val rdd_S = inputFileName.map(x => (x.split(",")(1).toInt,(x.split(",")(0),x.split(",")(2)).toString())).
      filter(e => e._2.toString().contains("S")).
      map(x => (x._1,x._2.toString.replace("S,","").replace("(","").replace(")",""))).
      sortByKey(ascending = true).
      groupByKey().
      map(x => (x._1,"S"+x._2.toString.replace("List","").replace("CompactBuffer","")))
      .cache()

    val mappedRDD_S: Map[Int, String] = rdd_S.collectAsMap()

    val rdd_R = inputFileName.map(x => (x.split(",")(1).toInt,(x.split(",")(0),x.split(",")(2)).toString())).
      filter(e => e._2.toString().contains("R")).
      map(x => (x._1,x._2.toString.replace("R,","").replace("(","").replace(")",""))).
      filter(e => removeUnwantedTuples(e._1, mappedRDD_S).isDefined).
      sortByKey(ascending = true).
      groupByKey().
      map(x => (x._1,"R"+x._2.toString.replace("List","").replace("CompactBuffer","")))
      .cache()

    val ss = SparkSession.builder().master("local[*]").appName("Scala Programming").getOrCreate()

    import ss.implicits._

    val R_Df =rdd_R.toDF().withColumnRenamed("_1","Key").withColumnRenamed("_2","Value_R")
    val S_Df =rdd_S.toDF().withColumnRenamed("_1","Key").withColumnRenamed("_2","Value_S")

    val countR = R_Df.count()
    val countS = S_Df.count()

    // Register the DataFrame as a SQL temporary view
    R_Df.createOrReplaceTempView("file_R")
    S_Df.createOrReplaceTempView("file_S")


    println("Time for Simple Join Dataframes with SQL of Dataframe S with Dataframe R (Question 3) of file "+inputFileNameString+" is : \n")

    val joinedDf_Result = time{ss.sql("SELECT r.key,s.value_s,r.value_r " +
                                      "FROM file_S s JOIN file_R r " +
                                      "ON r.key = s.key " +
                                      "ORDER BY r.key")}

    println("Count for Simple Join Dataframes with SQL of Dataframe S with Dataframe R (Question 3) of file "+joinedDf_Result.count())

    val repartitioned = joinedDf_Result.repartition(1)
    repartitioned.write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").option("header", "true").save("src/main/resources/"+outputFileName)

    fileWritter.appendFile("src/main/resources/times.txt","for SQL join Dataframe result count : "+joinedDf_Result.count().toString+".\n")

    rdd_S.unpersist()
    rdd_R.unpersist()
    R_Df.unpersist()
    S_Df.unpersist()
    ss.sqlContext.uncacheTable("file_R")
    ss.sqlContext.uncacheTable("file_S")
    ss.sqlContext.clearCache()
  }


  /** broadCastHashJoin is a method that broadcast one of the two RDDs, usually the smaller but in our example both files
    * gives around the same size, but RDD_S gives a little bit smaller, to the larger RDD, and finally we are using
    * mapPartitions to combine the tuples based on their equality of Keys.
    * The preprocess of RDDs is the same as the question's one.
    *
    * The actual execution of the join executes via calling executeBroadCastHashJoin inside broadCastHashJoin, getting
    * the rdd's as parameters.
    * */


  def broadCastHashJoin(inputFileName : RDD[String], outputFileName : String, inputFileNameString : String) : Unit ={
    //   Broadcast join between Small File R and S //
    val rdd_S = inputFileName.map(x => (x.split(",")(1).toInt,(x.split(",")(0),x.split(",")(2)).toString())).
      filter(e => e._2.toString().contains("S")).
      map(x => (x._1,x._2.toString.replace("S,","").replace("(","").replace(")",""))).
      sortByKey(ascending = true).
      groupByKey().
      map(x => (x._1,"S"+x._2.toString.replace("List","").replace("CompactBuffer","")))
      .cache()

    val mappedRDD_S: Map[Int, String] = rdd_S.collectAsMap()

    val rdd_R = inputFileName.map(x => (x.split(",")(1).toInt,(x.split(",")(0),x.split(",")(2)).toString())).
      filter(e => e._2.toString().contains("R")).
      map(x => (x._1,x._2.toString.replace("R,","").replace("(","").replace(")",""))).
      filter(e => removeUnwantedTuples(e._1, mappedRDD_S).isDefined).
      sortByKey(ascending = true).
      groupByKey().
      map(x => (x._1,"R"+x._2.toString.replace("List","").replace("CompactBuffer","")))
      .cache()

    val runRDDS2 = rdd_S.take(1)
    val runRDDR2 = rdd_R.take(1)

    println("Time for BroadCastHashJoin between RDDs (Question 4), of file "+inputFileNameString+" is : \n")

    val finalBroadCastResult = executeBroadCastHashJoin(rdd_S,rdd_R).sortByKey(ascending = true)

    println("Count for BroadCastHashJoin between RDDs (Question 4), of file "+finalBroadCastResult.count())

    fileWritter.appendFile("src/main/resources/times.txt","for BroadcastHash Join, RDD result count : "+finalBroadCastResult.count().toString+".\n")

    fileWritter.saveToOutputFile2(finalBroadCastResult,outputFileName)

    rdd_S.unpersist()
    rdd_R.unpersist()
  }

  def executeBroadCastHashJoin[K : Ordering : ClassTag, V1 : ClassTag](rdd_S : RDD[(K, String)],
                                                                       rdd_R : RDD[(K, String)])
  : RDD[(K, (String))] = {

    val mapRDD_S : Map[K, String] = rdd_S.groupByKey.mapValues(_.toList.mkString("")).collectAsMap()
    val groupByKeyRDD_R : RDD[(K, String)] = rdd_R.groupByKey.mapValues(_.toList.mkString(""))

    val flag1 = mapRDD_S.take(1)
    val flag2 = groupByKeyRDD_R.take(1)

    time{
      val smallRDDLocalBcast = groupByKeyRDD_R.sparkContext.broadcast(mapRDD_S)

      groupByKeyRDD_R.mapPartitions(iter => {
        iter.flatMap{

          case (k,v1 ) =>
            smallRDDLocalBcast.value.get(k) match {
              case None => Seq.empty[(K, String)]
              case Some(v2) => {
                Seq((k, (v2, v1).toString()))}
            }
        }
      }, preservesPartitioning = true)
    }

  }

  /** broadCastHashJoinSortedStringValues and broadCastHashJoinSortedIntValues are methods that have as a result sorted
    * values. The first method is for executing files that need to sort values contains Strings, second is for sorting
    * values containing Ints.
    *
    * The preprocess of RDD's is different here, we create double key tuples, and sending them to groupByKeyAndSortBySecondaryKey
    * of SecondaryKeySort Object. There repartitionAndSortWithinPartitions is used combined with a manually created
    * Partitioner and an implicit ordering of the tuples to sort the second key.
    *
    * The actual execution of the join executes via calling executeBroadCastHashJoin inside broadCastHashJoinWithSecondarySorting,
    * getting the rdd's as parameters but this time with sorted values.
    * */

  def broadCastHashJoinSortedStringValues(inputFileName : RDD[String], outputFileName : String, inputFileNameString : String) : Unit ={

    val DoubleKey_RDD_R = inputFileName.map(x => (x.split(",")(1).toInt,x.split(",")(2),x.split(",")(0))).
      filter(e => e._3.contains("R")).
      map(x => ((x._1,x._2),x._3))
      .cache()

    val secondaryKeySortedRDD_R = SecondarySort.groupByKeyAndSortBySecondaryKey(DoubleKey_RDD_R,2)

    val toJoinRDD_R = secondaryKeySortedRDD_R.
      map(x => (x._1,x._2.toString.replace("(","").replace(")","").replace(",","").replace("R",""))).
      groupByKey().
      map(x => (x._1,x._2.toString.replace("List","").replace("CompactBuffer",""))).
      map(x => (x._1,"R"+x._2)).
      sortByKey(ascending = true)
      .cache()

    val DoubleKey_RDD_S = inputFileName.map(x => (x.split(",")(1).toInt,x.split(",")(2),x.split(",")(0))).
      filter(e => e._3.contains("S")).
      map(x => ((x._1,x._2),x._3))
      .cache()

    val secondaryKeySortedRDD_S = SecondarySort.groupByKeyAndSortBySecondaryKey(DoubleKey_RDD_S,2)

    val toJoinRDD_S = secondaryKeySortedRDD_S.
      map(x => (x._1,x._2.toString.replace("(","").replace(")","").replace(",","").replace("S",""))).
      groupByKey().
      map(x => (x._1,x._2.toString.replace("List","").replace("CompactBuffer",""))).
      map(x => (x._1,"S"+x._2)).
      sortByKey(ascending = true)
      .cache()

    val runRDDS = toJoinRDD_S.count()
    val runRDDR = toJoinRDD_R.count()

    println("Time for BroadCastHashJoin between RDDs with Secondary Sorting, of file "+inputFileNameString+" is : \n")

    val finalJoinedRDD_S_R = broadCastHashJoinWithSecondarySorting(toJoinRDD_S,toJoinRDD_R).sortByKey(ascending = true)

    println("Count for BroadCastHashJoin between RDDs with Secondary Sorting, of file "+finalJoinedRDD_S_R.count())

    fileWritter.appendFile("src/main/resources/times.txt","for BroadcastHash Join with Secondary Sort, RDD result count : "+finalJoinedRDD_S_R.count().toString+".\n")

    fileWritter.saveToOutputFile2(finalJoinedRDD_S_R,outputFileName)

    toJoinRDD_R.unpersist()
    toJoinRDD_S.unpersist()
  }

  def broadCastHashJoinSortedIntValues(inputFileName : RDD[String], outputFileName : String, inputFileNameString : String) : Unit ={
    val DoubleKey_RDD_R = inputFileName.map(x => (x.split(",")(1).toInt,x.split(",")(2).toInt,x.split(",")(0))).
      filter(e => e._3.contains("R")).
      map(x => ((x._1,x._2),x._3))
      .cache()

    val secondaryKeySortedRDD_R = SecondarySort.groupByKeyAndSortBySecondaryKey(DoubleKey_RDD_R,2)

    val toJoinRDD_R = secondaryKeySortedRDD_R.
      map(x => (x._1,x._2.toString.replace("(","").replace(")","").replace(",","").replace("R",""))).
      groupByKey().
      map(x => (x._1,x._2.toString.replace("List","").replace("CompactBuffer",""))).
      map(x => (x._1,"R"+x._2)).
      sortByKey(ascending = true).cache()

    val DoubleKey_RDD_S = inputFileName.map(x => (x.split(",")(1).toInt,x.split(",")(2).toInt,x.split(",")(0))).
      filter(e => e._3.contains("S")).
      map(x => ((x._1,x._2),x._3))
      .cache()

    val secondaryKeySortedRDD_S = SecondarySort.groupByKeyAndSortBySecondaryKey(DoubleKey_RDD_S,2)

    val toJoinRDD_S = secondaryKeySortedRDD_S.
      map(x => (x._1,x._2.toString.replace("(","").replace(")","").replace(",","").replace("S",""))).
      groupByKey().
      map(x => (x._1,x._2.toString.replace("List","").replace("CompactBuffer",""))).
      map(x => (x._1,"S"+x._2)).
      sortByKey(ascending = true)
      .cache()

    val runRDDS = toJoinRDD_S.count()
    val runRDDR = toJoinRDD_R.count()

    println("Time for BroadCastHashJoin between RDDs with Secondary Sorting (Question 5), of file "+inputFileNameString+" is : \n")

    val finalJoinedRDD_S_R = broadCastHashJoinWithSecondarySorting(toJoinRDD_S,toJoinRDD_R).sortByKey(ascending = true)

    println("Count for BroadCastHashJoin between RDDs with Secondary Sorting (Question 5), of file "+finalJoinedRDD_S_R.count())

    fileWritter.appendFile("src/main/resources/times.txt"," for BroadcastHash Join with Secondary Sort, RDD result count : "+finalJoinedRDD_S_R.count().toString+".\n")

    fileWritter.saveToOutputFile2(finalJoinedRDD_S_R,outputFileName)

    toJoinRDD_R.unpersist()
    toJoinRDD_S.unpersist()
  }

  def broadCastHashJoinWithSecondarySorting[K : Ordering : ClassTag](rdd_S_DistinctKeyList : RDD[(K, String)],
                                                                     rdd_R : RDD[(K, String)])
  : RDD[(K, (String))] = {

    val mappedRDD_S : Map[K, String] = rdd_S_DistinctKeyList.groupByKey.mapValues(_.toList.mkString("")).collectAsMap()
    val mappedRDD_R : RDD[(K, String)] = rdd_R.groupByKey.mapValues(_.toList.mkString(""))

    val flag1 = mappedRDD_S.take(1)
    val flag2 = mappedRDD_R.take(1)

    time{
      val smallRDDLocalBcast = mappedRDD_R.sparkContext.broadcast(mappedRDD_S)

      mappedRDD_R.mapPartitions(iter => {
        iter.flatMap{

          case (k,v1 ) =>
            smallRDDLocalBcast.value.get(k) match {
              case None => Seq.empty[(K, String)]
              case Some(v2) => {
                Seq((k, (v2, v1).toString()))}
            }
        }
      }, preservesPartitioning = true)
    }

  }


  /** Keeps time in ms of what we write in its block */
  def time[R](block: => R): R = {
    val t0 = System.currentTimeMillis()
    val result = block    // call-by-name
    val t1 = System.currentTimeMillis()
    println("Elapsed time: " + (t1 - t0) + "ms")
    val timeToWrite = "\t Time:"+(t1 - t0).toString + "ms , "
    fileWritter.appendFile("src/main/resources/times.txt",timeToWrite)
    result
  }


}
