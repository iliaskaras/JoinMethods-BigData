import org.apache.spark.{SparkConf, SparkContext}
import SparkJoinControllers.JoinController
import DAO.FileWritter
import org.apache.spark

/* Join operation in spark is similar to relation database operation INNER JOIN */
object SparkJoinProject {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]")
      .setAppName("Scala Programming").setSparkHome("src/main/resources")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val smallFile = sc.textFile("src/main/resources/SmallFile.csv")
    val largeFile = sc.textFile("src/main/resources/LargeFile.csv")

    var joinController = new JoinController()
    var fileWritter = new FileWritter()

    fileWritter.initializeEmptyFile("src/main/resources/times.txt")

    /* ============================= Large File Joins! ============================================================== */

    fileWritter.appendFile("src/main/resources/times.txt","\n Large file times : \n\n")

    /* ============================= Simple Join with RDDs 1 ======================================================== */
    joinController.simpleJoin(largeFile,"largeFileSimpleJoin","LargeFile")

    /* =========================== Simple Join with DataSets 2 =======================================================*/
    joinController.simpleJoinDataset(largeFile,"largeFileDatasetSimpleoin",sc,"LargeFile")

    /* =========================== SQL Join with DataFrames 3 ========================================================*/
    joinController.sqlJoinDataFrames(largeFile,"largeFileDataFramesSQLJoin",sc,"LargeFile")

    /* ============================= Manually BroadcastHash Join 4 ===================================================*/
    joinController.broadCastHashJoin(largeFile,"largeFileBroadcastHashJoin","LargeFile")

    /* ===================== BroadcastHash Join With Secondary Key Sorted 5 ==========================================*/
    joinController.broadCastHashJoinSortedIntValues(largeFile,"largeFileRepartitionAnJoinByPartition","LargeFile")



    /* ============================= Small File Joins! ============================================================== */

    fileWritter.appendFile("src/main/resources/times.txt","\n Small file times : \n\n")

    /* ============================= Simple Join with RDDs 1 ======================================================== */
    joinController.simpleJoin(smallFile,"smallFileSimpleJoin","SmallFile")

    /* ============================ Simple Join with DataSets 2 ======================================================*/
    joinController.simpleJoinDataset(smallFile,"smallFileDatasetSimpleJoin",sc,"SmallFile")

    /* =========================== SQL Join with DataFrames 3 ========================================================*/
    joinController.sqlJoinDataFrames(smallFile,"smallFileDataFramesSQLJoin",sc,"SmallFile")

    /* ============================= BroadcastHash Join 4 ============================================================*/
    joinController.broadCastHashJoin(smallFile,"smallFileBroadcastHashJoin","SmallFile")

    /* ===================== BroadcastHash Join With Secondary Key Sorted 5 ==========================================*/
    joinController.broadCastHashJoinSortedStringValues(smallFile,"smallFileRepartitionAnJoinByPartition","SmallFile")

  }


}

