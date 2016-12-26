import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhaokangpan on 2016/12/19.
  */
object Kmeans {

  def main(args : Array[String]): Unit ={

    val conf = new SparkConf().setAppName("Kmeans").setMaster("local")
    val sc = new SparkContext(conf)

    // Load and parse the data
    val data = sc.textFile("666.csv")
    val parsedData = data.map(s => {
      val p = s.split(';')
      val array = new Array[Double](6)
     /* for(i <- 0 until 2){
        array(i) = p(1 + i * 3).toDouble + p(2 + i * 3).toDouble + p(3 + i * 3).toDouble
      }*/
      for(i <- 0 until 6){
        array(i) = p(1 + i).toDouble
      }
      //array(0) + "\t" + array(1)
      Vectors.dense(array)
    }).cache()


    // Cluster the data into two classes using KMeans
    val numClusters = 5
    val numIterations = 100
    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    clusters.clusterCenters.foreach(l => println(l))

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)

    // Save and load model
    //clusters.save(sc, "target/org/apache/spark/KMeansExample/KMeansModel")
    //val sameModel = KMeansModel.load(sc, "target/org/apache/spark/KMeansExample/KMeansModel")*/
  }
}
