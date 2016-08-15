package kinesis

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kinesis.KinesisUtils
import org.apache.spark.streaming.{StreamingContext, Seconds}


/**
  * Created by karolsudol on 14/08/2016.
  */
object EventsStats {

  def main(args: Array[String]): Unit = {

    val interval = Seconds(5)

    val spark = new StreamingContext(
      new SparkConf().setMaster("spark://ec2-54-89-117-210.compute-1.amazonaws.com:7077")
        .setAppName("spark-demo")
        .set("spark.streaming.stopGracefullyOnShutdown", "true"), interval)

    val dstream = spark.union(

      (0 until 1).map { _ =>
        KinesisUtils.createStream(spark,
          kinesisAppName = "spark-demo",
          streamName     = "spark-demo",
          endpointUrl    = "https://kinesis.us-east-1.amazonaws.com",
          regionName     = "us-east-1",
          storageLevel   = StorageLevel.MEMORY_AND_DISK,
          checkpointInterval      = interval,
          initialPositionInStream = InitialPositionInStream.LATEST)
      }

    )




    dstream
      .flatMap(new String(_) split ",")
      .map(_ -> 1)
      .reduceByKey(_ + _)
      .print()

    spark.start()
    spark.stop(stopSparkContext=false, stopGracefully=true)



  }



}
