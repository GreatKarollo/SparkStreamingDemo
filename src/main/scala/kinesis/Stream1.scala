package kinesis

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{StreamingContext, Milliseconds}
import org.apache.spark.streaming.kinesis.KinesisUtils


/**
  * Created by karolsudol on 15/08/2016.
  */
object Stream1 extends App {

  val streamName = "stream1"
  val appName = "demo1"
  val endpointUrl = "https://kinesis.us-east-1.amazonaws.com"
  val batchInterval = Milliseconds(2000)
  val kinesisCheckpointInterval = batchInterval
  val regionName = RegionUtils.getRegionByEndpoint(endpointUrl).getName()

  val credentials = new DefaultAWSCredentialsProviderChain().getCredentials()
  require(credentials != null,
    "No AWS credentials found. Please specify credentials using one of the methods specified " +
      "in http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html")
  val kinesisClient = new AmazonKinesisClient(credentials)
  kinesisClient.setEndpoint(endpointUrl)
  val numShards = kinesisClient.describeStream(streamName).getStreamDescription().getShards().size
  val numStreams = numShards

  val sparkConfig = new SparkConf()
    .setAppName(appName)
    .setMaster("spark://ec2-54-89-117-210.compute-1.amazonaws.com:7077")
  val ssc = new StreamingContext(sparkConfig, batchInterval)


  // Create the Kinesis DStreams
  val kinesisStreams = (0 until numStreams).map { i =>
    KinesisUtils.createStream(ssc, appName, streamName, endpointUrl, regionName,
      InitialPositionInStream.LATEST, kinesisCheckpointInterval, StorageLevel.MEMORY_AND_DISK_2)
  }

  val unionStreams = ssc.union(kinesisStreams)

  // Convert each line of Array[Byte] to String, and split into words
  val words = unionStreams.flatMap(byteArray => new String(byteArray).split(" "))

  words.print()
  ssc.start()
  wait(10000)
  ssc.stop(stopSparkContext=false, stopGracefully=true)

















}
