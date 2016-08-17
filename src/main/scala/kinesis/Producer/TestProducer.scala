package kinesis.Producer

/**
  * Created by karolsudol on 16/08/2016.
  */
object TestProducer  extends App{

  if (args.length != 4) {
    System.err.println(

      """
        |Usage: KinesisProducerASL <stream-name> <endpoint-url> <records-per-sec>
                                         <records-per-record>
        |
        |    <stream-name> is the name of the Kinesis stream
        |    <endpoint-url> is the endpoint of the Kinesis service
        |                   (e.g. https://kinesis.us-east-1.amazonaws.com)
        |    <records-per-sec> is the rate of records per second to put onto the stream
        |    <records-per-record> is the rate of records per second to put onto the stream
        |
      """.stripMargin)
    System.exit(1)
  }






}
