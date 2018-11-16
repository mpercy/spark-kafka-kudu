package org.apache.kudu

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kudu.spark.kudu.{KuduContext, KuduWriteOptions}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/** Case class coerceable to the Kudu schema for the destination table */
case class KuduMetricsRecord(host: String, metric: String, context: String,
                             tsInEpochMillis: Long, value: Long)

case class AppOptions(kuduMasterAddresses: String,
                      kafkaBootstrapServers: String,
                      kuduTableName: String,
                      kafkaTopic: String,
                      sparkAppName: String)

object App {

  /** Parse a KuduMetricsRecord from a tab-separated record */
  def parseRecordFromLine(s: String): KuduMetricsRecord = {
    val fields = s.stripLineEnd.split("\t")
    KuduMetricsRecord(fields(0), fields(1), fields(2),
                      fields(3).toLong, fields(4).toLong)
  }
  
  def startStream(opts: AppOptions): Unit = {

    // TODO: Do we need to setMaster() here?
    val sparkConf = new SparkConf().setAppName(opts.sparkAppName)
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> opts.kafkaBootstrapServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // Set up an InputDStream of Kafka ConsumerRecords.
    val stream = KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Array(opts.kafkaTopic), kafkaParams)
    )

    // Set up our Kudu connection.
    val kuduContext = new KuduContext(opts.kuduMasterAddresses)

    // This allows us to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    // Operate on the stream in automatically-segmented microbatches.
    // For each microbatch, transform from Kafka format to a format insertable
    // by the Kudu Spark connector. For this schema, we use KuduMetricsRecord.
    stream.foreachRDD { rdd =>
      val records = ArrayBuffer.empty[KuduMetricsRecord]
      rdd.foreach {
        consumerRecord => records.append(parseRecordFromLine(consumerRecord.value))
      }
      if (records.nonEmpty) {
        // Convert the parsed records to a DataFrame using reflection and the sqlContext
        // implicits imported above.
        val recordsRDD = sc.parallelize(records)
        val recordsDF = recordsRDD.toDF

        // Insert our transformed DataFrame into the Kudu table. Here we ignore
        // duplicate keys at insert time, although we could use upsertRows() to
        // instead overwrite any existing records.
        kuduContext.insertRows(recordsDF, opts.kuduTableName,
                               new KuduWriteOptions(ignoreDuplicateRowErrors = true))
      }
    }
  }
  
  def main(args : Array[String]) {
    // TODO(mpercy): configure this to parse command line options or something.
    val kuduMasterAddresses = "vc1320.halxg.cloudera.com"
    val kafkaBootstrapServers = "vc1320.halxg.cloudera.com"
    val kuduTableName = "impala::default.kudu-metrics"
    val kafkaTopic = "kudu-metrics"
    val sparkAppName = "kudu-metrics"

    val opts = AppOptions(kuduMasterAddresses, kafkaBootstrapServers,
                          kuduTableName, kafkaTopic, sparkAppName)

    startStream(opts)
  }

}
