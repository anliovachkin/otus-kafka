import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.csv.{CsvMapper, CsvSchema}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.util
import java.util.Properties

object Producer extends App {

 val props = new Properties()
 props.put("bootstrap.servers", "localhost:29092")
 props.put("value.serializer", "org.apache.kafka.connect.json.JsonSerializer")
 val producer = new KafkaProducer(props, new StringSerializer, new StringSerializer)

 val jsonMapper = new ObjectMapper() with ScalaObjectMapper
 jsonMapper.registerModule(DefaultScalaModule)

 readBooksFromCsv.forEach(book => sendBook(book))

 private def readBooksFromCsv: util.List[util.LinkedHashMap[String, String]] = {
  val csvMapper = new CsvMapper
  csvMapper
    .readerFor(classOf[util.Map[String, String]])
    .`with`(CsvSchema.emptySchema.withHeader)
    .readValues(this.getClass.getResource("bestsellers_with_categories-1801-9dc31f.csv"))
    .readAll()
 }

 private def sendBook(book: util.LinkedHashMap[String, String]) = {
  producer.send(new ProducerRecord("books", jsonMapper.writeValueAsString(book)))
 }

 producer.close()
}
