package observatory

import java.time.LocalDate

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DataTypes, StructType}

import scala.util.Try

/**
  * 1st milestone: data extraction
  */
object Extraction {
  val stationsTableName = "stations"
  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Observatory")
      .master("local")
      .getOrCreate()
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  import spark.implicits._

  private val stationsSchema = new StructType()
    .add("stn", DataTypes.StringType, nullable = true)
    .add("wban", DataTypes.StringType, nullable = true)
    .add("lat", DataTypes.DoubleType, nullable = false)
    .add("lon", DataTypes.DoubleType, nullable = false)

  private val temperatureSchema = new StructType()
    .add("stn", DataTypes.StringType, nullable = true)
    .add("wban", DataTypes.StringType, nullable = true)
    .add("month", DataTypes.IntegerType, nullable = false)
    .add("day", DataTypes.IntegerType, nullable = false)
    .add("temp", DataTypes.DoubleType, nullable = false)

  final case class StationKey(stn: Option[String], wban: Option[String])
  object StationKey {
    implicit val encoder: Encoder[StationKey] = Encoders.product
  }

  final case class Station(stationKey: StationKey, location: Location)
  object Station {
    implicit val encoder: Encoder[Station] = Encoders.product
  }

  final case class TemperatureLine(stationKey: StationKey, month: Int, day: Int, temperature: Double)
  object TemperatureLine {
    implicit val encoder: Encoder[TemperatureLine] = Encoders.product
  }

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {
    val stationsDs = parseStations(stationsFile)
    val baseTemps = spark.read
      .schema(temperatureSchema)
      .option("mode", "DROPMALFORMED")
      .csv(getClass.getResource(temperaturesFile).getPath)
    baseTemps.show()
    stationsDs.show()
    val temps = baseTemps
      .na.drop(Seq("month", "day", "temp"))
      .map {
        case Row(stn: String, wban: String, month: Int, day: Int, temp: Double) =>
          TemperatureLine(StationKey(Option(stn), Option(wban)), month, day, temp)
        case Row(stn: String, _, month: Int, day: Int, temp: Double) =>
          TemperatureLine(StationKey(Option(stn), None), month, day, temp)
        case Row(_, wban: String, month: Int, day: Int, temp: Double) =>
          TemperatureLine(StationKey(None, Option(wban)), month, day, temp)
      }
    temps
      .joinWith(stationsDs, stationsDs("stationKey") === temps("stationKey"))
      .collect()
      .map {
        case (tempLine, station) =>
          val centigrade = (tempLine.temperature - 32) / 1.8
          val localDate = LocalDate.of(year, tempLine.month, tempLine.day)
          (localDate, station.location, centigrade)
      }
  }

  private[observatory] def parseStations(stationsFile: String): Dataset[Station] = {
    spark.read
      .schema(stationsSchema)
      .option("mode", "DROPMALFORMED")
      .csv(getClass.getResource(stationsFile).getPath)
      .na.drop(Seq("lat", "lon"))
      .map {
        case Row(stn: String, wban: String, lat: Double, lon: Double) =>
          Station(StationKey(Option(stn), Option(wban)), Location(lat, lon))

        case Row(stn: String, _, lat: Double, lon: Double) =>
          Station(StationKey(Option(stn), None), Location(lat, lon))

        case Row(_, wban: String, lat: Double, lon: Double) =>
          Station(StationKey(None, Option(wban)), Location(lat, lon))
      }
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    records
      .groupBy(_._2)
      .par
      .mapValues(_.foldLeft((0.0, 0)){
        case ((sum, count), (_, _, temp)) =>
          (sum + temp, count + 1)
      })
      .mapValues(pair => pair._1 / pair._2)
      .toStream
  }

}
