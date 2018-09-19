package observatory

import org.scalatest.{FunSuite, Matchers}

trait ExtractionTest extends FunSuite with Matchers {
  test("Stations with no location are ignored") {
    Extraction.parseStations("/stations.csv").count() should equal(4)
  }

  test("Weather stations are identified by the composite (STN, WBAN)") {
    Extraction.locateTemperatures(2000, "/stations.csv", "/2000.csv").foreach(println)
  }
}