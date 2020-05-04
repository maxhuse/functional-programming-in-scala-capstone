package observatory

import java.time.LocalDate

import scala.io.Source


/**
  * 1st milestone: data extraction
  */
object Extraction extends ExtractionInterface {

  def toCelsius(t: Temperature): Temperature = (t - 32) * 5 / 9

  def read(path: String): Iterator[String] = {
    val stream = getClass.getResourceAsStream(path)
    Source.fromInputStream(stream, "utf-8").getLines()
  }

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {
    val stationsRaw = read(stationsFile)

    val stations = stationsRaw.map(_.split(",", -1))
      .filter(r => (r(0).nonEmpty || r(1).nonEmpty) && r(2).nonEmpty && r(3).nonEmpty)
      .map(r => (r(0), r(1)) -> Location(r(2).toDouble, r(3).toDouble))
      .toMap

    val temperatureRaw = read(temperaturesFile)

    temperatureRaw.map(_.split(",", -1))
      .filter(r => (r(0).nonEmpty || r(1).nonEmpty) && r(2).nonEmpty && r(3).nonEmpty && r(4).nonEmpty)
      .filter(r => stations.contains((r(0), r(1))))
      .map(r => (LocalDate.of(year, r(2).toInt, r(3).toInt), stations((r(0), r(1))), toCelsius(r(4).toDouble)))
      .toSeq
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    records.map { case (_, location, temperature) => (location, temperature) }
      .groupBy { case (location, _) => location }
      .mapValues(loc => {
        val temperatures = loc.map(_._2)
        temperatures.sum / temperatures.size
      })
  }

}
