package eu.neverblink.jelly.cli.util

import eu.ostrzyciel.jelly.core.IoUtils
import eu.ostrzyciel.jelly.core.proto.v1.{RdfStreamFrame, RdfStreamOptions}

import java.io.{InputStream, OutputStream}
import scala.collection.mutable.ListBuffer

class FrameInfo:
  var metadata: Map[String, String] = Map.empty
  var optionCount: Int = 0
  var nameCount: Int = 0
  var namespaceCount: Int = 0
  var tripleCount: Int = 0
  var quadCount: Int = 0
  var prefixCount: Int = 0
  var datatypeCount: Int = 0
  var graphStartCount: Int = 0
  var graphEndCount: Int = 0

end FrameInfo

class MetricsPrinter:

  var printOptions: Option[RdfStreamOptions] = None
  var frameInfo: ListBuffer[FrameInfo] = ListBuffer.empty

  def printPerFrame(o: OutputStream): Unit = {
    val parsedString = YamlDocBuilder.build(YamlDocBuilder.YamlList(frameInfo.map { frame =>
      YamlDocBuilder.YamlMap(
        "optionCount" -> YamlDocBuilder.YamlString(frame.optionCount.toString),
        "nameCount" -> YamlDocBuilder.YamlString(frame.nameCount.toString),
        "namespaceCount" -> YamlDocBuilder.YamlString(frame.namespaceCount.toString),
        "tripleCount" -> YamlDocBuilder.YamlString(frame.tripleCount.toString),
        "quadCount" -> YamlDocBuilder.YamlString(frame.quadCount.toString),
        "prefixCount" -> YamlDocBuilder.YamlString(frame.prefixCount.toString),
        "datatypeCount" -> YamlDocBuilder.YamlString(frame.datatypeCount.toString),
        "graphStartCount" -> YamlDocBuilder.YamlString(frame.graphStartCount.toString),
        "graphEndCount" -> YamlDocBuilder.YamlString(frame.graphEndCount.toString),
      )
    }.toSeq))
    o.write(parsedString.getBytes)

  }

  def printAggregate(o: OutputStream): Unit = {
    val sumCounts = frameInfo.reduce((a, b) => {
      a.optionCount += b.optionCount
      a.nameCount += b.nameCount
      a.namespaceCount += b.namespaceCount
      a.tripleCount += b.tripleCount
      a.quadCount += b.quadCount
      a.prefixCount += b.prefixCount
      a.datatypeCount += b.datatypeCount
      a.graphStartCount += b.graphStartCount
      a.graphEndCount += b.graphEndCount
      a
    })
    val parsedString = YamlDocBuilder.build(
      YamlDocBuilder.YamlMap(
        "optionCount" -> YamlDocBuilder.YamlString(sumCounts.optionCount.toString),
        "nameCount" -> YamlDocBuilder.YamlString(sumCounts.nameCount.toString),
        "nameSpaceCount" -> YamlDocBuilder.YamlString(sumCounts.namespaceCount.toString),
        "tripleCount" -> YamlDocBuilder.YamlString(sumCounts.tripleCount.toString),
        "quadCount" -> YamlDocBuilder.YamlString(sumCounts.quadCount.toString),
        "prefixCount" -> YamlDocBuilder.YamlString(sumCounts.prefixCount.toString),
        "datatypeCount" -> YamlDocBuilder.YamlString(sumCounts.datatypeCount.toString),
        "graphStartCount" -> YamlDocBuilder.YamlString(sumCounts.graphStartCount.toString),
        "graphEndCount" -> YamlDocBuilder.YamlString(sumCounts.graphEndCount.toString),
      ),
    )
    o.write(parsedString.getBytes)
  }

end MetricsPrinter

object MetricsPrinter:

end MetricsPrinter

object JellyUtil:
  /** This method reads the Jelly file and returns an iterator of RdfStreamFrame
    *
    * @param inputStream
    * @param outputStream
    * @return
    */
  def iterateRdfStream(
      inputStream: InputStream,
  ): Iterator[RdfStreamFrame] =
    IoUtils.autodetectDelimiting(inputStream) match
      case (false, newIn) =>
        // Non-delimited Jelly file
        // In this case, we can only read one frame
        Iterator(RdfStreamFrame.parseFrom(newIn))
      case (true, newIn) =>
        // Delimited Jelly file
        // In this case, we can read multiple frames
        Iterator.continually(RdfStreamFrame.parseDelimitedFrom(newIn))
          .takeWhile(_.isDefined).map(_.get)
