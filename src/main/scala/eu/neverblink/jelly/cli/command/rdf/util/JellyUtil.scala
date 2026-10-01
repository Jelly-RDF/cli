package eu.neverblink.jelly.cli.command.rdf.util

import eu.neverblink.jelly.cli.util.io.IoUtil
import eu.neverblink.jelly.core.utils.IoUtils
import eu.neverblink.jelly.core.proto.v1.{RdfStreamFrame, RdfStreamOptions}

import java.io.InputStream
import scala.util.Using

object JellyUtil:
  /** Reads the Jelly file and returns an iterator of RdfStreamFrame
    *
    * @param inputStream
    * @param outputStream
    * @return
    */
  def iterateRdfStream(
      inputStream: InputStream,
  ): Iterator[RdfStreamFrame] = iterateRdfStreamWithDelimitingInfo(inputStream)._2

  /** Reads the Jelly file and returns an iterator of RdfStreamFrame and a boolean indicating if the
    * file is delimited or not
    * @param inputStream
    * @return
    */
  def iterateRdfStreamWithDelimitingInfo(
      inputStream: InputStream,
  ): (Boolean, Iterator[RdfStreamFrame]) = {
    val delimitingResponse = IoUtils.autodetectDelimiting(inputStream)
    if delimitingResponse.isDelimited then
      // Delimited Jelly file
      // In this case, we can read multiple frames
      (
        true,
        Iterator.continually(RdfStreamFrame.parseDelimitedFrom(delimitingResponse.newInput))
          .takeWhile(_ != null),
      )
    else
      // Non-delimited Jelly file
      // In this case, we can only read one frame
      (false, Iterator(RdfStreamFrame.parseFrom(delimitingResponse.newInput)))
  }

  /** Reads the stream options from the first row of a Jelly-RDF file.
    * @param fileName
    *   file to read
    * @return
    *   the stream options
    */
  def loadOptionsFromFile(fileName: String): RdfStreamOptions =
    Using.resource(IoUtil.inputStream(fileName)) { is =>
      iterateRdfStream(is).next().getRows.iterator().next().getOptions
    }
