package eu.neverblink.jelly.cli.util

import eu.ostrzyciel.jelly.core.IoUtils
import eu.ostrzyciel.jelly.core.proto.v1.RdfStreamFrame

import java.io.InputStream

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
