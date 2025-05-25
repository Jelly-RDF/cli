package eu.neverblink.jelly.cli.command.rdf

import com.google.protobuf.InvalidProtocolBufferException
import eu.neverblink.jelly.cli.command.helpers.DataGenHelper
import eu.neverblink.jelly.core.internal.BaseJellyOptions.{
  BIG_DT_TABLE_SIZE,
  BIG_NAME_TABLE_SIZE,
  BIG_PREFIX_TABLE_SIZE,
}
import eu.neverblink.jelly.core.{JellyOptions, JellyTranscoderFactory}
import eu.neverblink.jelly.core.proto.v1.{PhysicalStreamType, RdfStreamFrame, RdfStreamOptions}
import eu.neverblink.jelly.core.proto.google.v1 as google
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class HammerTranscoderSpec extends AnyWordSpec, Matchers:
  "hammer" should {
    "transcode" in {
      val result = ArrayBuffer[Either[Unit, Throwable]]()
      var printed = false

      for i <- 1 to 10_000 do
        val j1 = DataGenHelper.generateJellyBytes(Random.nextInt(100) + 2)
        val f1 = RdfStreamFrame.parseDelimitedFrom(ByteArrayInputStream(j1))
        val os = ByteArrayOutputStream()
//        val options = JellyOptions.BIG_GENERALIZED.clone
//          .setPhysicalType(PhysicalStreamType.TRIPLES)
        val options = RdfStreamOptions.newInstance
          .setMaxNameTableSize(BIG_NAME_TABLE_SIZE)
          .setMaxPrefixTableSize(BIG_PREFIX_TABLE_SIZE)
          .setMaxDatatypeTableSize(BIG_DT_TABLE_SIZE)
          .setGeneralizedStatements(true)
          .setPhysicalType(PhysicalStreamType.TRIPLES)
        val transcoder = JellyTranscoderFactory.fastMergingTranscoderUnsafe(
          options,
        )
        val frames = 1
        val transcoded =
          for _ <- 0 until frames yield transcoder.ingestFrame(f1).writeDelimitedTo(os)
        val bytes = os.toByteArray
        val input = ByteArrayInputStream(bytes)
        try
          val parsed = Iterator.continually(
            google.RdfStreamFrame.parseDelimitedFrom(input),
          )
            .takeWhile(frame => frame != null)
            .toSeq
          parsed.size should be(frames)
          result.append(Left(()))
        catch
          case e: Throwable =>
            result.append(Right(e))
            if !printed then
              println(f"Error in transcoding $i: ${e.getMessage}")
              println(f"Original:   ${j1.map(b => f"$b%02x").mkString(" ")}")
              println(f"Transcoded: ${bytes.map(b => f"$b%02x").mkString(" ")}")
              println(f"preset cached size: ${JellyOptions.BIG_GENERALIZED.getCachedSize}")
              println(f"modified preset cached size: ${options.getCachedSize}")
              println(
                f"transcoded cached size: ${transcoded.head.getRows.iterator().next().getCachedSize}",
              )
              printed = true

      println(f"Errors: ${result.count(_.isRight)} of ${result.size}")
      var regions = 0
      var regionType = 0
      for r <- result do
        val newType = r match
          case Left(_) => -1
          case Right(e) => 1

        if regionType != newType then
          regions += 1
          regionType = newType

      println(f"Regions: $regions")
      println("Throwing last error if found...")
      result.filter(_.isRight).lastOption match
        case Some(Right(e)) =>
          e match
            case e2: InvalidProtocolBufferException =>
              println(e2.getUnfinishedMessage.toString)
              throw e
            case _ => throw e
        case _ => // No error to throw, all good
    }

//    for i <- 1 to 10_000 do
//      f"transcode $i" in {
//        val j1 = DataGenHelper.generateJellyBytes(Random.nextInt(100) + 2)
//        val f1 = RdfStreamFrame.parseDelimitedFrom(ByteArrayInputStream(j1))
//        val os = ByteArrayOutputStream()
//        val transcoder = JellyTranscoderFactory.fastMergingTranscoderUnsafe(
//          JellyOptions.BIG_GENERALIZED.clone
//            .setPhysicalType(PhysicalStreamType.TRIPLES),
//        )
//        val frames = Random.nextInt(40) + 3
//        for _ <- 0 until frames do transcoder.ingestFrame(f1).writeDelimitedTo(os)
//        val bytes = os.toByteArray
//        val input = ByteArrayInputStream(bytes)
//        val parsed = Iterator.continually(
//          google.RdfStreamFrame.parseDelimitedFrom(input),
//        )
//          .takeWhile(frame => frame != null)
//          .toSeq
//        parsed.size should be(frames)
//      }
  }
