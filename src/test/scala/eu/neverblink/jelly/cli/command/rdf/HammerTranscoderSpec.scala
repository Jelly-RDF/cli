package eu.neverblink.jelly.cli.command.rdf

import eu.neverblink.jelly.cli.command.helpers.DataGenHelper
import eu.neverblink.jelly.core.{JellyOptions, JellyTranscoderFactory}
import eu.neverblink.jelly.core.proto.v1.{PhysicalStreamType, RdfStreamFrame}
import eu.neverblink.jelly.core.proto.google.v1 as google
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import scala.util.Random

class HammerTranscoderSpec extends AnyWordSpec, Matchers:
  "hammer" should {
    for i <- 1 to 10_000 do
      f"transcode $i" in {
        val j1 = DataGenHelper.generateJellyBytes(Random.nextInt(100) + 2)
        val f1 = RdfStreamFrame.parseDelimitedFrom(ByteArrayInputStream(j1))
        val os = ByteArrayOutputStream()
        val transcoder = JellyTranscoderFactory.fastMergingTranscoderUnsafe(
          JellyOptions.BIG_GENERALIZED.clone
            .setPhysicalType(PhysicalStreamType.TRIPLES),
        )
        val frames = Random.nextInt(40) + 3
        for _ <- 0 until frames do transcoder.ingestFrame(f1).writeDelimitedTo(os)
        val bytes = os.toByteArray
        val input = ByteArrayInputStream(bytes)
        val parsed = Iterator.continually(
          google.RdfStreamFrame.parseDelimitedFrom(input),
        )
          .takeWhile(frame => frame != null)
          .toSeq
        parsed.size should be(frames)
      }
  }
