package eu.neverblink.jelly.cli.util.jena.riot

import eu.neverblink.jelly.convert.jena.JenaConverterFactory
import eu.neverblink.jelly.convert.jena.riot.{JellyFormatVariant, JellyLanguage, JellyStreamWriter}
import eu.neverblink.jelly.core.proto.v1.{LogicalStreamType, PhysicalStreamType, RdfStreamOptions}
import org.apache.jena.riot.RIOT
import org.apache.jena.riot.system.{StreamRDF, StreamRDFWriter}

import java.io.OutputStream

object JellyWriterUtil:

  /** Creates a StreamRDF that writes Jelly-RDF with the given options.
    *
    * If the physical type is unspecified, the writer picks TRIPLES or QUADS based on the first
    * statement it gets.
    *
    * @param jellyOpt
    *   stream options of the output
    * @param frameSize
    *   target number of rows per frame
    * @param enableNamespaceDeclarations
    *   whether to write namespace declarations
    * @param delimited
    *   whether the output should be delimited
    * @param out
    *   where to write the output
    */
  def createWriter(
      jellyOpt: RdfStreamOptions,
      frameSize: Int,
      enableNamespaceDeclarations: Boolean,
      delimited: Boolean,
      out: OutputStream,
  ): StreamRDF =
    jellyOpt.getPhysicalType match
      case PhysicalStreamType.GRAPHS =>
        JellyStreamWriterGraphs(
          JellyFormatVariant
            .builder()
            .options(
              jellyOpt.clone.setLogicalType(
                if jellyOpt.getLogicalType == LogicalStreamType.UNSPECIFIED then
                  LogicalStreamType.FLAT_QUADS
                else jellyOpt.getLogicalType,
              ),
            )
            .frameSize(frameSize)
            .enableNamespaceDeclarations(enableNamespaceDeclarations)
            .isDelimited(delimited)
            .build(),
          out = out,
        )
      case PhysicalStreamType.UNSPECIFIED =>
        val writerContext = RIOT.getContext.copy()
          .set(JellyLanguage.SYMBOL_STREAM_OPTIONS, jellyOpt)
          .set(JellyLanguage.SYMBOL_FRAME_SIZE, frameSize)
          .set(JellyLanguage.SYMBOL_ENABLE_NAMESPACE_DECLARATIONS, enableNamespaceDeclarations)
          .set(JellyLanguage.SYMBOL_DELIMITED_OUTPUT, delimited)
        StreamRDFWriter.getWriterStream(out, JellyLanguage.JELLY, writerContext)
      case _ =>
        // TRIPLES or QUADS: if the physical type is specified, we can just construct the writer
        val variant = JellyFormatVariant
          .builder()
          .options(jellyOpt)
          .frameSize(frameSize)
          .enableNamespaceDeclarations(enableNamespaceDeclarations)
          .isDelimited(delimited)
          .build()
        JellyStreamWriter.create(JenaConverterFactory.getInstance(), variant, out)
