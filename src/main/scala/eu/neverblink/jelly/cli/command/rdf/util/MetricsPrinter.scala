package eu.neverblink.jelly.cli.command.rdf.util

import com.google.protobuf.ByteString
import eu.neverblink.jelly.cli.util.io.YamlDocBuilder
import eu.neverblink.jelly.cli.util.io.YamlDocBuilder.*
import eu.neverblink.jelly.core.proto.v1.*

import java.io.OutputStream
import scala.language.postfixOps

/** This class is used to store the metrics for a single frame
  */
class FrameInfo(val frameIndex: Long, val metadata: Map[String, ByteString]):
  var frameCount: Long = 1
  var optionCount: Long = 0
  var nameCount: Long = 0
  var namespaceCount: Long = 0
  var tripleCount: Long = 0
  var quadCount: Long = 0
  var prefixCount: Long = 0
  var datatypeCount: Long = 0
  var graphStartCount: Long = 0
  var graphEndCount: Long = 0

  def +=(other: FrameInfo): FrameInfo = {
    this.frameCount += 1
    this.optionCount += other.optionCount
    this.nameCount += other.nameCount
    this.namespaceCount += other.namespaceCount
    this.tripleCount += other.tripleCount
    this.quadCount += other.quadCount
    this.prefixCount += other.prefixCount
    this.datatypeCount += other.datatypeCount
    this.graphStartCount += other.graphStartCount
    this.graphEndCount += other.graphEndCount
    this
  }

  def processStreamRow(row: RdfStreamRow): Unit = row.getRow match {
    case r: RdfTriple => handleTriple(r)
    case r: RdfQuad => handleQuad(r)
    case r: RdfNameEntry => handleNameEntry(r)
    case r: RdfPrefixEntry => handlePrefixEntry(r)
    case r: RdfNamespaceDeclaration => handleNamespaceDeclaration(r)
    case r: RdfDatatypeEntry => handleDatatypeEntry(r)
    case r: RdfGraphStart => handleGraphStart(r)
    case r: RdfGraphEnd => handleGraphEnd(r)
    case r: RdfStreamOptions => handleOption(r)
  }

  protected def handleTriple(r: RdfTriple): Unit = tripleCount += 1
  protected def handleQuad(r: RdfQuad): Unit = quadCount += 1
  protected def handleNameEntry(r: RdfNameEntry): Unit = nameCount += 1
  protected def handlePrefixEntry(r: RdfPrefixEntry): Unit = prefixCount += 1
  protected def handleNamespaceDeclaration(r: RdfNamespaceDeclaration): Unit = namespaceCount += 1
  protected def handleDatatypeEntry(r: RdfDatatypeEntry): Unit = datatypeCount += 1
  protected def handleGraphStart(r: RdfGraphStart): Unit = graphStartCount += 1
  protected def handleGraphEnd(r: RdfGraphEnd): Unit = graphEndCount += 1
  protected def handleOption(r: RdfStreamOptions): Unit = optionCount += 1

  def format(): Seq[(String, Long)] = Seq(
    ("option_count", optionCount),
    ("triple_count", tripleCount),
    ("quad_count", quadCount),
    ("graph_start_count", graphStartCount),
    ("graph_end_count", graphEndCount),
    ("namespace_count", namespaceCount),
    ("name_count", nameCount),
    ("prefix_count", prefixCount),
    ("datatype_count", datatypeCount),
  )

end FrameInfo

class TermDetailInfo:
  var iriCount: Long = 0
  var bNodeCount: Long = 0
  var literalCount: Long = 0
  var tripleCount: Long = 0
  var elseCount: Long = 0
  def handle(o: Object): Unit = o match {
    case r: RdfIri => iriCount += 1
    case r: String => bNodeCount += 1 // bnodes are strings
    case r: RdfLiteral => literalCount += 1
    case r: RdfTriple => tripleCount += 1
    case _ => elseCount += 1
  }
  def format(): Seq[(String, Long)] = Seq(
    ("iri_count", iriCount),
    ("bnode_count", bNodeCount),
    ("literal_count", literalCount),
    ("triple_count", tripleCount)
  )

class GraphDetailInfo:
  var iriCount: Long = 0
  var bNodeCount: Long = 0
  var literalCount: Long = 0
  var defaultGraphCount: Long = 0

  def handle(o: Object): Unit = o match {
    case r: RdfIri => iriCount += 1
    case r: String => bNodeCount += 1 // bnodes are strings
    case r: RdfLiteral => literalCount += 1
    case r: RdfDefaultGraph => defaultGraphCount += 1
  }

  def format(): Seq[(String, Long)] = Seq(
    ("iri_count", iriCount),
    ("bnode_count", bNodeCount),
    ("literal_count", literalCount),
    ("default_graph_count", defaultGraphCount)
  )

class TripleDetailInfo:
  var subjectInfo = TermDetailInfo()
  var predicateInfo = TermDetailInfo()
  var objectInfo = TermDetailInfo()
  def handleTriple(rdfSubject: Object, rdfPredicate: Object, rdfObject: Object): Unit = {
    subjectInfo.handle(rdfSubject)
    predicateInfo.handle(rdfPredicate)
    objectInfo.handle(rdfObject)
  }

  def format(): Seq[(String, Long)] = subjectInfo.format().map("subject_" ++ _ -> _) ++
    predicateInfo.format().map("predicate_" ++ _ -> _) ++
    objectInfo.format().map("object_" ++ _ -> _)

class QuadDetailInfo extends TripleDetailInfo:
   var graphInfo = GraphDetailInfo()
   def handleQuad(rdfSubject: Object, rdfPredicate: Object, rdfObject: Object, rdfGraph: Object): Unit = {
     handleTriple(rdfSubject, rdfPredicate, rdfObject)
     graphInfo.handle(rdfGraph)
   }
   override def format(): Seq[(String, Long)] = super.format() ++ graphInfo.format().map("graph_" ++ _ -> _)

class FrameDetailInfo(frameIndex: Long, metadata: Map[String, ByteString]) extends FrameInfo(frameIndex, metadata):
  var tripleDetailInfo = TripleDetailInfo()
  var quadDetailInfo = QuadDetailInfo()

  override def handleTriple(r: RdfTriple): Unit = {
    super.handleTriple(r)
    tripleDetailInfo.handleTriple(r.getSubject, r.getPredicate, r.getObject)
  }

  override def handleQuad(r: RdfQuad): Unit = {
    super.handleQuad(r)
    quadDetailInfo.handleQuad(r.getSubject, r.getPredicate, r.getObject, r.getGraph)
  }

  override def format(): Seq[(String, Long)] = super.format() ++
    tripleDetailInfo.format().map("triple_"++_ -> _) ++
    quadDetailInfo.format().map("quad_"++_->_)

object MetricsPrinter:

  def printPerFrame(
      options: RdfStreamOptions,
      iterator: Iterator[FrameInfo],
      o: OutputStream,
  ): Unit =
    printOptions(options, o)
    val builder =
      YamlDocBuilder.build(
        YamlMap(
          "frames" -> YamlBlank(),
        ),
      )
    val fullString = builder.getString
    o.write(fullString.getBytes)
    iterator.foreach { frame =>
      val yamlFrame = YamlListElem(formatStatsIndex(frame))
      val fullString = YamlDocBuilder.build(yamlFrame, builder.currIndent).getString
      o.write(fullString.getBytes)
      o.write(System.lineSeparator().getBytes)
    }

  def printAggregate(
      options: RdfStreamOptions,
      iterator: Iterator[FrameInfo],
      o: OutputStream,
  ): Unit = {
    printOptions(options, o)
    val sumCounts = iterator.reduce((a, b) => a += b)
    val fullString =
      YamlDocBuilder.build(
        YamlMap(
          "frames" -> formatStatsCount(sumCounts),
        ),
      ).getString
    o.write(fullString.getBytes)
  }

  private def printOptions(
      printOptions: RdfStreamOptions,
      o: OutputStream,
  ): Unit =
    val options = formatOptions(options = printOptions)
    val fullString =
      YamlDocBuilder.build(
        YamlMap(
          "stream_options" -> options,
        ),
      ).getString
    o.write(fullString.getBytes)
    o.write(System.lineSeparator().getBytes)

  private def formatOptions(
      options: RdfStreamOptions,
  ): YamlMap =
    YamlMap(
      "stream_name" -> YamlString(options.getStreamName),
      "physical_type" -> YamlEnum(options.getPhysicalType.toString, options.getPhysicalTypeValue),
      "generalized_statements" -> YamlBool(options.getGeneralizedStatements),
      "rdf_star" -> YamlBool(options.getRdfStar),
      "max_name_table_size" -> YamlInt(options.getMaxNameTableSize),
      "max_prefix_table_size" -> YamlInt(options.getMaxPrefixTableSize),
      "max_datatype_table_size" -> YamlInt(options.getMaxDatatypeTableSize),
      "logical_type" -> YamlEnum(options.getLogicalType.toString, options.getLogicalTypeValue),
      "version" -> YamlInt(options.getVersion),
    )

  private def formatStatsIndex(
      frame: FrameInfo,
  ): YamlMap =
    YamlMap(
      Seq(("frame_index", YamlLong(frame.frameIndex))) ++
        formatMetadata(frame.metadata).map(("metadata", _)) ++
        formatStats(frame)*,
    )

  private def formatStatsCount(
      frame: FrameInfo,
  ): YamlMap =
    // Not printing metadata in this case, as there is no upper bound on the number of frames
    // and thus on the size of the collected metadata.
    YamlMap(Seq(("frame_count", YamlLong(frame.frameCount))) ++ formatStats(frame)*)

  private def formatMetadata(
      metadata: Map[String, ByteString],
  ): Option[YamlMap] =
    if metadata.isEmpty then None
    else
      Some(
        YamlMap(
          metadata.map { case (k, v) =>
            k -> YamlString(v.toByteArray.map("%02x" format _).mkString)
          }.toSeq*,
        ),
      )

  private def formatStats(
      frame: FrameInfo,
  ): Seq[(String, YamlValue)] = frame.format().map(_ -> YamlLong(_))

end MetricsPrinter
