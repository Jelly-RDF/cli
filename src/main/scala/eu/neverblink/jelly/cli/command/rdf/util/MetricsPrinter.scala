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

class NodeDetailInfo:
  var iriCount: Long = 0
  var bNodeCount: Long = 0
  var literalCount: Long = 0
  var tripleCount: Long = 0
  var defaultGraphCount: Long = 0

  def handle(o: Object): Unit = o match {
    case r: RdfIri => iriCount += 1
    case r: String => bNodeCount += 1 // bnodes are strings
    case r: RdfLiteral => literalCount += 1
    case r: RdfTriple => tripleCount += 1
    case r: RdfDefaultGraph => defaultGraphCount += 1
  }

  def format(): Seq[(String, Long)] = Seq(
    ("iri_count", iriCount),
    ("bnode_count", bNodeCount),
    ("literal_count", literalCount),
    ("triple_count", tripleCount),
    ("default_graph_count", defaultGraphCount),
  )

  def +=(other: NodeDetailInfo): NodeDetailInfo = {
    this.iriCount += other.iriCount
    this.bNodeCount += other.bNodeCount
    this.literalCount += other.literalCount
    this.tripleCount += other.tripleCount
    this.defaultGraphCount += other.defaultGraphCount
    this
  }

  def total(): Long = iriCount
    + bNodeCount
    + literalCount
    + tripleCount
    + defaultGraphCount

class FrameDetailInfo(frameIndex: Long, metadata: Map[String, ByteString]) extends FrameInfo(frameIndex, metadata):
  var subjectInfo = new NodeDetailInfo()
  var predicateInfo = new NodeDetailInfo()
  var objectInfo = new NodeDetailInfo()
  var graphInfo = new NodeDetailInfo()

  override def +=(other: FrameInfo): FrameInfo = {
    super.+=(other)
    (this, other) match {
      case (tthis: FrameDetailInfo, oother: FrameDetailInfo) =>
        tthis.subjectInfo += oother.subjectInfo
        tthis.predicateInfo += oother.predicateInfo
        tthis.objectInfo += oother.objectInfo
        tthis.graphInfo += oother.graphInfo
      case _ =>
    }
    this
  }

  override def handleTriple(r: RdfTriple): Unit = {
    super.handleTriple(r)
    if r.hasSubject then subjectInfo.handle(r.getSubject)
    if r.hasPredicate then predicateInfo.handle(r.getPredicate)
    if r.hasObject then objectInfo.handle(r.getObject)
  }

  override def handleQuad(r: RdfQuad): Unit = {
    super.handleQuad(r)
    if r.hasSubject then subjectInfo.handle(r.getSubject)
    if r.hasPredicate then predicateInfo.handle(r.getPredicate)
    if r.hasObject then objectInfo.handle(r.getObject)
    if r.hasGraph then graphInfo.handle(r.getGraph)
  }

  def formatFlat(): Seq[(String, Long)] =
    subjectInfo.format().map("subject_"++_->_) ++
    predicateInfo.format().map("predicate_"++_->_) ++
    objectInfo.format().map("object_"++_->_) ++
    graphInfo.format().map("graph_"++_->_)

  def formatGroupByNode(): Seq[(String, Long)] =
    val out = new NodeDetailInfo()
    out += subjectInfo
    out += predicateInfo
    out += objectInfo
    out += graphInfo
    out.format()

  def formatGroupByTerm(): Seq[(String, Long)] = Seq(
    "subject_count" -> subjectInfo.total(),
    "predicate_count" -> predicateInfo.total(),
    "object_count" -> objectInfo.total(),
    "graph_count" -> graphInfo.total()
  )

type Formatter = FrameInfo => Seq[(String, YamlValue)]
type DetailFormatter = FrameDetailInfo => Seq[(String, YamlValue)]

object MetricsPrinter:
  private def withFallback(formatter: DetailFormatter): Formatter =
    {
      case frame@(detailInfo: FrameDetailInfo) =>
        frame.format().map(_ -> YamlLong(_)) ++ formatter(detailInfo)
      case frame@(frameInfo: FrameInfo) => frame.format().map(_ -> YamlLong(_))
    }

  val flatFormatter: Formatter = withFallback(detailInfo => {
    detailInfo.formatFlat().map(_ -> YamlLong(_))
  })

  val termGroupFormatter: Formatter = withFallback(detailInfo => {
    Seq("term_details" -> YamlMap(detailInfo.formatGroupByTerm().map(_ -> YamlLong(_))*))
  })

  val nodeGroupFormatter: Formatter = withFallback(detailInfo => {
    Seq("node_details" -> YamlMap(detailInfo.formatGroupByNode().map(_ -> YamlLong(_))*))
  })

class MetricsPrinter(val formatter: Formatter):
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
  ): Seq[(String, YamlValue)] = this.formatter(frame)

end MetricsPrinter
