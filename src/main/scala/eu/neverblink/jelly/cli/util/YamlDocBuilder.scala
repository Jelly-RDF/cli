package eu.neverblink.jelly.cli.util

object YamlDocBuilder:
  /** A lightweight YAML document builder based on
    * https://github.com/RiverBench/ci-worker/blob/main/src%2Fmain%2Fscala%2Futil%2FYamlDocBuilder.scala
    */

  sealed trait YamlValue
  sealed trait YamlScalar extends YamlValue
  case class YamlInt(v: Int) extends YamlScalar
  case class YamlString(v: String) extends YamlScalar

  case class YamlList(v: Seq[YamlValue]) extends YamlValue

  object YamlMap:
    def apply(v: (String, YamlValue)*): YamlMap = YamlMap(v.toMap)
    def apply(k: String, v: String): YamlMap = YamlMap(Map(k -> YamlString(v)))
    def apply(k: String, v: Int): YamlMap = YamlMap(Map(k -> YamlInt(v)))
    def apply(k: String, v: YamlValue): YamlMap = YamlMap(Map(k -> v))

  case class YamlMap(v: Map[String, YamlValue]) extends YamlValue

  def build(root: YamlValue): String =
    val sb = new StringBuilder
    build(root, sb, 0)
    sb.toString

  private def build(root: YamlValue, sb: StringBuilder, indent: Int): Unit =
    root match
      case YamlString(v) =>
        sb.append(quoteAndEscape(v))
      case YamlInt(v) =>
        sb.append(v)
      case YamlList(v) =>
        sb.append(System.lineSeparator())
        v.zipWithIndex.foreach { (e, index) =>
          // We want to add a comment about which frame we're summing up
          sb.append(f"# frame ${index}")
          sb.append(System.lineSeparator())
          sb.append("  " * indent).append("- ")
          build(e, sb, indent + 1)
          if e != v.last then sb.append(System.lineSeparator())
        }
      case YamlMap(v) =>
        v.zipWithIndex.foreach { case ((k, e), ix) =>
          if ix != 0 then sb.append("  " * indent)
          sb.append(k)
          sb.append(": ")
          if e.isInstanceOf[YamlMap] then
            // If a map nested inside a map we have to indent it properly
            sb.append(System.lineSeparator())
            sb.append("  " * (indent + 1))
            build(e, sb, indent + 1)
          else build(e, sb, indent + 1)
          if ix != v.size - 1 then sb.append(System.lineSeparator())
        }

  private def quoteAndEscape(s: String): String =
    "\"" + escape(s) + "\""

  private def escape(s: String): String =
    s.replace("\\", "\\\\")
      .replace("\"", "\\\"")
      .replace("\n", "\\n")
      .replace("\r", "\\r")
      .replace("\t", "\\t")
