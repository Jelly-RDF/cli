package eu.neverblink.jelly.cli.util

object YamlDocBuilder:
  /** A lightweight YAML document builder based on
    * https://github.com/RiverBench/ci-worker/blob/main/src%2Fmain%2Fscala%2Futil%2FYamlDocBuilder.scala
    */

  sealed trait YamlValue
  case class YamlString(v: String) extends YamlValue

  case class YamlList(v: Seq[YamlValue]) extends YamlValue

  object YamlMap:
    def apply(v: (String, YamlValue)*): YamlMap = YamlMap(v.toMap)
    def apply(k: String, v: String): YamlMap = YamlMap(Map(k -> YamlString(v)))
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
      case YamlList(v) =>
        sb.append(System.lineSeparator())
        v.foreach { e =>
          sb.append("  " * indent).append("- ")
          build(e, sb, indent + 1)
          if e != v.last then sb.append(System.lineSeparator())
        }
      case YamlMap(v) =>
        v.zipWithIndex.foreach { case ((k, e), ix) =>
          if ix != 0 then sb.append("  " * indent)
          sb.append(quoteAndEscape(k))
          sb.append(": ")
          build(e, sb, indent + 1)
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
