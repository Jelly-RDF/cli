package eu.neverblink.jelly.cli.util

import scala.collection.mutable

object YamlDocBuilder:
  /** A lightweight YAML document builder based on
    * https://github.com/RiverBench/ci-worker/blob/2d57a085f65a6eabbfe76f2de6794f025b211f4e/src/main/scala/util/YamlDocBuilder.scala#L4
    */

  sealed trait YamlValue
  sealed trait YamlScalar extends YamlValue
  case class YamlEnum(v: String, i: Int) extends YamlScalar
  case class YamlInt(v: Int) extends YamlScalar
  case class YamlBool(v: Boolean) extends YamlScalar
  case class YamlString(v: String) extends YamlScalar

  case class YamlList(v: Seq[YamlValue]) extends YamlValue

  object YamlMap:
    def apply(v: (String, YamlValue)*): YamlMap = YamlMap(v.to(mutable.LinkedHashMap))
    def apply(k: String, v: String): YamlMap = YamlMap(mutable.LinkedHashMap(k -> YamlString(v)))
    def apply(k: String, v: Int): YamlMap = YamlMap(mutable.LinkedHashMap(k -> YamlInt(v)))
    def apply(k: String, v: YamlValue): YamlMap = YamlMap(mutable.LinkedHashMap(k -> v))

  case class YamlMap(v: mutable.LinkedHashMap[String, YamlValue]) extends YamlValue

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
      case YamlBool(v) =>
        sb.append(v.toString)
      case YamlEnum(v, i) =>
        sb.append(f"${v} (${i})")
      case YamlList(v) =>
        sb.append(System.lineSeparator())
        v.zipWithIndex.foreach { (e, index) =>
          sb.append("  " * indent).append("- ")
          build(e, sb, indent + 1)
          sb.append(System.lineSeparator())
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
            sb.append(System.lineSeparator())
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
