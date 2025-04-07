package eu.neverblink.jelly.cli.util.io

import scala.collection.mutable

class YamlDocBuilder(var currIndent: Int = 0):
  import YamlDocBuilder.*
  private val sb = new StringBuilder

  def getString: String = sb.toString

  private def build(root: YamlValue, indent: Int = currIndent): Unit =
    if indent > currIndent then currIndent = indent
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
        v.zipWithIndex.foreach { (e, index) =>
          this.build(e, indent)
          if e != v.last then sb.append(System.lineSeparator())
        }
      case YamlListElem(v) =>
        sb.append(System.lineSeparator())
        sb.append("  " * indent).append("- ")
        this.build(v, indent + 1)
      case YamlMap(v) =>
        v.zipWithIndex.foreach { case ((k, e), ix) =>
          if ix != 0 then sb.append("  " * indent)
          sb.append(k)
          sb.append(": ")
          if e.isInstanceOf[YamlMap] then
            // If a map nested inside a map we have to indent it properly
            sb.append(System.lineSeparator())
            sb.append("  " * (indent + 1))
            this.build(e, indent + 1)
            sb.append(System.lineSeparator())
          else this.build(e, indent + 1)
          if ix != v.size - 1 then sb.append(System.lineSeparator())
        }
      case YamlBlank() => ()

object YamlDocBuilder:
  /** A lightweight YAML document builder based on
    * https://github.com/RiverBench/ci-worker/blob/2d57a085f65a6eabbfe76f2de6794f025b211f4e/src/main/scala/util/YamlDocBuilder.scala#L4
    */

  sealed trait YamlValue
  sealed trait YamlScalar extends YamlValue
  case class YamlBlank() extends YamlScalar
  case class YamlEnum(v: String, i: Int) extends YamlScalar
  case class YamlInt(v: Int) extends YamlScalar
  case class YamlBool(v: Boolean) extends YamlScalar
  case class YamlString(v: String) extends YamlScalar

  case class YamlList(v: Seq[YamlListElem]) extends YamlValue
  case class YamlListElem(v: YamlValue) extends YamlValue

  object YamlMap:
    def apply(v: (String, YamlValue)*): YamlMap = YamlMap(v.to(mutable.LinkedHashMap))
    def apply(k: String, v: String): YamlMap = YamlMap(mutable.LinkedHashMap(k -> YamlString(v)))
    def apply(k: String, v: Int): YamlMap = YamlMap(mutable.LinkedHashMap(k -> YamlInt(v)))
    def apply(k: String, v: YamlValue): YamlMap = YamlMap(mutable.LinkedHashMap(k -> v))

  case class YamlMap(v: mutable.LinkedHashMap[String, YamlValue]) extends YamlValue

  def build(root: YamlValue, indent: Int = 0): YamlDocBuilder =
    val builder = YamlDocBuilder(currIndent = indent)
    builder.build(root)
    builder

  private def quoteAndEscape(s: String): String =
    "\"" + escape(s) + "\""

  private def escape(s: String): String =
    s.replace("\\", "\\\\")
      .replace("\"", "\\\"")
      .replace("\n", "\\n")
      .replace("\r", "\\r")
      .replace("\t", "\\t")
