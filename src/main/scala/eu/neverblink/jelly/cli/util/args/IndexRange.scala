package eu.neverblink.jelly.cli.util.args

import eu.neverblink.jelly.cli.InvalidArgument

import scala.collection.IterableOnceOps

final case class IndexRange(
    start: Option[Int],
    end: Option[Int],
):
  def slice[T, C <: IterableOnceOps[T, ?, C]](it: C): C =
    val startIndex = start.getOrElse(0)
    val endIndex = end.getOrElse(it.size)
    it.slice(startIndex, endIndex)

/** Parser for Rust-style index ranges.
  */
object IndexRange:
  def apply(range: String, argumentName: String): IndexRange = try {
    range.trim match {
      case "" => IndexRange(None, None)
      case s if s.contains("..") =>
        val ix = s.indexOf("..")
        val before = s.substring(0, ix)
        val after = s.substring(ix + 2)
        val start = if before.isEmpty then None else Some(before.toInt)
        val end =
          if after.startsWith("=") then
            if after.length == 1 then None else Some(after.substring(1).toInt + 1)
          else if after.isEmpty then None
          else Some(after.toInt)
        IndexRange(start, end)
      case s if s.toIntOption.isDefined => IndexRange(Some(s.toInt), Some(s.toInt + 1))
      case _ => throw new IllegalArgumentException(s"Invalid range format: $range")
    }
  } catch
    case e: Throwable =>
      throw InvalidArgument(
        argumentName,
        range,
        Some(
          "Correct ranges are in the form '3' (one index), '..3' (up to exclusive), " +
            "'3..' (from inclusive), or '1..3' (range up to exclusive), or '1..=3' (inclusive)",
        ),
      )
