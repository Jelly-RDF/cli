package eu.neverblink.jelly.cli.util.args

import eu.neverblink.jelly.cli.InvalidArgument
import eu.neverblink.jelly.core.proto.v1.RdfVersion
import eu.neverblink.jelly.core.utils.RdfVersionUtils

/** Parsing of the --opt.* arguments shared by Jelly-RDF and Jelly-SPARQL. */
object StreamOptionArgs:
  val rdfVersionHelp: String =
    "Version of RDF for the stream. One of: 1.1, 1.2-basic (base directions, no triple terms), " +
      "1.2 (all RDF 1.2 terms), unspecified."

  /** Parses the value of --opt.rdf-version. */
  def rdfVersion(value: String): RdfVersion =
    value.trim.toLowerCase match
      case "unspecified" => RdfVersion.RDF_VERSION_UNSPECIFIED
      case label =>
        try RdfVersionUtils.rdfVersionFromLabel(label)
        catch
          case _: IllegalArgumentException =>
            throw InvalidArgument(
              "--opt.rdf-version",
              value,
              Some("Must be one of: 1.1, 1.2-basic, 1.2, unspecified"),
            )

  /** Checks that a lookup table size set on the command line is at least `minSize`. */
  def checkTableSize(argument: String, size: Option[Int], minSize: Int): Unit =
    size.filter(_ < minSize).foreach { s =>
      throw InvalidArgument(argument, s.toString, Some(f"Must be at least $minSize"))
    }
