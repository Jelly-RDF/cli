package eu.neverblink.jelly.cli.command.rdf

enum RdfFormatOptions(val cliOption: String):
  case NQuads extends RdfFormatOptions("n-quad-format")
  case JellyText extends RdfFormatOptions("jelly-text-format")
  case JellyBinary extends RdfFormatOptions("jelly-binary-format")
