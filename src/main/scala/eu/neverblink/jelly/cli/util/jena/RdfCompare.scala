package eu.neverblink.jelly.cli.util.jena

trait RdfCompare:
  def compare(
      expected: StreamRdfCollector,
      actual: StreamRdfCollector,
  ): Unit
