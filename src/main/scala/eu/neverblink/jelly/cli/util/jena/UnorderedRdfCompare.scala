package eu.neverblink.jelly.cli.util.jena

import eu.neverblink.jelly.cli.CriticalException
import org.apache.jena.riot.system.StreamRDFLib
import org.apache.jena.sparql.core.DatasetGraphFactory

import scala.jdk.CollectionConverters.*

object UnorderedRdfCompare extends RdfCompare:
  def compare(
      expected: StreamRdfCollector,
      actual: StreamRdfCollector,
  ): Unit =
    val eDataset = DatasetGraphFactory.create()
    val aDataset = DatasetGraphFactory.create()
    expected.replay(StreamRDFLib.dataset(eDataset))
    actual.replay(StreamRDFLib.dataset(aDataset))
    if eDataset.size() != aDataset.size() then
      throw new CriticalException(
        s"Expected ${eDataset.size()} named graph(s), but got ${aDataset.size()}.",
      )
    if !eDataset.getDefaultGraph.isIsomorphicWith(aDataset.getDefaultGraph) then
      throw new CriticalException(
        "Default graph is not isomorphic with the expected one.",
      )
    for name <- eDataset.listGraphNodes().asScala do
      if !aDataset.containsGraph(name) then
        throw new CriticalException(
          s"Named graph $name is missing in the actual dataset.",
        )
      if !eDataset.getGraph(name).isIsomorphicWith(aDataset.getGraph(name)) then
        throw new CriticalException(
          s"Named graph $name is not isomorphic with the expected one.",
        )
