package eu.neverblink.jelly.cli.util.jena

import eu.neverblink.jelly.cli.CriticalException
import eu.ostrzyciel.jelly.core.NamespaceDeclaration
import org.apache.jena.graph.{Node, Triple}
import org.apache.jena.sparql.core.Quad

object OrderedRdfCompare extends RdfCompare:
  import StatementUtils.*

  def compare(
      expected: StreamRdfCollector,
      actual: StreamRdfCollector,
  ): Unit =
    val eSeq = expected.getBuffer
    val aSeq = actual.getBuffer
    if eSeq.size != aSeq.size then
      throw new CriticalException(
        s"Expected ${eSeq.size} RDF elements, but got ${aSeq.size} elements.",
      )
    val bNodeMap = scala.collection.mutable.Map.empty[String, String]
    def tryIsomorphism(e: Seq[Node], a: Seq[Node], i: Int): Unit =
      e.zip(a).foreach { (et, at) =>
        if et.isBlank && at.isBlank then
          val eId = et.getBlankNodeLabel
          val aId = at.getBlankNodeLabel
          if bNodeMap.contains(eId) then
            if bNodeMap(eId) != aId then
              throw new CriticalException(
                s"RDF element $i is different: expected $e, got $a. $eId is " +
                  s"already mapped to ${bNodeMap(eId)}.",
              )
          else bNodeMap(eId) = aId
        else if et != at then
          throw new CriticalException(
            s"RDF element $i is different: expected $e, got $a.",
          )
      }
    eSeq.zip(aSeq).zipWithIndex.foreach { case ((e, a), i) =>
      (e, a) match {
        case (e: Triple, a: Triple) =>
          tryIsomorphism(iterateTerms(e), iterateTerms(a), i)
        case (e: Quad, a: Quad) =>
          tryIsomorphism(iterateTerms(e), iterateTerms(a), i)
        case (e: NamespaceDeclaration, a: NamespaceDeclaration) =>
          if e != a then
            throw new CriticalException(
              s"RDF element $i is different: expected $e, got $a.",
            )
        case _ =>
          throw new CriticalException(
            s"RDF element $i is of different type: expected ${e.getClass}, got ${a.getClass}.",
          )
      }
    }
