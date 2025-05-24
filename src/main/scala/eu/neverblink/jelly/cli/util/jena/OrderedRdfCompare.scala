package eu.neverblink.jelly.cli.util.jena

import eu.neverblink.jelly.cli.CriticalException
import eu.neverblink.jelly.core.NamespaceDeclaration
import org.apache.jena.graph.{Node, Triple}
import org.apache.jena.sparql.core.Quad

import scala.collection.mutable

object OrderedRdfCompare extends RdfCompare:
  import StatementUtils.*

  private val termNames = Seq(
    "subject",
    "predicate",
    "object",
    "graph",
  )

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
    val bNodeMap = mutable.Map.empty[String, String]

    def tryIsomorphism(e: Seq[Node], a: Seq[Node], location: String): Unit =
      e.zip(a).zipWithIndex.foreach { (terms, termIndex) =>
        val (et, at) = terms
        if et.isBlank && at.isBlank then
          val eId = et.getBlankNodeLabel
          val aId = at.getBlankNodeLabel
          if bNodeMap.contains(eId) then
            if bNodeMap(eId) != aId then
              throw new CriticalException(
                s"RDF element $location is different in ${termNames(termIndex)} term: " +
                  s"expected $e, got $a. $eId is already mapped to ${bNodeMap(eId)}.",
              )
          else bNodeMap(eId) = aId
        else if et.isNodeTriple && at.isNodeTriple then
          // Recurse into the RDF-star quoted triple
          tryIsomorphism(
            iterateTerms(et.getTriple),
            iterateTerms(at.getTriple),
            f"${location}_${termNames(termIndex)}",
          )
        else if et != at then
          throw new CriticalException(
            s"RDF element $location is different in ${termNames(termIndex)} term: " +
              s"expected $e, got $a.",
          )
      }

    eSeq.zip(aSeq).zipWithIndex.foreach { case ((e, a), i) =>
      (e, a) match {
        case (e: Triple, a: Triple) =>
          tryIsomorphism(iterateTerms(e), iterateTerms(a), i.toString)
        case (e: Quad, a: Quad) =>
          tryIsomorphism(iterateTerms(e), iterateTerms(a), i.toString)
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
