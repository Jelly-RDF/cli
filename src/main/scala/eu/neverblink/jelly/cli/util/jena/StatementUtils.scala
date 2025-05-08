package eu.neverblink.jelly.cli.util.jena

import org.apache.jena.graph.{Node, Triple}
import org.apache.jena.sparql.core.Quad

object StatementUtils:

  def iterateTerms(t: Triple): Seq[Node] =
    t.getSubject :: t.getPredicate :: t.getObject :: Nil

  def iterateTerms(q: Quad): Seq[Node] =
    q.getSubject :: q.getPredicate :: q.getObject :: q.getGraph :: Nil

  def isGeneralized(t: Triple): Boolean =
    (!t.getSubject.isBlank && !t.getSubject.isURI && !t.getSubject.isNodeTriple)
      || !t.getPredicate.isURI

  def isGeneralized(q: Quad): Boolean =
    (!q.getSubject.isBlank && !q.getSubject.isURI && !q.getSubject.isNodeTriple)
      || !q.getPredicate.isURI
      || (!q.getGraph.isBlank && !q.getGraph.isURI)

  def isRdfStar(t: Triple): Boolean = iterateTerms(t).exists(_.isNodeTriple)

  def isRdfStar(q: Quad): Boolean = iterateTerms(q).exists(_.isNodeTriple)
