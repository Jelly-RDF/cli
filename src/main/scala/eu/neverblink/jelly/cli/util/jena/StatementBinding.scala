package eu.neverblink.jelly.cli.util.jena

import org.apache.jena.graph.Node
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.engine.binding.{Binding, BindingBase}
import org.apache.jena.sparql.util.FmtUtils

import java.util.function.BiConsumer

/** Optimized Binding implementations for converting between Jelly-RDF and Jelly-SPARQL. */
object StatementBinding:
  val subject: Var = Var.alloc("s")
  val predicate: Var = Var.alloc("p")
  val `object`: Var = Var.alloc("o")
  val graph: Var = Var.alloc("g")

  val tripleVars: Seq[Var] = Seq(subject, predicate, `object`)
  val quadVars: Seq[Var] = Seq(subject, predicate, `object`, graph)

  // Shared by all bindings, so that a binding only holds its terms
  private[jena] val tripleVarList: java.util.List[Var] =
    java.util.List.of(subject, predicate, `object`)
  private[jena] val quadVarList: java.util.List[Var] =
    java.util.List.of(subject, predicate, `object`, graph)
  private[jena] val tripleVarSet: java.util.Set[Var] = java.util.Set.copyOf(tripleVarList)
  private[jena] val quadVarSet: java.util.Set[Var] = java.util.Set.copyOf(quadVarList)

  /** Finds the variable a binding may hold, first by identity, which is the usual case, and then by
    * name. Returns null if the variable is not one of ?s ?p ?o ?g.
    */
  private[jena] def resolve(v: Var): Var =
    if (v eq subject) || (v eq predicate) || (v eq `object`) || (v eq graph) then v
    else
      v.getVarName match
        case "s" => subject
        case "p" => predicate
        case "o" => `object`
        case "g" => graph
        case _ => null

  /** Formats a binding the way Jena does: ( ?s = <...> ) ( ?p = <...> ) ... */
  private[jena] def format(b: Binding): String =
    val sb = StringBuilder()
    b.forEach { (v, n) =>
      if sb.nonEmpty then sb.append(' ')
      sb.append("( ").append(v).append(" = ").append(FmtUtils.stringForNode(n)).append(" )")
    }
    sb.toString

/** Binding of ?s ?p ?o. Also used for quads in the default graph, where ?g is unbound. */
final class TripleBinding(s: Node, p: Node, o: Node) extends Binding:
  import StatementBinding.*

  override def vars(): java.util.Iterator[Var] = tripleVarList.iterator()

  override def varsMentioned(): java.util.Set[Var] = tripleVarSet

  override def forEach(action: BiConsumer[Var, Node]): Unit =
    action.accept(subject, s)
    action.accept(predicate, p)
    action.accept(`object`, o)

  override def contains(v: Var): Boolean = get(v) != null

  override def get(v: Var): Node =
    val resolved = resolve(v)
    if resolved eq subject then s
    else if resolved eq predicate then p
    else if resolved eq `object` then o
    else null

  override def size(): Int = 3

  override def isEmpty: Boolean = false

  override def detach(): Binding = this

  override def hashCode(): Int = BindingBase.hashCode(this)

  // Same as Jena's: the same variables, bound to the same terms
  override def equals(other: Any): Boolean = other match
    case b: Binding =>
      b.size() == 3 && s == b.get(subject) && p == b.get(predicate) && o == b.get(`object`)
    case _ => false

  override def toString: String = format(this)

/** Binding of ?s ?p ?o ?g, for quads in a named graph. All terms must be non-null. */
final class QuadBinding(s: Node, p: Node, o: Node, g: Node) extends Binding:
  import StatementBinding.*

  override def vars(): java.util.Iterator[Var] = quadVarList.iterator()

  override def varsMentioned(): java.util.Set[Var] = quadVarSet

  override def forEach(action: BiConsumer[Var, Node]): Unit =
    action.accept(subject, s)
    action.accept(predicate, p)
    action.accept(`object`, o)
    action.accept(graph, g)

  override def contains(v: Var): Boolean = get(v) != null

  override def get(v: Var): Node =
    val resolved = resolve(v)
    if resolved eq subject then s
    else if resolved eq predicate then p
    else if resolved eq `object` then o
    else if resolved eq graph then g
    else null

  override def size(): Int = 4

  override def isEmpty: Boolean = false

  // There is no parent to detach from
  override def detach(): Binding = this

  override def hashCode(): Int = BindingBase.hashCode(this)

  // Same as Jena's: the same variables, bound to the same terms
  override def equals(other: Any): Boolean = other match
    case b: Binding =>
      b.size() == 4 && s == b.get(subject) && p == b.get(predicate) &&
      o == b.get(`object`) && g == b.get(graph)
    case _ => false

  override def toString: String = format(this)
