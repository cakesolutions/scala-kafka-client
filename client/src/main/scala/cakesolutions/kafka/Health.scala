package cakesolutions.kafka

import cakesolutions.kafka.Health.{Ok, Status}

trait Health {
  val status: Status
  val message: String
  val counts: Map[Status, Int]

  def indent(i: Int): String = " " * i

  def detailsTree(i: Int): String
}

case class NestedHealth(topMessage: String, nested: List[Health]) extends Health {

  val status = nested.foldLeft[Status](Ok) { case (acc, health) => acc.combine(health.status) }
  val counts = nested.foldLeft(Health.emptyCounts) { case (acc, health) => Health.addCounts(acc, health.counts) }

  def countsString = counts.toList.sortBy(_._1).map { case (s, i) => s"$s: $i" }.mkString(", ")

  def detailsTree(i: Int) = s"${indent(i)}$topMessage:\n" + nested.sortBy(_.status).map(h => s"${h.detailsTree(i + 2)}").mkString("\n")

  def summary(i: Int) = s"${indent(i)}$topMessage: $countsString"

  def indentedMsg(i: Int) = s"${summary(i)}\n${indent(i)}Details:\n${detailsTree(i)}"

  val message = indentedMsg(0)
}

/**
  * Model to represent the health of a component, for integration with sensu.
  */
case class SimpleHealth(status: Status, message: String = "", exceptions: Seq[Throwable] = Seq()) extends Health {
  override val counts = Map(status -> 1)

  override def detailsTree(i: Int): String = s"${indent(i)}${status}: ${message}"
}

/**
  * Created by user on 16/07/2016.
  */
object Health {

  sealed trait Status extends Ordered[Status] {
    val priority: Int

    def combine(status: Status): Status =
      (this, status) match {
        case (Ok, s) => s
        case (s, Ok) => s
        case (Critical, _) => Critical
        case (_, Critical) => Critical
        case (Warning, _) => Warning
        case (_, Warning) => Warning
        case (Unknown, Unknown) => Unknown
      }

    override def compare(that: Status): Int = this.priority.compare(that.priority)

  }

  case object Critical extends Status {
    override val priority = 1
  }

  case object Warning extends Status {
    override val priority = 2
  }

  case object Unknown extends Status {
    override val priority = 3
  }

  case object Ok extends Status {
    override val priority = 4
  }

  val values = Set(Ok, Warning, Critical, Unknown)

  val emptyCounts: Map[Status, Int] = values.map((_, 0)).toMap

  private def doGet(map: Map[Status, Int], s: Status) = map.getOrElse(s, 0)

  private def add(map1: Map[Status, Int], map2: Map[Status, Int], s: Status) =
    doGet(map1, s) + doGet(map2, s)

  def addCounts(map1: Map[Status, Int], map2: Map[Status, Int]): Map[Status, Int] = Map(
    Critical -> add(map1, map2, Critical),
    Warning -> add(map1, map2, Warning),
    Unknown -> add(map1, map2, Unknown),
    Ok -> add(map1, map2, Ok)
  )

  /**
    * A component that can expose its health
    */
  trait HealthStatus {
    def getHealth: Health
  }
}
