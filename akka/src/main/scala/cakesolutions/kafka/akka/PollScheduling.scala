package com.pirum.akka

import akka.actor.{ActorLogging, Actor, Cancellable}

import scala.concurrent.duration.FiniteDuration

private object PollScheduling {

  /** Internal poll trigger
    *
    * @param correlationId unique correlation id
    * @param timeout Kafka blocking timeout.  Usually we don't want to block when polling the driver for new messages (default 0),
    *                but in some cases we want to apply a small timeout to reduce poll latency.  Specifically when we are
    *                consuming faster than we are pulling from Kafka, but there is still a backlog to get through.
    */
  final case class Poll(correlationId: Long, timeout: Int = 0)
}

private[akka] trait PollScheduling extends ActorLogging {
  self: Actor =>

  import PollScheduling._

  // Tracks identifier of most recent poll request, so we can ignore old ones.
  private var lastCorrelationId = 0L

  // Handle on the poll schedule cancellable (cancelled by default).
  private var pollCancellable = new Cancellable {
    override def isCancelled: Boolean = true
    override def cancel(): Boolean = false
  }

  private def nextPoll(timeout: Int) = {
    lastCorrelationId += 1
    Poll(lastCorrelationId, timeout)
  }

  /** Match poll messages's correlation ID against the last sent correlation ID.
    * Used for ensuring that poll messages don't build up.
    *
    * @param poll the poll message to compare to current correlation ID
    * @return true when message matches the last known correlation ID, and false otherwise
    */
  protected def isCurrentPoll(poll: Poll): Boolean =
    poll.correlationId == lastCorrelationId

  /** Match poll messages's correlation ID against the last sent correlation ID.
    * Used for ensuring that poll messages don't build up.
    *
    * @param id the correlation ID to compare to current correlation ID
    * @return true when message matches the last known correlation ID, and false otherwise
    */
  protected def isCurrentPoll(id: Long): Boolean =
    id == lastCorrelationId

  /** Schedule poll immediately.
    *
    * @param timeout Kafka blocking timeout
    */
  protected def pollImmediate(timeout: Int = 0): Unit = {
    log.debug("Poll immediate with {} timeout", timeout)
    cancelPoll()
    context.self ! nextPoll(timeout)
  }

  /** Schedule a poll to occur after a given timeout. The poll will not block the Kafka driver (blocking timeout = 0).
    *
    * @param timeout the time after which a poll is scheduled to occur
    */
  protected def schedulePoll(timeout: FiniteDuration): Unit = {
    log.debug("Schedule poll to occur after {}", timeout)
    import context.dispatcher
    cancelPoll()
    pollCancellable =
      context.system.scheduler.scheduleOnce(timeout, context.self, nextPoll(0))
  }

  protected def cancelPoll(): Unit = pollCancellable.cancel()
}
