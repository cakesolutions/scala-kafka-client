package cakesolutions.kafka.akka

import akka.actor.SupervisorStrategy.{Restart, Resume}
import akka.actor.{Actor, ActorRef, ActorSystem, OneForOneStrategy, Props, SupervisorStrategy}
import akka.testkit.{TestKit, TestProbe}
import cakesolutions.kafka.akka.Retry.Strategy
import org.scalatest.{FlatSpecLike, Matchers}

import scala.concurrent.duration._

class ActorWithInternalRetrySpec(system_ : ActorSystem) extends TestKit(system_) with FlatSpecLike with Matchers {

  import Retry._

  def this() = this(ActorSystem("ActorWithInternalRetrySpec"))

  val defaultStrategy = Strategy(Interval.Linear(0.millis), Logic.Infinite)

  "ActorWithInternalRetry" should "be able to succeed on first try" in {
    val probe = TestProbe()
    val actor = sampleActor(probe.ref)

    probe.send(actor, (0, defaultStrategy))

    probe.expectMsg(List('start, 'success))
  }

  it should "retry if it doesn't succeed" in {
    val probe = TestProbe()
    val actor = sampleActor(probe.ref)

    probe.send(actor, (3, defaultStrategy))

    probe.expectMsg(statesSequence(3, success = true))
  }

  it should "not succeed when it exceeds retries" in {
    val probe = TestProbe()
    val actor = sampleActor(probe.ref)
    val strategy = defaultStrategy.copy(retryLogic = Logic.FiniteTimes(7))

    probe.send(actor, (10, strategy))

    probe.expectMsg('exception)

    probe.send(actor, 'get)

    probe.expectMsg((4, statesSequence(6, success = false)))
  }

  it should "not answer to new messages until it finishes the computation" in {
    val probe = TestProbe()
    val actor = sampleActor(probe.ref)
    val strategy = defaultStrategy.copy(Interval.Linear(100.millis))
    val expectedResult = statesSequence(10, success = true)

    probe.send(actor, (10, strategy))
    probe.send(actor, 'get)

    probe.expectMsg(expectedResult)
    probe.expectMsg((0, expectedResult))
  }

  private def statesSequence(recoveries: Int, success: Boolean) = {
    val finalState = if (success) 'success else 'fail
    'start :: failRecover(recoveries) ++ (finalState :: Nil)
  }

  private def failRecover(times: Int) =
    (1 to times).flatMap(_ => List('fail, 'recover)).toList

  private def sampleActor(nextActor: ActorRef) = system.actorOf(Props(classOf[SampleRetrySupervisor], nextActor))
}

class SampleRetrySupervisor(nextActor: ActorRef) extends Actor {
  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case _: SampleException =>
      nextActor ! 'exception
      Resume
    case _ => Restart
  }

  private val actor = context.actorOf(Props[SampleRetryActor])

  def receive: Receive = {
    case m => actor forward m
  }
}

private class SampleRetryActor extends ActorWithInternalRetry {

  // state that makes `doStuff` fail. recovery attempts to "correct" the state.
  private var failTimes: Int = 0

  // log of states
  private var states: List[Symbol] = Nil

  def receive: Receive = {
    case (timesToFail: Int, strategy: Strategy) =>
      failTimes = timesToFail
      pushState('start)
      val s = sender()
      evaluateUntilFinished(strategy, recovery) {
        doStuff(s)
      }

    case 'get => sender() ! (failTimes, getState)
  }

  private def pushState(state: Symbol): Unit = {
    states = state :: states
  }

  private def recovery(t: Throwable): Unit = {
    failTimes -= 1
    pushState('recover)
  }

  private def doStuff(ref: ActorRef): Unit =
    if (failTimes == 0) {
      pushState('success)
      ref ! getState
    } else {
      pushState('fail)
      throw new SampleException
    }

  private def getState = states.reverse
}

class SampleException extends Exception