package cakesolutions.kafka.akka

import akka.actor.{Actor, Stash}
import com.typesafe.config.Config

import scala.concurrent.duration._

object Retry {

  sealed trait Interval {
    def duration: FiniteDuration
    def next: Interval
  }

  object Interval {
    import scala.concurrent.duration.{MILLISECONDS => Millis}
    private def durationFromConfig(config: Config, path: String) = Duration(config.getDuration(path, Millis), Millis)

    def apply(config: Config): Interval =
      config.getString("type").toLowerCase match {
        case "linear" => Linear(durationFromConfig(config, "duration"))
        case t => sys.error(s"Unexpected interval type: $t")
      }

    final case class Linear(duration: FiniteDuration) extends Interval {
      def next: Interval = this
    }
  }

  sealed trait Logic {
    def next: Logic
    def isFinished: Boolean
  }

  object Logic {
    def apply(config: Config): Logic =
      config.getString("type").toLowerCase match {
        case "infinite" => Infinite
        case "finitetimes" => FiniteTimes(config.getInt("retryCount"))
        case t => sys.error(s"Unexpected logic type: $t")
      }

    case object Infinite extends Logic {
      def next: Logic = Infinite
      def isFinished: Boolean = false
    }

    final case class FiniteTimes(retryCount: Int) extends Logic {
      require(retryCount >= 0, "Retry count cannot be negative")

      def next: Logic = FiniteTimes(retryCount - 1)
      def isFinished: Boolean = retryCount == 0
    }
  }

  object Strategy {
    def apply(config: Config): Strategy =
      Strategy(
        Interval(config.getConfig("interval")),
        Logic(config.getConfig("logic"))
      )
  }

  final case class Strategy(interval: Interval, retryLogic: Logic) {
    def next: Strategy = Strategy(interval.next, retryLogic.next)
    def isFinished: Boolean = retryLogic.isFinished
    def evaluate[T](f: => T): Result[T] =
      try {
        Result.Success(f)
      } catch {
        case error: Exception =>
          val nextStrategy = this.next
          if (nextStrategy.isFinished) Result.Failure(error)
          else Result.Next(error, nextStrategy)
      }
  }

  sealed trait Result[+T]

  object Result {
    final case class Next(error: Throwable, strategy: Strategy) extends Result[Nothing]
    final case class Failure(error: Throwable) extends Result[Nothing]
    final case class Success[+T](value: T) extends Result[T]
  }
}

private object ActorWithInternalRetry {
  case object CooldownDone
}

private trait ActorWithInternalRetry extends Actor with Stash {

  import ActorWithInternalRetry._
  import Retry._
  import context.dispatcher

  def evaluateUntilFinished(strategy: Strategy, recover: Throwable => Unit)(effect: => Unit): Unit =
    strategy.evaluate(effect) match {
      case Result.Success(_) => unstashAll()
      case Result.Failure(error) => throw error
      case Result.Next(error, nextStrategy) =>
        recover(error)
        scheduleCooldown(nextStrategy)
        context.become(cooldownReceive(nextStrategy, recover, effect), discardOld = false)
    }

  private def scheduleCooldown(strategy: Strategy): Unit =
    context.system.scheduler.scheduleOnce(strategy.interval.duration, self, CooldownDone)

  private def cooldownReceive(strategy: Strategy, recover: Throwable => Unit, effect: => Unit): Receive = {
    case CooldownDone =>
      context.unbecome()
      evaluateUntilFinished(strategy, recover)(effect)

    case _ => stash()
  }
}
