package de.lightningpayments.lib.csvstreams

import akka.actor.ActorSystem

trait ActorSpec {
  implicit val as: ActorSystem = ActorSystem(
    "test-system"
  )
}
