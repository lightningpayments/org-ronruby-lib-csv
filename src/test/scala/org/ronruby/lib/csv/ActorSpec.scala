package org.ronruby.lib.csv

import akka.actor.ActorSystem

trait ActorSpec {
  implicit val as: ActorSystem = ActorSystem(
    "test-system"
  )
}
