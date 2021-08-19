package com.bluelabs.s3stream.impl

import akka.NotUsed
import akka.stream.scaladsl.Source
import scala.concurrent.Future

private[s3stream] object StreamUtils {
  def counter(initial: Int = 0): Source[Int, NotUsed] = {
    Source.unfold(initial)((i: Int) => Some((i + 1, i)))
  }

  /** The following two methods are copied from
    * https://github.com/MfgLabs/akka-stream-extensions
    *
    * Licensed under Apache-2
    * https://github.com/MfgLabs/akka-stream-extensions/blob/master/LICENSE
    */
  def singleLazyAsync[A](fut: => Future[A]): Source[A, NotUsed] =
    singleLazy(fut).mapAsync(1)(identity)
  def singleLazy[A](a: => A): Source[A, NotUsed] =
    Source.single(() => a).map(_())
}
