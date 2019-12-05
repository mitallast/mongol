package org.github.mitallast.mongol.hi

import cats.~>
import fs2.Stream

import com.mongodb.client.ClientSession

object client {

  private def translator(session: ClientSession): SessionIO ~> ClientIO =
    λ[SessionIO ~> ClientIO] { fa =>
      FC.embed(session, fa)
    }

  def session[A](fa: SessionIO[A]): ClientIO[A] =
    for {
      session <- FC.startSession
      a <- FC.embed(session, HS.deferClose(fa))
    } yield a

  def session[A](fa: Stream[SessionIO, A]): Stream[ClientIO, A] =
    for {
      session <- Stream.eval(FC.startSession)
      a <- fa.translate(translator(session))
    } yield a
}
