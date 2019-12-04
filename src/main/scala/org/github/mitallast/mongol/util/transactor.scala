package org.github.mitallast.mongol.util

import cats.{~>, Monad}
import cats.data.Kleisli
import cats.effect.{Async, Blocker, ContextShift}
import com.mongodb.client.MongoClient
import fs2.Stream
import org.github.mitallast.mongol.free.client.{ClientIO, ClientOp}
import org.github.mitallast.mongol.free.interpreters

object transactor {

  type ClientInterpreter[M[_]] = ClientOp ~> Kleisli[M, MongoClient, *]

  final class Transactor[M[_]] private (client: MongoClient, interpret: ClientInterpreter[M]) { self =>
    def trans(implicit ev: Monad[M]): ClientIO ~> M =
      λ[ClientIO ~> M] { f =>
        f.foldMap(interpret).run(client)
      }

    def transP(implicit ev: Monad[M]): Stream[ClientIO, *] ~> Stream[M, *] =
      λ[Stream[ClientIO, *] ~> Stream[M, *]] { s =>
        s.translate(trans)
      }
  }

  object Transactor {

    def apply[M[_]](kernel: MongoClient, interpret: ClientInterpreter[M]): Transactor[M] =
      new Transactor[M](kernel, interpret)

    object fromClient {
      def apply[M[_]] = new FromClientUnapplied[M]

      class FromClientUnapplied[M[_]] {
        def apply(client: MongoClient, blocker: Blocker)(implicit ev: Async[M], cs: ContextShift[M]): Transactor[M] = {
          val interpreter = interpreters[M](blocker).ClientInterpreter
          Transactor(client, interpreter)
        }
      }
    }
  }
}
