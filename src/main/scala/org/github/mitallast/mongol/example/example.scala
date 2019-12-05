package org.github.mitallast.mongol.example

import cats.effect.{Blocker, ContextShift, IO}
import fs2.Stream
import com.mongodb.client.MongoClients
import org.github.mitallast.mongol._
import org.github.mitallast.mongol.syntax.all.toClientIOOpsOps
import org.github.mitallast.mongol.util.transactor.Transactor

import scala.concurrent.ExecutionContext

object example extends App {

  private val ec = ExecutionContext.global
  private implicit val shift: ContextShift[IO] = IO.contextShift(ec)
  private val blocker = Blocker.liftExecutionContext(ec)

  private val xa = Transactor.fromClient[IO](MongoClients.create(), blocker)

  private val app: ClientIO[Unit] = for {
    _ <- HC.session {
      for {
        has <- FS.hasActiveTransaction
        _ <- FS.delay(println(s"has active transaction: $has"))
        _ <- HS.database("test") {
          for {
            name <- FDB.getName
            _ <- FDB.delay(println(s"database: $name"))
            collectionNames <- HDB.collectionNames.compile.to[Vector]
            _ <- FDB.delay(println(s"collectionNames: $collectionNames"))
          } yield ()
        }
      } yield ()
    }
  } yield ()

  app.transact(xa).unsafeRunSync()

  private val app2: Stream[ClientIO, String] =
    for {
      a <- HC.session {
        for {
          has <- Stream.eval(FS.hasActiveTransaction)
          _ <- Stream.eval(FS.delay(println(s"has active transaction: $has")))
          a <- HS.databaseP("test") {
            for {
              a <- HDB.collectionNames
              _ <- Stream.eval(FDB.delay(println(s"collectionName: $a")))
            } yield a
          }
        } yield a
      }
    } yield a

  xa.transP.apply(app2).compile.to[Vector].unsafeRunSync()
}
