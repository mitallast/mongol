package org.github.mitallast.mongol.example

import cats.effect.{Blocker, ContextShift, IO}
import com.mongodb.client.MongoClients
import org.github.mitallast.mongol._
import org.github.mitallast.mongol.syntax.all.toClientIOOpsOps
import org.github.mitallast.mongol.implicits._
import org.github.mitallast.mongol.util.transactor.Transactor

import scala.concurrent.ExecutionContext

object example extends App {

  private val ec = ExecutionContext.global
  private implicit val shift: ContextShift[IO] = IO.contextShift(ec)
  private val blocker = Blocker.liftExecutionContext(ec)

  private val xa = Transactor.fromClient[IO](MongoClients.create(), blocker)

  private val app: ClientIO[Unit] = for {
    _ <- CL.transaction {
      for {
        has <- CS.hasActiveTransaction
        _ <- CS.delay(println(s"has active transaction: $has"))
        _ <- CS.database("test") {
          for {
            name <- DB.getName
            _ <- DB.delay(println(s"db name: $name"))
          } yield ()
        }
      } yield ()
    }
  } yield ()

  app.transact(xa).unsafeRunSync()
}
