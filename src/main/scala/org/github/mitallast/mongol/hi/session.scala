package org.github.mitallast.mongol.hi

import cats.effect.ExitCase
import cats.~>
import com.mongodb.client.MongoDatabase
import fs2.Stream

object session {
  import implicits._

  private def translator(db: MongoDatabase): DatabaseIO ~> SessionIO =
    λ[DatabaseIO ~> SessionIO] { fa =>
      FS.embed(db, fa)
    }

  def transaction[A](fa: SessionIO[A]): SessionIO[A] =
    FS.bracketCase(FS.startTransaction)(_ => fa) {
      case (_, ExitCase.Completed) => FS.commitTransaction
      case _                       => FS.abortTransaction
    }

  def transaction[A](fa: Stream[SessionIO, A]): Stream[SessionIO, A] =
    Stream
      .bracketCase(FS.startTransaction) {
        case (_, ExitCase.Completed) => FS.commitTransaction
        case _                       => FS.abortTransaction
      }
      .flatMap(_ => fa)

  def deferClose[A](app: SessionIO[A]): SessionIO[A] =
    FS.bracketCase(FS.unit)(_ => app)((_, _) => FS.close)

  def database[A](databaseName: String)(databaseIO: DatabaseIO[A]): SessionIO[A] =
    for {
      db <- FS.getDatabase(databaseName)
      a <- FS.embed(db, databaseIO)
    } yield a

  def databaseP[A](databaseName: String)(fa: Stream[DatabaseIO, A]): Stream[SessionIO, A] =
    for {
      db <- Stream.eval(FS.getDatabase(databaseName))
      a <- fa.translate(translator(db))
    } yield a
}
