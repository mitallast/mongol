package org.github.mitallast.mongol.hi

import cats.effect.ExitCase

object session {
  import implicits._

  def transaction[A](app: SessionIO[A]): SessionIO[A] =
    FS.bracketCase(FS.startTransaction)(_ => app) {
      case (_, ExitCase.Completed) => FS.commitTransaction
      case _                       => FS.abortTransaction
    }

  def deferClose[A](app: SessionIO[A]): SessionIO[A] =
    FS.bracketCase(FS.unit)(_ => app)((_, _) => FS.close)

  def database[A](databaseName: String)(databaseIO: DatabaseIO[A]): SessionIO[A] =
    for {
      db <- FS.getDatabase(databaseName)
      a <- FS.embed(db, databaseIO)
    } yield a
}
