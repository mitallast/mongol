package org.github.mitallast.mongol.free

import cats.free.Free
import com.mongodb.client.{ClientSession, MongoCollection, MongoCursor, MongoDatabase}
import org.bson.Document

sealed trait Embedded[A]
object Embedded {
  final case class Session[A](j: ClientSession, fa: SessionIO[A]) extends Embedded[A]
  final case class Database[A](j: MongoDatabase, fa: DatabaseIO[A]) extends Embedded[A]
  final case class Collection[A](j: MongoCollection[Document], fa: CollectionIO[A]) extends Embedded[A]
  final case class Cursor[A](j: MongoCursor[_], fa: CursorIO[A]) extends Embedded[A]
}

trait Embeddable[F[_], -J] {
  def embed[A](j: J, fa: Free[F, A]): Embedded[A]
}
