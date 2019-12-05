package org.github.mitallast.mongol.free

import cats.effect.Async

trait Types {
  type ClientIO[A] = client.ClientIO[A]
  type SessionIO[A] = session.SessionIO[A]
  type DatabaseIO[A] = database.DatabaseIO[A]
  type CollectionIO[A] = collection.CollectionIO[A]
  type CursorIO[A] = cursor.CursorIO[A]
}

//noinspection TypeAnnotation
trait Modules {
  lazy val FC = client
  lazy val FS = session
  lazy val FDB = database
  lazy val FCL = collection
  lazy val FCR = cursor
}

//noinspection TypeAnnotation
trait Instances {
  implicit lazy val AsyncClientIO: Async[ClientIO] = client.AsyncClientIO
  implicit lazy val AsyncSessionIO: Async[SessionIO] = session.AsyncSessionIO
  implicit lazy val AsyncDatabaseIO: Async[DatabaseIO] = database.AsyncDatabaseIO
  implicit lazy val AsyncCollectionIO: Async[CollectionIO] = collection.AsyncCollectionIO
  implicit lazy val AsyncCursorIO: Async[CursorIO] = cursor.AsyncCursorIO

  implicit lazy val SessionOpEmbeddable = session.SessionOpEmbeddable
  implicit lazy val DatabaseOpEmbeddable = database.DatabaseOpEmbeddable
  implicit lazy val CollectionOpEmbeddable = collection.CollectionOpEmbeddable
  implicit lazy val CursorOpEmbeddable = cursor.CursorOpEmbeddable
}
