package org.github.mitallast.mongol.free

import cats.effect.Async

trait Types {
  type ClientIO[A] = client.ClientIO[A]
  type SessionIO[A] = session.SessionIO[A]
  type DatabaseIO[A] = database.DatabaseIO[A]
  type CollectionIO[A] = collection.CollectionIO[A]
}

//noinspection TypeAnnotation
trait Modules {
  lazy val CL = client
  lazy val CS = session
  lazy val DB = database
  lazy val DL = collection
}

//noinspection TypeAnnotation
trait Instances {
  implicit lazy val AsyncClientIO: Async[ClientIO] = client.AsyncClientIO
  implicit lazy val AsyncSessionIO: Async[SessionIO] = session.AsyncSessionIO
  implicit lazy val AsyncDatabaseIO: Async[DatabaseIO] = database.AsyncDatabaseIO
  implicit lazy val AsyncCollectionIO: Async[CollectionIO] = collection.AsyncCollectionIO

  implicit lazy val SessionOpEmbeddable = session.SessionOpEmbeddable
  implicit lazy val DatabaseOpEmbeddable = database.DatabaseOpEmbeddable
  implicit lazy val CollectionOpEmbeddable = collection.CollectionOpEmbeddable
}
