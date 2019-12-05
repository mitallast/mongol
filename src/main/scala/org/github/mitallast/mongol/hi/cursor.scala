package org.github.mitallast.mongol.hi

import cats.Foldable
import cats.implicits._
import cats.effect.syntax.bracket._
import com.mongodb.client.MongoCursor
import fs2.{Chunk, Stream}

import scala.collection.Factory

object cursor {
  import implicits._

  def delay[A](f: => A): CursorIO[A] =
    FCR.delay(f)

  def build[F[_], A](implicit F: Factory[A, F[A]]): CursorIO[F[A]] =
    FCR.raw { cursor: MongoCursor[A] =>
      val b = F.newBuilder
      while (cursor.hasNext) {
        b += cursor.next()
      }
      b.result()
    }

  def vector[A]: CursorIO[Vector[A]] =
    build[Vector, A]

  def getNextChunk[A](chunkSize: Int): CursorIO[Chunk[A]] =
    FCR.raw { cursor: MongoCursor[A] =>
      var n = chunkSize
      val b = Vector.newBuilder[A]
      while (n > 0 && cursor.hasNext) {
        b += cursor.next()
        n -= 1
      }
      Chunk.vector(b.result())
    }
}
