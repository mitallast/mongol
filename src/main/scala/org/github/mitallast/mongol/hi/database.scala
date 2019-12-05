package org.github.mitallast.mongol.hi

import cats.Foldable
import cats.implicits._
import cats.effect.syntax.bracket._
import fs2.Stream
import com.mongodb.client.{MongoCursor, MongoIterable}
import org.github.mitallast.mongol.util.stream.repeatEvalChunks

object database {
  import implicits._

  def stream[A](acquire: DatabaseIO[MongoIterable[A]], chunkSize: Int): Stream[DatabaseIO, A] =
    for {
      iterable <- Stream.eval(acquire)
      _ <- Stream.eval(FDB.delay(iterable.batchSize(chunkSize)))
      cr <- Stream.eval(FDB.delay(iterable.cursor().asInstanceOf[MongoCursor[_]]))
      a <- repeatEvalChunks(FDB.embed(cr, cursor.getNextChunk[A](chunkSize)))
    } yield a

  def collectionNames: Stream[DatabaseIO, String] =
    stream(FDB.listCollectionNames, 512)
}
