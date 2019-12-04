package org.github.mitallast.mongol.free

import cats.~>
import cats.effect.{Async, ContextShift, ExitCase}
import cats.free.{Free => FF}
import com.mongodb.bulk.BulkWriteResult
import com.mongodb.{MongoNamespace, ReadConcern, ReadPreference, WriteConcern}
import com.mongodb.client._
import com.mongodb.client.model._
import com.mongodb.client.result.{DeleteResult, UpdateResult}
import org.bson.Document
import org.bson.conversions.Bson

import scala.concurrent.ExecutionContext

object collection { module =>
  sealed trait CollectionOp[A] {
    def visit[F[_]](v: CollectionOp.Visitor[F]): F[A]
  }

  type CollectionIO[A] = FF[CollectionOp, A]

  object CollectionOp {

    trait Visitor[F[_]] extends (CollectionOp ~> F) {
      final def apply[A](fa: CollectionOp[A]): F[A] = fa.visit(this)

      // Common
      def embed[A](e: Embedded[A]): F[A]
      def delay[A](a: => A): F[A]
      def handleErrorWith[A](fa: CollectionIO[A], f: Throwable => CollectionIO[A]): F[A]
      def raiseError[A](e: Throwable): F[A]
      def async[A](k: (Either[Throwable, A] => Unit) => Unit): F[A]
      def asyncF[A](k: (Either[Throwable, A] => Unit) => CollectionIO[Unit]): F[A]
      def bracketCase[A, B](acquire: CollectionIO[A])(use: A => CollectionIO[B])(
        release: (A, ExitCase[Throwable]) => CollectionIO[Unit]
      ): F[B]
      def shift: F[Unit]
      def evalOn[A](ec: ExecutionContext)(fa: CollectionIO[A]): F[A]

      // Collection
      def getNamespace: F[MongoNamespace]
      def getReadPreference: F[ReadPreference]
      def getWriteConcern: F[WriteConcern]
      def getReadConcern: F[ReadConcern]

      def setReadPreference(readPreference: ReadPreference): F[Unit]
      def setWriteConcern(writeConcern: WriteConcern): F[Unit]
      def setReadConcern(readConcern: ReadConcern): F[Unit]

      def countDocuments(): F[Long]
      def countDocuments(filter: Bson): F[Long]
      def countDocuments(filter: Bson, options: CountOptions): F[Long]

      def estimatedDocumentCount(): F[Long]
      def estimatedDocumentCount(options: EstimatedDocumentCountOptions): F[Long]

      def distinct(fieldName: String): F[DistinctIterable[Bson]]
      def distinct(fieldName: String, filter: Bson): F[DistinctIterable[Bson]]

      def find(): F[FindIterable[Document]]
      def find(filter: Bson): F[FindIterable[Document]]

      def aggregate(pipeline: Seq[Bson]): F[AggregateIterable[Document]]

      def watch(): F[ChangeStreamIterable[Document]]
      def watch(pipeline: Seq[Bson]): F[ChangeStreamIterable[Document]]

      def mapReduce(mapFunction: String, reduceFunction: String): F[MapReduceIterable[Document]]

      def bulkWrite(requests: Seq[WriteModel[_ <: Document]]): F[BulkWriteResult]
      def bulkWrite(requests: Seq[WriteModel[_ <: Document]], options: BulkWriteOptions): F[BulkWriteResult]

      def insertOne(document: Document): F[Unit]
      def insertOne(document: Document, options: InsertOneOptions): F[Unit]

      def insertMany(documents: Seq[_ <: Document]): F[Unit]
      def insertMany(documents: Seq[_ <: Document], options: InsertManyOptions): F[Unit]

      def deleteOne(filter: Bson): F[DeleteResult]
      def deleteOne(filter: Bson, options: DeleteOptions): F[DeleteResult]

      def deleteMany(filter: Bson): F[DeleteResult]
      def deleteMany(filter: Bson, options: DeleteOptions): F[DeleteResult]

      def replaceOne(filter: Bson, replacement: Document): F[UpdateResult]
      def replaceOne(filter: Bson, replacement: Document, options: ReplaceOptions): F[UpdateResult]

      def updateOne(filter: Bson, update: Bson): F[UpdateResult]
      def updateOne(filter: Bson, update: Bson, options: UpdateOptions): F[UpdateResult]

      def updateMany(filter: Bson, update: Bson): F[UpdateResult]
      def updateMany(filter: Bson, update: Bson, options: UpdateOptions): F[UpdateResult]

      def findOneAndDelete(filter: Bson): F[Option[Document]]
      def findOneAndDelete(filter: Bson, options: FindOneAndDeleteOptions): F[Option[Document]]

      def findOneAndReplace(filter: Bson, replacement: Document): F[Option[Document]]
      def findOneAndReplace(filter: Bson, replacement: Document, options: FindOneAndReplaceOptions): F[Option[Document]]

      def findOneAndUpdate(filter: Bson, update: Bson): F[Option[Document]]
      def findOneAndUpdate(filter: Bson, update: Bson, options: FindOneAndUpdateOptions): F[Option[Document]]

      def drop(): F[Unit]
      def createIndex(keys: Bson): F[String]
      def createIndex(keys: Bson, options: IndexOptions): F[String]

      def createIndexes(indexes: Seq[IndexModel]): F[Vector[String]]
      def createIndexes(indexes: Seq[IndexModel], options: CreateIndexOptions): F[Vector[String]]

      def listIndexes(): F[ListIndexesIterable[Document]]

      def dropIndex(indexName: String): F[Unit]
      def dropIndex(indexName: String, options: DropIndexOptions): F[Unit]
      def dropIndex(bson: Bson): F[Unit]
      def dropIndex(bson: Bson, options: DropIndexOptions): F[Unit]

      def dropIndexes(): F[Unit]
      def dropIndexes(options: DropIndexOptions): F[Unit]

      def renameCollection(namespace: MongoNamespace): F[Unit]
      def renameCollection(namespace: MongoNamespace, options: RenameCollectionOptions): F[Unit]
    }

    // Common operations for all algebras
    final case class Embed[A](e: Embedded[A]) extends CollectionOp[A] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[A] = v.embed(e)
    }
    final case class Delay[A](a: () => A) extends CollectionOp[A] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[A] = v.delay(a())
    }
    final case class HandleErrorWith[A](fa: CollectionIO[A], f: Throwable => CollectionIO[A]) extends CollectionOp[A] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[A] = v.handleErrorWith(fa, f)
    }
    final case class RaiseError[A](e: Throwable) extends CollectionOp[A] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[A] = v.raiseError(e)
    }
    final case class Async1[A](k: (Either[Throwable, A] => Unit) => Unit) extends CollectionOp[A] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[A] = v.async(k)
    }
    final case class AsyncF[A](k: (Either[Throwable, A] => Unit) => CollectionIO[Unit]) extends CollectionOp[A] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[A] = v.asyncF(k)
    }
    final case class BracketCase[A, B](
      acquire: CollectionIO[A],
      use: A => CollectionIO[B],
      release: (A, ExitCase[Throwable]) => CollectionIO[Unit]
    ) extends CollectionOp[B] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[B] = v.bracketCase(acquire)(use)(release)
    }
    final case object Shift extends CollectionOp[Unit] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[Unit] = v.shift
    }
    final case class EvalOn[A](ec: ExecutionContext, fa: CollectionIO[A]) extends CollectionOp[A] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[A] = v.evalOn(ec)(fa)
    }

    // Collection-specific operations
    final case object GetNamespace extends CollectionOp[MongoNamespace] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[MongoNamespace] = v.getNamespace
    }
    final case object GetReadPreference extends CollectionOp[ReadPreference] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[ReadPreference] = v.getReadPreference
    }
    final case object GetWriteConcern extends CollectionOp[WriteConcern] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[WriteConcern] = v.getWriteConcern
    }
    final case object GetReadConcern extends CollectionOp[ReadConcern] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[ReadConcern] = v.getReadConcern
    }

    final case class SetReadPreference(readPreference: ReadPreference) extends CollectionOp[Unit] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[Unit] = v.setReadPreference(readPreference)
    }
    final case class SetWriteConcern(writeConcern: WriteConcern) extends CollectionOp[Unit] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[Unit] = v.setWriteConcern(writeConcern)
    }
    final case class SetReadConcern(readConcern: ReadConcern) extends CollectionOp[Unit] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[Unit] = v.setReadConcern(readConcern)
    }

    final case object CountDocuments extends CollectionOp[Long] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[Long] = v.countDocuments()
    }
    final case class CountDocumentsFilter(filter: Bson) extends CollectionOp[Long] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[Long] = v.countDocuments(filter)
    }
    final case class CountDocumentsFilterOptions(filter: Bson, options: CountOptions) extends CollectionOp[Long] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[Long] = v.countDocuments(filter, options)
    }

    final case object EstimatedDocumentCount extends CollectionOp[Long] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[Long] = v.estimatedDocumentCount()
    }
    final case class EstimatedDocumentCountWithOptions(options: EstimatedDocumentCountOptions)
        extends CollectionOp[Long] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[Long] = v.estimatedDocumentCount(options)
    }

    final case class Distinct(fieldName: String) extends CollectionOp[DistinctIterable[Bson]] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[DistinctIterable[Bson]] = v.distinct(fieldName)
    }
    final case class DistinctFilter(fieldName: String, filter: Bson) extends CollectionOp[DistinctIterable[Bson]] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[DistinctIterable[Bson]] = v.distinct(fieldName, filter)
    }

    final case object Find extends CollectionOp[FindIterable[Document]] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[FindIterable[Document]] = v.find()
    }
    final case class FindFilter(filter: Bson) extends CollectionOp[FindIterable[Document]] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[FindIterable[Document]] = v.find(filter)
    }

    final case class Aggregate(pipeline: Seq[Bson]) extends CollectionOp[AggregateIterable[Document]] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[AggregateIterable[Document]] = v.aggregate(pipeline)
    }

    final case object Watch extends CollectionOp[ChangeStreamIterable[Document]] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[ChangeStreamIterable[Document]] = v.watch()
    }
    final case class WatchPipeline(pipeline: Seq[Bson]) extends CollectionOp[ChangeStreamIterable[Document]] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[ChangeStreamIterable[Document]] = v.watch(pipeline)
    }

    final case class MapReduce(map: String, reduce: String) extends CollectionOp[MapReduceIterable[Document]] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[MapReduceIterable[Document]] = v.mapReduce(map, reduce)
    }

    final case class BulkWrite(requests: Seq[WriteModel[_ <: Document]]) extends CollectionOp[BulkWriteResult] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[BulkWriteResult] = v.bulkWrite(requests)
    }
    final case class BulkWriteWithOptions(requests: Seq[WriteModel[_ <: Document]], options: BulkWriteOptions)
        extends CollectionOp[BulkWriteResult] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[BulkWriteResult] = v.bulkWrite(requests, options)
    }

    final case class InsertOne(document: Document) extends CollectionOp[Unit] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[Unit] = v.insertOne(document)
    }
    final case class InsertOneWithOptions(document: Document, options: InsertOneOptions) extends CollectionOp[Unit] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[Unit] = v.insertOne(document, options)
    }

    final case class InsertMany(documents: Seq[_ <: Document]) extends CollectionOp[Unit] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[Unit] = v.insertMany(documents)
    }
    final case class InsertManyWithOptions(documents: Seq[_ <: Document], options: InsertManyOptions)
        extends CollectionOp[Unit] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[Unit] = v.insertMany(documents, options)
    }

    final case class DeleteOne(filter: Bson) extends CollectionOp[DeleteResult] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[DeleteResult] = v.deleteOne(filter)
    }
    final case class DeleteOneOptions(filter: Bson, options: DeleteOptions) extends CollectionOp[DeleteResult] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[DeleteResult] = v.deleteOne(filter, options)
    }

    final case class DeleteMany(filter: Bson) extends CollectionOp[DeleteResult] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[DeleteResult] = v.deleteMany(filter)
    }
    final case class DeleteManyOptions(filter: Bson, options: DeleteOptions) extends CollectionOp[DeleteResult] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[DeleteResult] = v.deleteMany(filter, options)
    }

    final case class ReplaceOne(filter: Bson, replacement: Document) extends CollectionOp[UpdateResult] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[UpdateResult] = v.replaceOne(filter, replacement)
    }
    final case class ReplaceOneOptions(filter: Bson, replacement: Document, options: ReplaceOptions)
        extends CollectionOp[UpdateResult] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[UpdateResult] = v.replaceOne(filter, replacement, options)
    }

    final case class UpdateOne(filter: Bson, update: Bson) extends CollectionOp[UpdateResult] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[UpdateResult] = v.updateOne(filter, update)
    }
    final case class UpdateOneOptions(filter: Bson, update: Bson, options: UpdateOptions)
        extends CollectionOp[UpdateResult] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[UpdateResult] = v.updateOne(filter, update, options)
    }

    final case class UpdateMany(filter: Bson, update: Bson) extends CollectionOp[UpdateResult] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[UpdateResult] = v.updateMany(filter, update)
    }
    final case class UpdateManyOptions(filter: Bson, update: Bson, options: UpdateOptions)
        extends CollectionOp[UpdateResult] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[UpdateResult] = v.updateMany(filter, update, options)
    }

    final case class FindOneAndDelete(filter: Bson) extends CollectionOp[Option[Document]] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[Option[Document]] = v.findOneAndDelete(filter)
    }
    final case class FindOneAndDeleteWithOptions(filter: Bson, options: FindOneAndDeleteOptions)
        extends CollectionOp[Option[Document]] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[Option[Document]] = v.findOneAndDelete(filter, options)
    }

    final case class FindOneAndReplace(filter: Bson, replacement: Document) extends CollectionOp[Option[Document]] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[Option[Document]] =
        v.findOneAndReplace(filter, replacement)
    }
    final case class FindOneAndReplaceWithOptions(
      filter: Bson,
      replacement: Document,
      options: FindOneAndReplaceOptions
    ) extends CollectionOp[Option[Document]] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[Option[Document]] =
        v.findOneAndReplace(filter, replacement, options)
    }

    final case class FindOneAndUpdate(filter: Bson, update: Bson) extends CollectionOp[Option[Document]] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[Option[Document]] =
        v.findOneAndUpdate(filter, update)
    }
    final case class FindOneAndUpdateWithOptions(filter: Bson, update: Bson, options: FindOneAndUpdateOptions)
        extends CollectionOp[Option[Document]] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[Option[Document]] =
        v.findOneAndUpdate(filter, update, options)
    }

    final case object Drop extends CollectionOp[Unit] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[Unit] = v.drop()
    }

    final case class CreateIndex(keys: Bson) extends CollectionOp[String] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[String] = v.createIndex(keys)
    }
    final case class CreateIndexWithOptions(keys: Bson, options: IndexOptions) extends CollectionOp[String] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[String] = v.createIndex(keys, options)
    }

    final case class CreateIndexes(indexes: Seq[IndexModel]) extends CollectionOp[Vector[String]] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[Vector[String]] = v.createIndexes(indexes)
    }
    final case class CreateIndexesOptions(indexes: Seq[IndexModel], options: CreateIndexOptions)
        extends CollectionOp[Vector[String]] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[Vector[String]] = v.createIndexes(indexes, options)
    }

    final case object ListIndexes extends CollectionOp[ListIndexesIterable[Document]] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[ListIndexesIterable[Document]] = v.listIndexes()
    }

    final case class DropIndexByName(indexName: String) extends CollectionOp[Unit] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[Unit] = v.dropIndex(indexName)
    }
    final case class DropIndexByNameOptions(indexName: String, options: DropIndexOptions) extends CollectionOp[Unit] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[Unit] = v.dropIndex(indexName, options)
    }
    final case class DropIndexByBson(bson: Bson) extends CollectionOp[Unit] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[Unit] = v.dropIndex(bson)
    }
    final case class DropIndexByBsonOptions(bson: Bson, options: DropIndexOptions) extends CollectionOp[Unit] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[Unit] = v.dropIndex(bson, options)
    }

    final case object DropIndexes extends CollectionOp[Unit] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[Unit] = v.dropIndexes()
    }
    final case class DropIndexesOptions(options: DropIndexOptions) extends CollectionOp[Unit] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[Unit] = v.dropIndexes(options)
    }

    final case class RenameCollection(namespace: MongoNamespace) extends CollectionOp[Unit] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[Unit] = v.renameCollection(namespace)
    }
    final case class RenameCollectionWithOptions(namespace: MongoNamespace, options: RenameCollectionOptions)
        extends CollectionOp[Unit] {
      override def visit[F[_]](v: CollectionOp.Visitor[F]): F[Unit] = v.renameCollection(namespace, options)
    }
  }

  import CollectionOp._

  // Smart constructors for operations common to all algebras
  val unit: CollectionIO[Unit] =
    FF.pure(())
  def pure[A](a: A): CollectionIO[A] =
    FF.pure(a)
  def embed[F[_], J, A](j: J, fa: FF[F, A])(implicit ev: Embeddable[F, J]): CollectionIO[A] =
    FF.liftF(Embed(ev.embed(j, fa)))
  def delay[A](a: => A): CollectionIO[A] =
    FF.liftF(Delay(() => a))
  def handleErrorWith[A](fa: CollectionIO[A], f: Throwable => CollectionIO[A]): CollectionIO[A] =
    FF.liftF(HandleErrorWith(fa, f))
  def raiseError[A](err: Throwable): CollectionIO[A] =
    FF.liftF(RaiseError(err))
  def async[A](k: (Either[Throwable, A] => Unit) => Unit): CollectionIO[A] =
    FF.liftF(Async1(k))
  def asyncF[A](k: (Either[Throwable, A] => Unit) => CollectionIO[Unit]): CollectionIO[A] =
    FF.liftF(AsyncF(k))
  def bracketCase[A, B](
    acquire: CollectionIO[A]
  )(use: A => CollectionIO[B])(release: (A, ExitCase[Throwable]) => CollectionIO[Unit]): CollectionIO[B] =
    FF.liftF(BracketCase(acquire, use, release))
  val shift: CollectionIO[Unit] =
    FF.liftF(Shift)
  def evalOn[A](ec: ExecutionContext)(fa: CollectionIO[A]): CollectionIO[A] =
    FF.liftF(EvalOn(ec, fa))

  // Smart constructors for collection-specific operations
  def getNamespace: CollectionIO[MongoNamespace] =
    FF.liftF(GetNamespace)
  def getReadPreference: CollectionIO[ReadPreference] =
    FF.liftF(GetReadPreference)
  def getWriteConcern: CollectionIO[WriteConcern] =
    FF.liftF(GetWriteConcern)
  def getReadConcern: CollectionIO[ReadConcern] =
    FF.liftF(GetReadConcern)
  def setReadPreference(readPreference: ReadPreference): CollectionIO[Unit] =
    FF.liftF(SetReadPreference(readPreference))
  def setWriteConcern(writeConcern: WriteConcern): CollectionIO[Unit] =
    FF.liftF(SetWriteConcern(writeConcern))
  def setReadConcern(readConcern: ReadConcern): CollectionIO[Unit] =
    FF.liftF(SetReadConcern(readConcern))
  def countDocuments(): CollectionIO[Long] =
    FF.liftF(CountDocuments)
  def countDocuments(filter: Bson): CollectionIO[Long] =
    FF.liftF(CountDocumentsFilter(filter))
  def countDocuments(filter: Bson, options: CountOptions): CollectionIO[Long] =
    FF.liftF(CountDocumentsFilterOptions(filter, options))
  def estimatedDocumentCount(): CollectionIO[Long] =
    FF.liftF(EstimatedDocumentCount)
  def estimatedDocumentCount(options: EstimatedDocumentCountOptions): CollectionIO[Long] =
    FF.liftF(EstimatedDocumentCountWithOptions(options))
  def distinct(fieldName: String): CollectionIO[DistinctIterable[Bson]] =
    FF.liftF(Distinct(fieldName))
  def distinct(fieldName: String, filter: Bson): CollectionIO[DistinctIterable[Bson]] =
    FF.liftF(DistinctFilter(fieldName, filter))
  def find(): CollectionIO[FindIterable[Document]] =
    FF.liftF(Find)
  def find(filter: Bson): CollectionIO[FindIterable[Document]] =
    FF.liftF(FindFilter(filter))
  def aggregate(pipeline: Seq[Bson]): CollectionIO[AggregateIterable[Document]] =
    FF.liftF(Aggregate(pipeline))
  def watch(): CollectionIO[ChangeStreamIterable[Document]] =
    FF.liftF(Watch)
  def watch(pipeline: Seq[Bson]): CollectionIO[ChangeStreamIterable[Document]] =
    FF.liftF(WatchPipeline(pipeline))
  def mapReduce(mapFunction: String, reduceFunction: String): CollectionIO[MapReduceIterable[Document]] =
    FF.liftF(MapReduce(mapFunction, reduceFunction))
  def bulkWrite(requests: Seq[WriteModel[_ <: Document]]): CollectionIO[BulkWriteResult] =
    FF.liftF(BulkWrite(requests))
  def bulkWrite(requests: Seq[WriteModel[_ <: Document]], options: BulkWriteOptions): CollectionIO[BulkWriteResult] =
    FF.liftF(BulkWriteWithOptions(requests, options))
  def insertOne(document: Document): CollectionIO[Unit] =
    FF.liftF(InsertOne(document))
  def insertOne(document: Document, options: InsertOneOptions): CollectionIO[Unit] =
    FF.liftF(InsertOneWithOptions(document, options))
  def insertMany(documents: Seq[Document]): CollectionIO[Unit] =
    FF.liftF(InsertMany(documents))
  def insertMany(documents: Seq[Document], options: InsertManyOptions): CollectionIO[Unit] =
    FF.liftF(InsertManyWithOptions(documents, options))
  def deleteOne(filter: Bson): CollectionIO[DeleteResult] =
    FF.liftF(DeleteOne(filter))
  def deleteOne(filter: Bson, options: DeleteOptions): CollectionIO[DeleteResult] =
    FF.liftF(DeleteOneOptions(filter, options))
  def deleteMany(filter: Bson): CollectionIO[DeleteResult] =
    FF.liftF(DeleteMany(filter))
  def deleteMany(filter: Bson, options: DeleteOptions): CollectionIO[DeleteResult] =
    FF.liftF(DeleteManyOptions(filter, options))
  def replaceOne(filter: Bson, replacement: Document): CollectionIO[UpdateResult] =
    FF.liftF(ReplaceOne(filter, replacement))
  def replaceOne(filter: Bson, replacement: Document, options: ReplaceOptions): CollectionIO[UpdateResult] =
    FF.liftF(ReplaceOneOptions(filter, replacement, options))
  def updateOne(filter: Bson, update: Bson): CollectionIO[UpdateResult] =
    FF.liftF(UpdateOne(filter, update))
  def updateOne(filter: Bson, update: Bson, options: UpdateOptions): CollectionIO[UpdateResult] =
    FF.liftF(UpdateOneOptions(filter, update, options))
  def updateMany(filter: Bson, update: Bson): CollectionIO[UpdateResult] =
    FF.liftF(UpdateMany(filter, update))
  def updateMany(filter: Bson, update: Bson, options: UpdateOptions): CollectionIO[UpdateResult] =
    FF.liftF(UpdateManyOptions(filter, update, options))
  def findOneAndDelete(filter: Bson): CollectionIO[Option[Document]] =
    FF.liftF(FindOneAndDelete(filter))
  def findOneAndDelete(filter: Bson, options: FindOneAndDeleteOptions): CollectionIO[Option[Document]] =
    FF.liftF(FindOneAndDeleteWithOptions(filter, options))
  def findOneAndReplace(filter: Bson, replacement: Document): CollectionIO[Option[Document]] =
    FF.liftF(FindOneAndReplace(filter, replacement))
  def findOneAndReplace(
    filter: Bson,
    replacement: Document,
    options: FindOneAndReplaceOptions
  ): CollectionIO[Option[Document]] =
    FF.liftF(FindOneAndReplaceWithOptions(filter, replacement, options))
  def findOneAndUpdate(filter: Bson, update: Bson): CollectionIO[Option[Document]] =
    FF.liftF(FindOneAndUpdate(filter, update))
  def findOneAndUpdate(filter: Bson, update: Bson, options: FindOneAndUpdateOptions): CollectionIO[Option[Document]] =
    FF.liftF(FindOneAndUpdateWithOptions(filter, update, options))
  def drop(): CollectionIO[Unit] =
    FF.liftF(Drop)
  def createIndex(keys: Bson): CollectionIO[String] =
    FF.liftF(CreateIndex(keys))
  def createIndex(keys: Bson, options: IndexOptions): CollectionIO[String] =
    FF.liftF(CreateIndexWithOptions(keys, options))
  def createIndexes(indexes: Seq[IndexModel]): CollectionIO[Vector[String]] =
    FF.liftF(CreateIndexes(indexes))
  def createIndexes(indexes: Seq[IndexModel], options: CreateIndexOptions): CollectionIO[Vector[String]] =
    FF.liftF(CreateIndexesOptions(indexes, options))
  def listIndexes(): CollectionIO[ListIndexesIterable[Document]] =
    FF.liftF(ListIndexes)
  def dropIndex(indexName: String): CollectionIO[Unit] =
    FF.liftF(DropIndexByName(indexName))
  def dropIndex(indexName: String, options: DropIndexOptions): CollectionIO[Unit] =
    FF.liftF(DropIndexByNameOptions(indexName, options))
  def dropIndex(bson: Bson): CollectionIO[Unit] =
    FF.liftF(DropIndexByBson(bson))
  def dropIndex(bson: Bson, options: DropIndexOptions): CollectionIO[Unit] =
    FF.liftF(DropIndexByBsonOptions(bson, options))
  def dropIndexes(): CollectionIO[Unit] =
    FF.liftF(DropIndexes)
  def dropIndexes(options: DropIndexOptions): CollectionIO[Unit] =
    FF.liftF(DropIndexesOptions(options))
  def renameCollection(namespace: MongoNamespace): CollectionIO[Unit] =
    FF.liftF(RenameCollection(namespace))
  def renameCollection(namespace: MongoNamespace, options: RenameCollectionOptions): CollectionIO[Unit] =
    FF.liftF(RenameCollectionWithOptions(namespace, options))

  implicit val CollectionOpEmbeddable: Embeddable[CollectionOp, MongoCollection[Document]] =
    new Embeddable[CollectionOp, MongoCollection[Document]] {
      def embed[A](j: MongoCollection[Document], fa: CollectionIO[A]): Embedded[A] = Embedded.Collection(j, fa)
    }

  implicit val AsyncCollectionIO: Async[CollectionIO] =
    new Async[CollectionIO] {
      val asyncM = FF.catsFreeMonadForFree[CollectionOp]
      def bracketCase[A, B](acquire: CollectionIO[A])(use: A => CollectionIO[B])(
        release: (A, ExitCase[Throwable]) => CollectionIO[Unit]
      ): CollectionIO[B] = module.bracketCase(acquire)(use)(release)
      def pure[A](x: A): CollectionIO[A] = asyncM.pure(x)
      def handleErrorWith[A](fa: CollectionIO[A])(f: Throwable => CollectionIO[A]): CollectionIO[A] =
        module.handleErrorWith(fa, f)
      def raiseError[A](e: Throwable): CollectionIO[A] = module.raiseError(e)
      def async[A](k: (Either[Throwable, A] => Unit) => Unit): CollectionIO[A] = module.async(k)
      def asyncF[A](k: (Either[Throwable, A] => Unit) => CollectionIO[Unit]): CollectionIO[A] = module.asyncF(k)
      def flatMap[A, B](fa: CollectionIO[A])(f: A => CollectionIO[B]): CollectionIO[B] = asyncM.flatMap(fa)(f)
      def tailRecM[A, B](a: A)(f: A => CollectionIO[Either[A, B]]): CollectionIO[B] = asyncM.tailRecM(a)(f)
      def suspend[A](thunk: => CollectionIO[A]): CollectionIO[A] = asyncM.flatten(module.delay(thunk))
    }

  implicit val contextShiftCollectionIO: ContextShift[CollectionIO] =
    new ContextShift[CollectionIO] {
      override def shift: CollectionIO[Unit] = module.shift
      override def evalOn[A](ec: ExecutionContext)(fa: CollectionIO[A]): CollectionIO[A] = module.evalOn(ec)(fa)
    }
}
