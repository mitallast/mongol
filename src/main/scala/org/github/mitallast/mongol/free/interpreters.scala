package org.github.mitallast.mongol.free

import cats.data.{Kleisli, StateT}
import cats.free.{Free => FF}
import cats.effect.{Async, Blocker, ContextShift, ExitCase}
import cats.~>
import com.mongodb.bulk.BulkWriteResult
import com.mongodb.client.model._
import com.mongodb.client.result.{DeleteResult, UpdateResult}
import com.mongodb.client._
import com.mongodb.connection.ClusterDescription
import com.mongodb.session.ServerSession
import com.mongodb.{
  ClientSessionOptions,
  MongoNamespace,
  ReadConcern,
  ReadPreference,
  ServerAddress,
  TransactionOptions,
  WriteConcern
}
import org.bson.{BsonDocument, BsonTimestamp, Document}
import org.bson.conversions.Bson
import org.github.mitallast.mongol.free.client.ClientOp
import org.github.mitallast.mongol.free.collection.CollectionOp
import org.github.mitallast.mongol.free.database.DatabaseOp
import org.github.mitallast.mongol.free.session.SessionOp

import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

object interpreters {
  def apply[M[_]](b: Blocker)(implicit am: Async[M], cs: ContextShift[M]): interpreters[M] =
    new interpreters[M] {
      override implicit val asyncM: Async[M] = am
      override val contextShiftM: ContextShift[M] = cs
      override val blocker: Blocker = b
    }
}

trait interpreters[M[_]] { outer =>
  implicit val asyncM: Async[M]
  val contextShiftM: ContextShift[M]
  val blocker: Blocker

  // Interpreters

  type ClientState[A] = Kleisli[M, MongoClient, A]
  type SessionState[A] = Kleisli[M, (MongoClient, ClientSession), A]
  type DatabaseState[A] = StateT[M, (ClientSession, MongoDatabase), A]
  type CollectionState[A] = StateT[M, (ClientSession, MongoCollection[Document]), A]

  lazy val ClientInterpreter: ClientOp ~> ClientState = new ClientInterpreter {}
  lazy val SessionInterpreter: SessionOp ~> SessionState = new SessionInterpreter {}
  lazy val DatabaseInterpreter: DatabaseOp ~> DatabaseState = new DatabaseInterpreter {}
  lazy val CollectionInterpreter: CollectionOp ~> CollectionState = new CollectionInterpreter {}

  private def block[A](f: => A): M[A] =
    blocker.blockOn[M, A] {
      try {
        asyncM.delay(f)
      } catch {
        case NonFatal(e) => asyncM.raiseError(e)
      }
    }(contextShiftM)

  trait KleisliInterpreter[Op[_], State] extends (Op ~> Kleisli[M, State, *]) {

    private[interpreters] def primitive[A](f: State => A): Kleisli[M, State, A] =
      Kleisli(s => block(f(s)))

    def delay[A](a: => A): Kleisli[M, State, A] =
      Kleisli.liftF(asyncM.delay(a))

    def raiseError[A](e: Throwable): Kleisli[M, State, A] =
      Kleisli.liftF(asyncM.raiseError(e))

    def async[A](k: (Either[Throwable, A] => Unit) => Unit): Kleisli[M, State, A] =
      Kleisli.liftF(asyncM.async(k))

    def handleErrorWith[A](fa: FF[Op, A], f: Throwable => FF[Op, A]): Kleisli[M, State, A] =
      Kleisli.apply { s =>
        asyncM.handleErrorWith(fa.foldMap(this).run(s))(f(_).foldMap(this).run(s))
      }

    def asyncF[A](k: (Either[Throwable, A] => Unit) => FF[Op, Unit]): Kleisli[M, State, A] =
      Kleisli.apply[M, State, A] { s =>
        asyncM.asyncF[A] { cb =>
          k.apply(cb).foldMap(this).run(s)
        }
      }

    def bracketCase[A, B](
      acquire: FF[Op, A]
    )(use: A => FF[Op, B])(release0: (A, ExitCase[Throwable]) => FF[Op, Unit]): Kleisli[M, State, B] =
      Kleisli.apply[M, State, B] { s =>
        asyncM.bracketCase[A, B](acquire.foldMap(this).run(s)) { a =>
          use(a).foldMap(this).run(s)
        } {
          case (a, e) =>
            release0(a, e).foldMap(this).run(s)
        }
      }

    def evalOn[A](ec: ExecutionContext)(fa: FF[Op, A]): Kleisli[M, State, A] =
      Kleisli.apply { s =>
        contextShiftM.evalOn(ec) {
          fa.foldMap(this).run(s)
        }
      }

    val shift: Kleisli[M, State, Unit] =
      Kleisli.liftF(contextShiftM.shift)
  }

  trait ClientInterpreter extends ClientOp.Visitor[ClientState] with KleisliInterpreter[ClientOp, MongoClient] {
    override def embed[A](e: Embedded[A]): ClientState[A] =
      Kleisli.apply { c =>
        e match {
          case Embedded.Session(j, fa) => fa.foldMap(SessionInterpreter).run((c, j))
          case _                       => asyncM.raiseError(new IllegalArgumentException())
        }
      }

    override def getDatabase(databaseName: String): ClientState[MongoDatabase] =
      primitive(_.getDatabase(databaseName))

    override def startSession(): ClientState[ClientSession] =
      primitive(_.startSession())

    override def startSession(options: ClientSessionOptions): ClientState[ClientSession] =
      primitive(_.startSession(options))

    override def close(): ClientState[Unit] =
      primitive(_.close())

    override def listDatabaseNames(): ClientState[MongoIterable[String]] =
      primitive(_.listDatabaseNames())

    override def watch(): ClientState[ChangeStreamIterable[Document]] =
      primitive(_.watch())

    override def watch(pipeline: Seq[_ <: Bson]): ClientState[ChangeStreamIterable[Document]] =
      primitive(_.watch(pipeline.asJava))

    override def getClusterDescription: ClientState[ClusterDescription] =
      primitive(_.getClusterDescription)
  }

  trait SessionInterpreter
      extends SessionOp.Visitor[SessionState]
      with KleisliInterpreter[SessionOp, (MongoClient, ClientSession)] {

    override def embed[A](e: Embedded[A]): SessionState[A] =
      Kleisli.apply {
        case (c, s) =>
          e match {
            case Embedded.Session(j, fa)    => fa.foldMap(this).run((c, j))
            case Embedded.Database(j, fa)   => fa.foldMap(DatabaseInterpreter).runA((s, j))
            case Embedded.Collection(j, fa) => fa.foldMap(CollectionInterpreter).runA((s, j))
            case _                          => asyncM.raiseError(new IllegalArgumentException)
          }
      }

    override def getPinnedServerAddress: SessionState[ServerAddress] =
      primitive { case (_, s) => s.getPinnedServerAddress }

    override def setPinnedServerAddress(address: ServerAddress): SessionState[Unit] =
      primitive { case (_, s) => s.setPinnedServerAddress(address) }

    override def getRecoveryToken: SessionState[Option[BsonDocument]] =
      primitive { case (_, s) => Option(s.getRecoveryToken) }

    override def setRecoveryToken(recoveryToken: BsonDocument): SessionState[Unit] =
      primitive { case (_, s) => s.setRecoveryToken(recoveryToken) }

    override def getOptions: SessionState[ClientSessionOptions] =
      primitive { case (_, s) => s.getOptions }

    override def isCausallyConsistent: SessionState[Boolean] =
      primitive { case (_, s) => s.isCausallyConsistent }

    override def getServerSession: SessionState[ServerSession] =
      primitive { case (_, s) => s.getServerSession }

    override def getOperationTime: SessionState[BsonTimestamp] =
      primitive { case (_, s) => s.getOperationTime }

    override def advanceOperationTime(operationTime: BsonTimestamp): SessionState[Unit] =
      primitive { case (_, s) => s.advanceOperationTime(operationTime) }

    override def advanceClusterTime(clusterTime: BsonDocument): SessionState[Unit] =
      primitive { case (_, s) => s.advanceClusterTime(clusterTime) }

    override def getClusterTime: SessionState[BsonDocument] =
      primitive { case (_, s) => s.getClusterTime }

    override def hasActiveTransaction: SessionState[Boolean] =
      primitive { case (_, s) => s.hasActiveTransaction }

    override def notifyMessageSent(): SessionState[Boolean] =
      primitive { case (_, s) => s.notifyMessageSent() }

    override def getTransactionOptions: SessionState[TransactionOptions] =
      primitive { case (_, s) => s.getTransactionOptions }

    override def startTransaction(): SessionState[Unit] =
      primitive { case (_, s) => s.startTransaction() }

    override def startTransaction(options: TransactionOptions): SessionState[Unit] =
      primitive { case (_, s) => s.startTransaction(options) }

    override def commitTransaction(): SessionState[Unit] =
      primitive { case (_, s) => s.commitTransaction() }

    override def abortTransaction(): SessionState[Unit] =
      primitive { case (_, s) => s.abortTransaction() }

    override def close(): SessionState[Unit] =
      primitive { case (_, s) => s.close() }

    override def getDatabase(databaseName: String): SessionState[MongoDatabase] =
      primitive { case (c, _) => c.getDatabase(databaseName) }

    override def listDatabaseNames(): SessionState[MongoIterable[String]] =
      primitive { case (c, s) => c.listDatabaseNames(s) }

    override def watch(): SessionState[ChangeStreamIterable[Document]] =
      primitive { case (c, s) => c.watch(s) }

    override def watch(pipeline: Seq[_ <: Bson]): SessionState[ChangeStreamIterable[Document]] =
      primitive { case (c, s) => c.watch(s, pipeline.asJava) }
  }

  trait StateTInterpreter[Op[_], State] extends (Op ~> StateT[M, State, *]) {

    private[interpreters] def primitive[A](f: State => A): StateT[M, State, A] =
      StateT.inspectF(s => block(f(s)))

    private[interpreters] def modify(f: State => State): StateT[M, State, Unit] =
      StateT.modifyF(s => block(f(s)))

    def delay[A](a: => A): StateT[M, State, A] =
      StateT.liftF(asyncM.delay(a))

    def raiseError[A](e: Throwable): StateT[M, State, A] =
      StateT.liftF(asyncM.raiseError(e))

    def async[A](k: (Either[Throwable, A] => Unit) => Unit): StateT[M, State, A] =
      StateT.liftF(asyncM.async(k))

    def handleErrorWith[A](fa: FF[Op, A], f: Throwable => FF[Op, A]): StateT[M, State, A] =
      StateT.apply { s =>
        asyncM.handleErrorWith(fa.foldMap(this).run(s))(f(_).foldMap(this).run(s))
      }

    def asyncF[A](k: (Either[Throwable, A] => Unit) => FF[Op, Unit]): StateT[M, State, A] =
      StateT.inspectF[M, State, A] { s =>
        asyncM.asyncF[A] { cb =>
          k.apply(cb).foldMap(this).runA(s)
        }
      }

    def bracketCase[A, B](
      acquire: FF[Op, A]
    )(use: A => FF[Op, B])(release0: (A, ExitCase[Throwable]) => FF[Op, Unit]): StateT[M, State, B] =
      StateT.apply[M, State, B] { s =>
        asyncM.bracketCase[(State, A), (State, B)](acquire.foldMap(this).run(s)) {
          case (s, a) =>
            use(a).foldMap(this).run(s)
        } {
          case ((s, a), e) =>
            release0(a, e).foldMap(this).runA(s)
        }
      }

    def evalOn[A](ec: ExecutionContext)(fa: FF[Op, A]): StateT[M, State, A] =
      StateT.apply { s =>
        contextShiftM.evalOn(ec) {
          fa.foldMap(this).run(s)
        }
      }

    val shift: StateT[M, State, Unit] =
      StateT.liftF(contextShiftM.shift)
  }

  trait DatabaseInterpreter
      extends DatabaseOp.Visitor[DatabaseState]
      with StateTInterpreter[DatabaseOp, (ClientSession, MongoDatabase)] {

    override def embed[A](e: Embedded[A]): DatabaseState[A] =
      StateT.inspectF {
        case (s, d) =>
          e match {
            case Embedded.Database(j, fa)   => fa.foldMap(this).runA((s, j))
            case Embedded.Collection(j, fa) => fa.foldMap(CollectionInterpreter).runA((s, j))
            case _                          => asyncM.raiseError(new IllegalArgumentException)
          }
      }

    override def getName: DatabaseState[String] =
      primitive { case (_, d) => d.getName }

    override def getReadPreference: DatabaseState[ReadPreference] =
      primitive { case (_, d) => d.getReadPreference }

    override def getWriteConcern: DatabaseState[WriteConcern] =
      primitive { case (_, d) => d.getWriteConcern }

    override def getReadConcern: DatabaseState[ReadConcern] =
      primitive { case (_, d) => d.getReadConcern }

    override def setReadPreference(readPreference: ReadPreference): DatabaseState[Unit] =
      primitive { case (_, d) => d.withReadPreference(readPreference) }

    override def setWriteConcern(writeConcern: WriteConcern): DatabaseState[Unit] =
      modify { case (s, d) => s -> d.withWriteConcern(writeConcern) }

    override def setReadConcern(readConcern: ReadConcern): DatabaseState[Unit] =
      modify { case (s, d) => s -> d.withReadConcern(readConcern) }

    override def getCollection(collectionName: String): DatabaseState[MongoCollection[Document]] =
      primitive { case (_, d) => d.getCollection(collectionName) }

    override def runCommand(command: Bson): DatabaseState[Bson] =
      primitive { case (s, d) => d.runCommand(s, command) }

    override def runCommand(command: Bson, readPreference: ReadPreference): DatabaseState[Bson] =
      primitive { case (s, d) => d.runCommand(s, command, readPreference) }

    override def drop(): DatabaseState[Unit] =
      primitive { case (s, d) => d.drop(s) }

    override def listCollectionNames(): DatabaseState[MongoIterable[String]] =
      primitive { case (s, d) => d.listCollectionNames(s) }

    override def listCollections(): DatabaseState[ListCollectionsIterable[Document]] =
      primitive { case (s, d) => d.listCollections(s) }

    override def createCollection(collectionName: String): DatabaseState[Unit] =
      primitive { case (s, d) => d.createCollection(s, collectionName) }

    override def createCollection(collectionName: String, options: CreateCollectionOptions): DatabaseState[Unit] =
      primitive { case (s, d) => d.createCollection(s, collectionName, options) }

    override def createView(viewName: String, viewOn: String, pipeline: Seq[_ <: Bson]): DatabaseState[Unit] =
      primitive { case (s, d) => d.createView(s, viewName, viewOn, pipeline.asJava) }

    override def createView(
      viewName: String,
      viewOn: String,
      pipeline: Seq[_ <: Bson],
      options: CreateViewOptions
    ): DatabaseState[Unit] =
      primitive { case (s, d) => d.createView(s, viewName, viewOn, pipeline.asJava, options) }

    override def watch(): DatabaseState[ChangeStreamIterable[Document]] =
      primitive { case (s, d) => d.watch(s) }

    override def watch(pipeline: Seq[_ <: Bson]): DatabaseState[ChangeStreamIterable[Document]] =
      primitive { case (s, d) => d.watch(s, pipeline.asJava) }

    override def aggregate(pipeline: Seq[_ <: Bson]): DatabaseState[AggregateIterable[Document]] =
      primitive { case (s, d) => d.aggregate(s, pipeline.asJava) }
  }

  trait CollectionInterpreter
      extends CollectionOp.Visitor[CollectionState]
      with StateTInterpreter[CollectionOp, (ClientSession, MongoCollection[Document])] {

    override def embed[A](e: Embedded[A]): CollectionState[A] =
      StateT.inspectF {
        case (s, c) =>
          e match {
            case Embedded.Database(j, fa)   => fa.foldMap(DatabaseInterpreter).runA((s, j))
            case Embedded.Collection(j, fa) => fa.foldMap(this).runA((s, j))
            case _                          => asyncM.raiseError(new IllegalArgumentException)
          }
      }

    override def getNamespace: CollectionState[MongoNamespace] =
      primitive { case (_, c) => c.getNamespace }

    override def getReadPreference: CollectionState[ReadPreference] =
      primitive { case (_, c) => c.getReadPreference }

    override def getWriteConcern: CollectionState[WriteConcern] =
      primitive { case (_, c) => c.getWriteConcern }

    override def getReadConcern: CollectionState[ReadConcern] =
      primitive { case (_, c) => c.getReadConcern }

    override def setReadPreference(readPreference: ReadPreference): CollectionState[Unit] =
      primitive { case (_, c) => c.withReadPreference(readPreference) }

    override def setWriteConcern(writeConcern: WriteConcern): CollectionState[Unit] =
      primitive { case (s, c) => s -> c.withWriteConcern(writeConcern) }

    override def setReadConcern(readConcern: ReadConcern): CollectionState[Unit] =
      primitive { case (s, c) => s -> c.withReadConcern(readConcern) }

    override def countDocuments(): CollectionState[Long] =
      primitive { case (s, c) => c.countDocuments(s) }

    override def countDocuments(filter: Bson): CollectionState[Long] =
      primitive { case (s, c) => c.countDocuments(s, filter) }

    override def countDocuments(filter: Bson, options: CountOptions): CollectionState[Long] =
      primitive { case (s, c) => c.countDocuments(s, filter, options) }

    override def estimatedDocumentCount(): CollectionState[Long] =
      primitive { case (_, c) => c.estimatedDocumentCount() }

    override def estimatedDocumentCount(options: EstimatedDocumentCountOptions): CollectionState[Long] =
      primitive { case (_, c) => c.estimatedDocumentCount(options) }

    override def distinct(fieldName: String): CollectionState[DistinctIterable[Bson]] =
      primitive { case (s, c) => c.distinct[Bson](s, fieldName, classOf[Bson]) }

    override def distinct(fieldName: String, filter: Bson): CollectionState[DistinctIterable[Bson]] =
      primitive { case (s, c) => c.distinct[Bson](s, fieldName, filter, classOf[Bson]) }

    override def find(): CollectionState[FindIterable[Document]] =
      primitive { case (s, c) => c.find(s) }

    override def find(filter: Bson): CollectionState[FindIterable[Document]] =
      primitive { case (s, c) => c.find(s, filter) }

    override def aggregate(pipeline: Seq[Bson]): CollectionState[AggregateIterable[Document]] =
      primitive { case (s, c) => c.aggregate(s, pipeline.asJava) }

    override def watch(): CollectionState[ChangeStreamIterable[Document]] =
      primitive { case (s, c) => c.watch(s) }

    override def watch(pipeline: Seq[Bson]): CollectionState[ChangeStreamIterable[Document]] =
      primitive { case (s, c) => c.watch(s, pipeline.asJava) }

    override def mapReduce(mapFunction: String, reduceFunction: String): CollectionState[MapReduceIterable[Document]] =
      primitive { case (s, c) => c.mapReduce(s, mapFunction, reduceFunction) }

    override def bulkWrite(requests: Seq[WriteModel[_ <: Document]]): CollectionState[BulkWriteResult] =
      primitive { case (s, c) => c.bulkWrite(s, requests.asJava) }

    override def bulkWrite(
      requests: Seq[WriteModel[_ <: Document]],
      options: BulkWriteOptions
    ): CollectionState[BulkWriteResult] =
      primitive { case (s, c) => c.bulkWrite(s, requests.asJava, options) }

    override def insertOne(document: Document): CollectionState[Unit] =
      primitive { case (s, c) => c.insertOne(s, document) }

    override def insertOne(document: Document, options: InsertOneOptions): CollectionState[Unit] =
      primitive { case (s, c) => c.insertOne(s, document, options) }

    override def insertMany(documents: Seq[_ <: Document]): CollectionState[Unit] =
      primitive { case (s, c) => c.insertMany(s, documents.asJava) }

    override def insertMany(documents: Seq[_ <: Document], options: InsertManyOptions): CollectionState[Unit] =
      primitive { case (s, c) => c.insertMany(s, documents.asJava, options) }

    override def deleteOne(filter: Bson): CollectionState[DeleteResult] =
      primitive { case (s, c) => c.deleteOne(s, filter) }

    override def deleteOne(filter: Bson, options: DeleteOptions): CollectionState[DeleteResult] =
      primitive { case (s, c) => c.deleteOne(s, filter, options) }

    override def deleteMany(filter: Bson): CollectionState[DeleteResult] =
      primitive { case (s, c) => c.deleteMany(s, filter) }

    override def deleteMany(filter: Bson, options: DeleteOptions): CollectionState[DeleteResult] =
      primitive { case (s, c) => c.deleteMany(s, filter, options) }

    override def replaceOne(filter: Bson, replacement: Document): CollectionState[UpdateResult] =
      primitive { case (s, c) => c.replaceOne(s, filter, replacement) }

    override def replaceOne(
      filter: Bson,
      replacement: Document,
      options: ReplaceOptions
    ): CollectionState[UpdateResult] =
      primitive { case (s, c) => c.replaceOne(s, filter, replacement, options) }

    override def updateOne(filter: Bson, update: Bson): CollectionState[UpdateResult] =
      primitive { case (s, c) => c.updateOne(s, filter, update) }

    override def updateOne(filter: Bson, update: Bson, options: UpdateOptions): CollectionState[UpdateResult] =
      primitive { case (s, c) => c.updateOne(s, filter, update, options) }

    override def updateMany(filter: Bson, update: Bson): CollectionState[UpdateResult] =
      primitive { case (s, c) => c.updateMany(s, filter, update) }

    override def updateMany(filter: Bson, update: Bson, options: UpdateOptions): CollectionState[UpdateResult] =
      primitive { case (s, c) => c.updateMany(s, filter, update, options) }

    override def findOneAndDelete(filter: Bson): CollectionState[Option[Document]] =
      primitive { case (s, c) => Option(c.findOneAndDelete(s, filter)) }

    override def findOneAndDelete(filter: Bson, options: FindOneAndDeleteOptions): CollectionState[Option[Document]] =
      primitive { case (s, c) => Option(c.findOneAndDelete(s, filter, options)) }

    override def findOneAndReplace(filter: Bson, replacement: Document): CollectionState[Option[Document]] =
      primitive { case (s, c) => Option(c.findOneAndReplace(s, filter, replacement)) }

    override def findOneAndReplace(
      filter: Bson,
      replacement: Document,
      options: FindOneAndReplaceOptions
    ): CollectionState[Option[Document]] =
      primitive { case (s, c) => Option(c.findOneAndReplace(s, filter, replacement)) }

    override def findOneAndUpdate(filter: Bson, update: Bson): CollectionState[Option[Document]] =
      primitive { case (s, c) => Option(c.findOneAndUpdate(s, filter, update)) }

    override def findOneAndUpdate(
      filter: Bson,
      update: Bson,
      options: FindOneAndUpdateOptions
    ): CollectionState[Option[Document]] =
      primitive { case (s, c) => Option(c.findOneAndUpdate(s, filter, update, options)) }

    override def drop(): CollectionState[Unit] =
      primitive { case (s, c) => c.drop(s) }

    override def createIndex(keys: Bson): CollectionState[String] =
      primitive { case (s, c) => c.createIndex(s, keys) }

    override def createIndex(keys: Bson, options: IndexOptions): CollectionState[String] =
      primitive { case (s, c) => c.createIndex(s, keys, options) }

    override def createIndexes(indexes: Seq[IndexModel]): CollectionState[Vector[String]] =
      primitive { case (s, c) => c.createIndexes(s, indexes.asJava).asScala.toVector }

    override def createIndexes(indexes: Seq[IndexModel], options: CreateIndexOptions): CollectionState[Vector[String]] =
      primitive { case (s, c) => c.createIndexes(s, indexes.asJava, options).asScala.toVector }

    override def listIndexes(): CollectionState[ListIndexesIterable[Document]] =
      primitive { case (s, c) => c.listIndexes(s) }

    override def dropIndex(indexName: String): CollectionState[Unit] =
      primitive { case (s, c) => c.dropIndex(s, indexName) }

    override def dropIndex(indexName: String, options: DropIndexOptions): CollectionState[Unit] =
      primitive { case (s, c) => c.dropIndex(s, indexName, options) }

    override def dropIndex(bson: Bson): CollectionState[Unit] =
      primitive { case (s, c) => c.dropIndex(s, bson) }

    override def dropIndex(bson: Bson, options: DropIndexOptions): CollectionState[Unit] =
      primitive { case (s, c) => c.dropIndex(s, bson, options) }

    override def dropIndexes(): CollectionState[Unit] =
      primitive { case (s, c) => c.dropIndexes(s) }

    override def dropIndexes(options: DropIndexOptions): CollectionState[Unit] =
      primitive { case (s, c) => c.dropIndexes(s, options) }

    override def renameCollection(namespace: MongoNamespace): CollectionState[Unit] =
      primitive { case (s, c) => c.renameCollection(s, namespace) }

    override def renameCollection(namespace: MongoNamespace, options: RenameCollectionOptions): CollectionState[Unit] =
      primitive { case (s, c) => c.renameCollection(s, namespace, options) }
  }
}
