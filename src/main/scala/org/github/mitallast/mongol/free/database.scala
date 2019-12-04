package org.github.mitallast.mongol.free

import cats.~>
import cats.effect.{Async, ContextShift, ExitCase}
import cats.free.{Free => FF}
import com.mongodb.client.model.{CreateCollectionOptions, CreateViewOptions}
import com.mongodb.{ReadConcern, ReadPreference, WriteConcern}
import com.mongodb.client.{
  AggregateIterable,
  ChangeStreamIterable,
  ListCollectionsIterable,
  MongoCollection,
  MongoDatabase,
  MongoIterable
}
import org.bson.Document
import org.bson.conversions.Bson

import scala.concurrent.ExecutionContext

object database { module =>

  sealed trait DatabaseOp[A] {
    def visit[F[_]](v: DatabaseOp.Visitor[F]): F[A]
  }

  type DatabaseIO[A] = FF[DatabaseOp, A]

  object DatabaseOp {

    trait Visitor[F[_]] extends (DatabaseOp ~> F) {
      final def apply[A](fa: DatabaseOp[A]): F[A] = fa.visit(this)

      // Common
      def embed[A](e: Embedded[A]): F[A]
      def delay[A](a: => A): F[A]
      def handleErrorWith[A](fa: DatabaseIO[A], f: Throwable => DatabaseIO[A]): F[A]
      def raiseError[A](e: Throwable): F[A]
      def async[A](k: (Either[Throwable, A] => Unit) => Unit): F[A]
      def asyncF[A](k: (Either[Throwable, A] => Unit) => DatabaseIO[Unit]): F[A]
      def bracketCase[A, B](acquire: DatabaseIO[A])(use: A => DatabaseIO[B])(
        release: (A, ExitCase[Throwable]) => DatabaseIO[Unit]
      ): F[B]
      def shift: F[Unit]
      def evalOn[A](ec: ExecutionContext)(fa: DatabaseIO[A]): F[A]

      // Database specific
      def getName: F[String]

      def getReadPreference: F[ReadPreference]
      def getWriteConcern: F[WriteConcern]
      def getReadConcern: F[ReadConcern]

      def setReadPreference(readPreference: ReadPreference): F[Unit]
      def setWriteConcern(writeConcern: WriteConcern): F[Unit]
      def setReadConcern(readConcern: ReadConcern): F[Unit]

      def getCollection(collectionName: String): F[MongoCollection[Document]]

      def runCommand(command: Bson): F[Bson]
      def runCommand(command: Bson, readPreference: ReadPreference): F[Bson]

      def drop(): F[Unit]

      def listCollectionNames(): F[MongoIterable[String]]
      def listCollections(): F[ListCollectionsIterable[Document]]

      def createCollection(collectionName: String): F[Unit]
      def createCollection(collectionName: String, options: CreateCollectionOptions): F[Unit]

      def createView(viewName: String, viewOn: String, pipeline: Seq[_ <: Bson]): F[Unit]
      def createView(viewName: String, viewOn: String, pipeline: Seq[_ <: Bson], options: CreateViewOptions): F[Unit]

      def watch(): F[ChangeStreamIterable[Document]]
      def watch(pipeline: Seq[_ <: Bson]): F[ChangeStreamIterable[Document]]

      def aggregate(pipeline: Seq[_ <: Bson]): F[AggregateIterable[Document]]
    }

    // Common operations for all algebras
    final case class Embed[A](e: Embedded[A]) extends DatabaseOp[A] {
      override def visit[F[_]](v: DatabaseOp.Visitor[F]): F[A] = v.embed(e)
    }
    final case class Delay[A](a: () => A) extends DatabaseOp[A] {
      override def visit[F[_]](v: DatabaseOp.Visitor[F]): F[A] = v.delay(a())
    }
    final case class HandleErrorWith[A](fa: DatabaseIO[A], f: Throwable => DatabaseIO[A]) extends DatabaseOp[A] {
      override def visit[F[_]](v: DatabaseOp.Visitor[F]): F[A] = v.handleErrorWith(fa, f)
    }
    final case class RaiseError[A](e: Throwable) extends DatabaseOp[A] {
      override def visit[F[_]](v: DatabaseOp.Visitor[F]): F[A] = v.raiseError(e)
    }
    final case class Async1[A](k: (Either[Throwable, A] => Unit) => Unit) extends DatabaseOp[A] {
      override def visit[F[_]](v: DatabaseOp.Visitor[F]): F[A] = v.async(k)
    }
    final case class AsyncF[A](k: (Either[Throwable, A] => Unit) => DatabaseIO[Unit]) extends DatabaseOp[A] {
      override def visit[F[_]](v: DatabaseOp.Visitor[F]): F[A] = v.asyncF(k)
    }
    final case class BracketCase[A, B](
      acquire: DatabaseIO[A],
      use: A => DatabaseIO[B],
      release: (A, ExitCase[Throwable]) => DatabaseIO[Unit]
    ) extends DatabaseOp[B] {
      override def visit[F[_]](v: DatabaseOp.Visitor[F]): F[B] = v.bracketCase(acquire)(use)(release)
    }
    final case object Shift extends DatabaseOp[Unit] {
      override def visit[F[_]](v: DatabaseOp.Visitor[F]): F[Unit] = v.shift
    }
    final case class EvalOn[A](ec: ExecutionContext, fa: DatabaseIO[A]) extends DatabaseOp[A] {
      override def visit[F[_]](v: DatabaseOp.Visitor[F]): F[A] = v.evalOn(ec)(fa)
    }

    // Database-specific operations
    final case object GetName extends DatabaseOp[String] {
      override def visit[F[_]](v: DatabaseOp.Visitor[F]): F[String] = v.getName
    }

    final case object GetReadPreference extends DatabaseOp[ReadPreference] {
      override def visit[F[_]](v: DatabaseOp.Visitor[F]): F[ReadPreference] = v.getReadPreference
    }
    final case object GetWriteConcern extends DatabaseOp[WriteConcern] {
      override def visit[F[_]](v: DatabaseOp.Visitor[F]): F[WriteConcern] = v.getWriteConcern
    }
    final case object GetReadConcern extends DatabaseOp[ReadConcern] {
      override def visit[F[_]](v: DatabaseOp.Visitor[F]): F[ReadConcern] = v.getReadConcern
    }

    final case class SetReadPreference(readPreference: ReadPreference) extends DatabaseOp[Unit] {
      override def visit[F[_]](v: DatabaseOp.Visitor[F]): F[Unit] =
        v.setReadPreference(readPreference)
    }
    final case class SetWriteConcern(writeConcern: WriteConcern) extends DatabaseOp[Unit] {
      override def visit[F[_]](v: DatabaseOp.Visitor[F]): F[Unit] =
        v.setWriteConcern(writeConcern)
    }
    final case class SetReadConcern(readConcern: ReadConcern) extends DatabaseOp[Unit] {
      override def visit[F[_]](v: DatabaseOp.Visitor[F]): F[Unit] =
        v.setReadConcern(readConcern)
    }

    final case class GetCollection(collectionName: String) extends DatabaseOp[MongoCollection[Document]] {
      override def visit[F[_]](v: DatabaseOp.Visitor[F]): F[MongoCollection[Document]] =
        v.getCollection(collectionName)
    }

    final case class RunCommand(command: Bson) extends DatabaseOp[Bson] {
      override def visit[F[_]](v: DatabaseOp.Visitor[F]): F[Bson] =
        v.runCommand(command)
    }
    final case class RunCommandWithReadPreference(command: Bson, readPreference: ReadPreference)
        extends DatabaseOp[Bson] {
      override def visit[F[_]](v: DatabaseOp.Visitor[F]): F[Bson] =
        v.runCommand(command, readPreference)
    }

    final case object Drop extends DatabaseOp[Unit] {
      override def visit[F[_]](v: DatabaseOp.Visitor[F]): F[Unit] =
        v.drop()
    }

    final case object ListCollectionNames extends DatabaseOp[MongoIterable[String]] {
      override def visit[F[_]](v: DatabaseOp.Visitor[F]): F[MongoIterable[String]] =
        v.listCollectionNames()
    }
    final case object ListCollections extends DatabaseOp[ListCollectionsIterable[Document]] {
      override def visit[F[_]](v: DatabaseOp.Visitor[F]): F[ListCollectionsIterable[Document]] =
        v.listCollections()
    }

    final case class CreateCollection(collectionName: String) extends DatabaseOp[Unit] {
      override def visit[F[_]](v: DatabaseOp.Visitor[F]): F[Unit] =
        v.createCollection(collectionName)
    }
    final case class CreateCollectionWithOptions(collectionName: String, options: CreateCollectionOptions)
        extends DatabaseOp[Unit] {
      override def visit[F[_]](v: DatabaseOp.Visitor[F]): F[Unit] =
        v.createCollection(collectionName, options)
    }

    final case class CreateView(viewName: String, viewOn: String, pipeline: Seq[_ <: Bson]) extends DatabaseOp[Unit] {
      override def visit[F[_]](v: DatabaseOp.Visitor[F]): F[Unit] =
        v.createView(viewName, viewOn, pipeline)
    }
    final case class CreateViewWithOptions(
      viewName: String,
      viewOn: String,
      pipeline: Seq[_ <: Bson],
      options: CreateViewOptions
    ) extends DatabaseOp[Unit] {
      override def visit[F[_]](v: DatabaseOp.Visitor[F]): F[Unit] =
        v.createView(viewName, viewOn, pipeline, options)
    }

    final case object Watch extends DatabaseOp[ChangeStreamIterable[Document]] {
      override def visit[F[_]](v: DatabaseOp.Visitor[F]): F[ChangeStreamIterable[Document]] =
        v.watch()
    }
    final case class WatchWithPipeline(pipeline: Seq[_ <: Bson]) extends DatabaseOp[ChangeStreamIterable[Document]] {
      override def visit[F[_]](v: DatabaseOp.Visitor[F]): F[ChangeStreamIterable[Document]] =
        v.watch(pipeline)
    }

    final case class Aggregate(pipeline: Seq[_ <: Bson]) extends DatabaseOp[AggregateIterable[Document]] {
      override def visit[F[_]](v: DatabaseOp.Visitor[F]): F[AggregateIterable[Document]] =
        v.aggregate(pipeline)
    }
  }

  import DatabaseOp._

  // Smart constructors for operations common to all algebras
  val unit: DatabaseIO[Unit] = FF.pure(())
  def pure[A](a: A): DatabaseIO[A] = FF.pure(a)
  def embed[F[_], J, A](j: J, fa: FF[F, A])(implicit ev: Embeddable[F, J]): DatabaseIO[A] =
    FF.liftF(Embed(ev.embed(j, fa)))
  def delay[A](a: => A): DatabaseIO[A] = FF.liftF(Delay(() => a))
  def handleErrorWith[A](fa: DatabaseIO[A], f: Throwable => DatabaseIO[A]): DatabaseIO[A] =
    FF.liftF(HandleErrorWith(fa, f))
  def raiseError[A](err: Throwable): DatabaseIO[A] = FF.liftF(RaiseError(err))
  def async[A](k: (Either[Throwable, A] => Unit) => Unit): DatabaseIO[A] = FF.liftF(Async1(k))
  def asyncF[A](k: (Either[Throwable, A] => Unit) => DatabaseIO[Unit]): DatabaseIO[A] =
    FF.liftF(AsyncF(k))
  def bracketCase[A, B](acquire: DatabaseIO[A])(use: A => DatabaseIO[B])(
    release: (A, ExitCase[Throwable]) => DatabaseIO[Unit]
  ): DatabaseIO[B] = FF.liftF(BracketCase(acquire, use, release))
  val shift: DatabaseIO[Unit] = FF.liftF(Shift)
  def evalOn[A](ec: ExecutionContext)(fa: DatabaseIO[A]): DatabaseIO[A] = FF.liftF(EvalOn(ec, fa))

  // Smart constructors for database-specific operations
  val getName: DatabaseIO[String] = FF.liftF(GetName)
  val getReadPreference: DatabaseIO[ReadPreference] = FF.liftF(GetReadPreference)
  val getWriteConcern: DatabaseIO[WriteConcern] = FF.liftF(GetWriteConcern)
  val getReadConcern: DatabaseIO[ReadConcern] = FF.liftF(GetReadConcern)

  def setReadPreference(readPreference: ReadPreference): DatabaseIO[Unit] =
    FF.liftF(SetReadPreference(readPreference))
  def setWriteConcern(writeConcern: WriteConcern): DatabaseIO[Unit] =
    FF.liftF(SetWriteConcern(writeConcern))
  def setReadConcern(readConcern: ReadConcern): DatabaseIO[Unit] =
    FF.liftF(SetReadConcern(readConcern))

  def getCollection(collectionName: String): DatabaseIO[MongoCollection[Document]] =
    FF.liftF(GetCollection(collectionName))

  def runCommand(command: Bson): DatabaseIO[Bson] =
    FF.liftF(RunCommand(command))

  def runCommand(command: Bson, readPreference: ReadPreference): DatabaseIO[Bson] =
    FF.liftF(RunCommandWithReadPreference(command, readPreference))

  def drop(): DatabaseIO[Unit] = FF.liftF(Drop)

  val listCollectionNames: DatabaseIO[MongoIterable[String]] =
    FF.liftF(ListCollectionNames)

  val listCollections: DatabaseIO[ListCollectionsIterable[Document]] =
    FF.liftF(ListCollections)

  def createCollection(collectionName: String): DatabaseIO[Unit] =
    FF.liftF(CreateCollection(collectionName))

  def createCollection(collectionName: String, options: CreateCollectionOptions): DatabaseIO[Unit] =
    FF.liftF(CreateCollectionWithOptions(collectionName, options))

  def createView(viewName: String, viewOn: String, pipeline: Seq[_ <: Bson]): DatabaseIO[Unit] =
    FF.liftF(CreateView(viewName, viewOn, pipeline))

  def createView(
    viewName: String,
    viewOn: String,
    pipeline: Seq[_ <: Bson],
    options: CreateViewOptions
  ): DatabaseIO[Unit] =
    FF.liftF(CreateViewWithOptions(viewName, viewOn, pipeline, options))

  val watch: DatabaseIO[ChangeStreamIterable[Document]] =
    FF.liftF(Watch)

  def watch(pipeline: Seq[_ <: Bson]): DatabaseIO[ChangeStreamIterable[Document]] =
    FF.liftF(WatchWithPipeline(pipeline))

  def aggregate(pipeline: Seq[_ <: Bson]): DatabaseIO[AggregateIterable[Document]] =
    FF.liftF(Aggregate(pipeline))

  implicit val DatabaseOpEmbeddable: Embeddable[DatabaseOp, MongoDatabase] =
    new Embeddable[DatabaseOp, MongoDatabase] {
      def embed[A](j: MongoDatabase, fa: DatabaseIO[A]): Embedded[A] = Embedded.Database(j, fa)
    }

  implicit val AsyncDatabaseIO: Async[DatabaseIO] =
    new Async[DatabaseIO] {
      val asyncM = FF.catsFreeMonadForFree[DatabaseOp]
      def bracketCase[A, B](acquire: DatabaseIO[A])(use: A => DatabaseIO[B])(
        release: (A, ExitCase[Throwable]) => DatabaseIO[Unit]
      ): DatabaseIO[B] = module.bracketCase(acquire)(use)(release)
      def pure[A](x: A): DatabaseIO[A] = asyncM.pure(x)
      def handleErrorWith[A](fa: DatabaseIO[A])(f: Throwable => DatabaseIO[A]): DatabaseIO[A] =
        module.handleErrorWith(fa, f)
      def raiseError[A](e: Throwable): DatabaseIO[A] = module.raiseError(e)
      def async[A](k: (Either[Throwable, A] => Unit) => Unit): DatabaseIO[A] = module.async(k)
      def asyncF[A](k: (Either[Throwable, A] => Unit) => DatabaseIO[Unit]): DatabaseIO[A] = module.asyncF(k)
      def flatMap[A, B](fa: DatabaseIO[A])(f: A => DatabaseIO[B]): DatabaseIO[B] = asyncM.flatMap(fa)(f)
      def tailRecM[A, B](a: A)(f: A => DatabaseIO[Either[A, B]]): DatabaseIO[B] = asyncM.tailRecM(a)(f)
      def suspend[A](thunk: => DatabaseIO[A]): DatabaseIO[A] = asyncM.flatten(module.delay(thunk))
    }

  implicit val contextShiftDatabaseIO: ContextShift[DatabaseIO] =
    new ContextShift[DatabaseIO] {
      override def shift: DatabaseIO[Unit] = module.shift
      override def evalOn[A](ec: ExecutionContext)(fa: DatabaseIO[A]): DatabaseIO[A] = module.evalOn(ec)(fa)
    }
}
