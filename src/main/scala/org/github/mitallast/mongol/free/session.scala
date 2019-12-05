package org.github.mitallast.mongol.free

import cats.~>
import cats.effect.{Async, ContextShift, ExitCase}
import cats.free.{Free => FF}
import cats.syntax.flatMap._
import com.mongodb.client.{ChangeStreamIterable, ClientSession, MongoDatabase, MongoIterable}
import com.mongodb.{ClientSessionOptions, ServerAddress, TransactionOptions}
import com.mongodb.session.ServerSession
import org.bson.conversions.Bson
import org.bson.{BsonDocument, BsonTimestamp, Document}

import scala.concurrent.ExecutionContext

object session { module =>
  sealed trait SessionOp[A] {
    def visit[F[_]](v: SessionOp.Visitor[F]): F[A]
  }

  type SessionIO[A] = FF[SessionOp, A]

  object SessionOp {
    trait Visitor[F[_]] extends (SessionOp ~> F) {
      final def apply[A](fa: SessionOp[A]): F[A] = fa.visit(this)
      // Common
      def embed[A](e: Embedded[A]): F[A]
      def delay[A](a: => A): F[A]
      def handleErrorWith[A](fa: SessionIO[A], f: Throwable => SessionIO[A]): F[A]
      def raiseError[A](e: Throwable): F[A]
      def async[A](k: (Either[Throwable, A] => Unit) => Unit): F[A]
      def asyncF[A](k: (Either[Throwable, A] => Unit) => SessionIO[Unit]): F[A]
      def bracketCase[A, B](acquire: SessionIO[A])(use: A => SessionIO[B])(
        release: (A, ExitCase[Throwable]) => SessionIO[Unit]
      ): F[B]
      def shift: F[Unit]
      def evalOn[A](ec: ExecutionContext)(fa: SessionIO[A]): F[A]

      // ClientSession specific
      def getPinnedServerAddress: F[ServerAddress]
      def setPinnedServerAddress(address: ServerAddress): F[Unit]
      def getRecoveryToken: F[Option[BsonDocument]]
      def setRecoveryToken(recoveryToken: BsonDocument): F[Unit]
      def getOptions: F[ClientSessionOptions]
      def isCausallyConsistent: F[Boolean]
      def getServerSession: F[ServerSession]
      def getOperationTime: F[BsonTimestamp]
      def advanceOperationTime(operationTime: BsonTimestamp): F[Unit]
      def advanceClusterTime(clusterTime: BsonDocument): F[Unit]
      def getClusterTime: F[BsonDocument]
      def hasActiveTransaction: F[Boolean]
      def notifyMessageSent(): F[Boolean]
      def getTransactionOptions: F[TransactionOptions]
      def startTransaction(): F[Unit]
      def startTransaction(options: TransactionOptions): F[Unit]
      def commitTransaction(): F[Unit]
      def abortTransaction(): F[Unit]
      def close(): F[Unit]

      // MongoClient specific
      def getDatabase(databaseName: String): F[MongoDatabase]
      def listDatabaseNames(): F[MongoIterable[String]]
      def watch(): F[ChangeStreamIterable[Document]]
      def watch(pipeline: Seq[_ <: Bson]): F[ChangeStreamIterable[Document]]
    }

    // Common operations for all algebras
    final case class Embed[A](e: Embedded[A]) extends SessionOp[A] {
      override def visit[F[_]](v: SessionOp.Visitor[F]): F[A] = v.embed(e)
    }
    final case class Delay[A](a: () => A) extends SessionOp[A] {
      override def visit[F[_]](v: SessionOp.Visitor[F]): F[A] = v.delay(a())
    }
    final case class HandleErrorWith[A](fa: SessionIO[A], f: Throwable => SessionIO[A]) extends SessionOp[A] {
      override def visit[F[_]](v: SessionOp.Visitor[F]): F[A] = v.handleErrorWith(fa, f)
    }
    final case class RaiseError[A](e: Throwable) extends SessionOp[A] {
      override def visit[F[_]](v: SessionOp.Visitor[F]): F[A] = v.raiseError(e)
    }
    final case class Async1[A](k: (Either[Throwable, A] => Unit) => Unit) extends SessionOp[A] {
      override def visit[F[_]](v: SessionOp.Visitor[F]): F[A] = v.async(k)
    }
    final case class AsyncF[A](k: (Either[Throwable, A] => Unit) => SessionIO[Unit]) extends SessionOp[A] {
      override def visit[F[_]](v: SessionOp.Visitor[F]): F[A] = v.asyncF(k)
    }
    final case class BracketCase[A, B](
      acquire: SessionIO[A],
      use: A => SessionIO[B],
      release: (A, ExitCase[Throwable]) => SessionIO[Unit]
    ) extends SessionOp[B] {
      override def visit[F[_]](v: SessionOp.Visitor[F]): F[B] = v.bracketCase(acquire)(use)(release)
    }
    final case object Shift extends SessionOp[Unit] {
      override def visit[F[_]](v: SessionOp.Visitor[F]): F[Unit] = v.shift
    }
    final case class EvalOn[A](ec: ExecutionContext, fa: SessionIO[A]) extends SessionOp[A] {
      override def visit[F[_]](v: SessionOp.Visitor[F]): F[A] = v.evalOn(ec)(fa)
    }

    // ClientSession specific
    final case object GetPinnedServerAddress extends SessionOp[ServerAddress] {
      override def visit[F[_]](v: SessionOp.Visitor[F]): F[ServerAddress] = v.getPinnedServerAddress
    }
    final case class SetPinnedServerAddress(address: ServerAddress) extends SessionOp[Unit] {
      override def visit[F[_]](v: SessionOp.Visitor[F]): F[Unit] = v.setPinnedServerAddress(address)
    }
    final case object GetRecoveryToken extends SessionOp[Option[BsonDocument]] {
      override def visit[F[_]](v: SessionOp.Visitor[F]): F[Option[BsonDocument]] = v.getRecoveryToken
    }
    final case class SetRecoveryToken(recoveryToken: BsonDocument) extends SessionOp[Unit] {
      override def visit[F[_]](v: SessionOp.Visitor[F]): F[Unit] = v.setRecoveryToken(recoveryToken)
    }
    final case object GetOptions extends SessionOp[ClientSessionOptions] {
      override def visit[F[_]](v: SessionOp.Visitor[F]): F[ClientSessionOptions] = v.getOptions
    }
    final case object IsCausallyConsistent extends SessionOp[Boolean] {
      override def visit[F[_]](v: SessionOp.Visitor[F]): F[Boolean] = v.isCausallyConsistent
    }
    final case object GetServerSession extends SessionOp[ServerSession] {
      override def visit[F[_]](v: SessionOp.Visitor[F]): F[ServerSession] = v.getServerSession
    }
    final case object GetOperationTime extends SessionOp[BsonTimestamp] {
      override def visit[F[_]](v: SessionOp.Visitor[F]): F[BsonTimestamp] = v.getOperationTime
    }
    final case class AdvanceOperationTime(operationTime: BsonTimestamp) extends SessionOp[Unit] {
      override def visit[F[_]](v: SessionOp.Visitor[F]): F[Unit] = v.advanceOperationTime(operationTime)
    }
    final case class AdvanceClusterTime(clusterTime: BsonDocument) extends SessionOp[Unit] {
      override def visit[F[_]](v: SessionOp.Visitor[F]): F[Unit] = v.advanceClusterTime(clusterTime)
    }
    final case object GetClusterTime extends SessionOp[BsonDocument] {
      override def visit[F[_]](v: SessionOp.Visitor[F]): F[BsonDocument] = v.getClusterTime
    }
    final case object HasActiveTransaction extends SessionOp[Boolean] {
      override def visit[F[_]](v: SessionOp.Visitor[F]): F[Boolean] = v.hasActiveTransaction
    }
    final case object NotifyMessageSent extends SessionOp[Boolean] {
      override def visit[F[_]](v: SessionOp.Visitor[F]): F[Boolean] = v.notifyMessageSent()
    }
    final case object GetTransactionOptions extends SessionOp[TransactionOptions] {
      override def visit[F[_]](v: SessionOp.Visitor[F]): F[TransactionOptions] = v.getTransactionOptions
    }
    final case object StartTransaction extends SessionOp[Unit] {
      override def visit[F[_]](v: SessionOp.Visitor[F]): F[Unit] = v.startTransaction()
    }
    final case class StartTransactionWithOptions(options: TransactionOptions) extends SessionOp[Unit] {
      override def visit[F[_]](v: SessionOp.Visitor[F]): F[Unit] = v.startTransaction(options)
    }
    final case object CommitTransaction extends SessionOp[Unit] {
      override def visit[F[_]](v: SessionOp.Visitor[F]): F[Unit] = v.commitTransaction()
    }
    final case object AbortTransaction extends SessionOp[Unit] {
      override def visit[F[_]](v: SessionOp.Visitor[F]): F[Unit] = v.abortTransaction()
    }
    final case object Close extends SessionOp[Unit] {
      override def visit[F[_]](v: SessionOp.Visitor[F]): F[Unit] = v.close()
    }

    // MongoClient specific
    final case class GetDatabase(databaseName: String) extends SessionOp[MongoDatabase] {
      override def visit[F[_]](v: SessionOp.Visitor[F]): F[MongoDatabase] = v.getDatabase(databaseName)
    }
    final case object ListDatabaseNames extends SessionOp[MongoIterable[String]] {
      override def visit[F[_]](v: SessionOp.Visitor[F]): F[MongoIterable[String]] = v.listDatabaseNames()
    }
    final case object Watch extends SessionOp[ChangeStreamIterable[Document]] {
      override def visit[F[_]](v: SessionOp.Visitor[F]): F[ChangeStreamIterable[Document]] = v.watch()
    }
    final case class WatchPipeline(pipeline: Seq[_ <: Bson]) extends SessionOp[ChangeStreamIterable[Document]] {
      override def visit[F[_]](v: SessionOp.Visitor[F]): F[ChangeStreamIterable[Document]] = v.watch(pipeline)
    }
  }

  import SessionOp._

  // Smart constructors for operations common to all algebras
  val unit: SessionIO[Unit] =
    FF.pure(())
  def pure[A](a: A): SessionIO[A] =
    FF.pure(a)
  def embed[F[_], J, A](j: J, fa: FF[F, A])(implicit ev: Embeddable[F, J]): SessionIO[A] =
    FF.liftF(Embed(ev.embed(j, fa)))
  def delay[A](a: => A): SessionIO[A] =
    FF.liftF(Delay(() => a))
  def handleErrorWith[A](fa: SessionIO[A], f: Throwable => SessionIO[A]): SessionIO[A] =
    FF.liftF(HandleErrorWith(fa, f))
  def raiseError[A](err: Throwable): SessionIO[A] =
    FF.liftF(RaiseError(err))
  def async[A](k: (Either[Throwable, A] => Unit) => Unit): SessionIO[A] =
    FF.liftF(Async1(k))
  def asyncF[A](k: (Either[Throwable, A] => Unit) => SessionIO[Unit]): SessionIO[A] =
    FF.liftF(AsyncF(k))
  def bracketCase[A, B](
    acquire: SessionIO[A]
  )(use: A => SessionIO[B])(release: (A, ExitCase[Throwable]) => SessionIO[Unit]): SessionIO[B] =
    FF.liftF(BracketCase(acquire, use, release))
  val shift: SessionIO[Unit] =
    FF.liftF(Shift)
  def evalOn[A](ec: ExecutionContext)(fa: SessionIO[A]): SessionIO[A] =
    FF.liftF(EvalOn(ec, fa))

  // Smart constructors for ClientSession specific operations
  val getPinnedServerAddress: SessionIO[ServerAddress] =
    FF.liftF(GetPinnedServerAddress)
  def setPinnedServerAddress(address: ServerAddress): SessionIO[Unit] =
    FF.liftF(SetPinnedServerAddress(address))
  val getRecoveryToken: SessionIO[Option[BsonDocument]] =
    FF.liftF(GetRecoveryToken)
  def setRecoveryToken(recoveryToken: BsonDocument): SessionIO[Unit] =
    FF.liftF(SetRecoveryToken(recoveryToken))
  val getOptions: SessionIO[ClientSessionOptions] =
    FF.liftF(GetOptions)
  val isCausallyConsistent: SessionIO[Boolean] =
    FF.liftF(IsCausallyConsistent)
  val getServerSession: SessionIO[ServerSession] =
    FF.liftF(GetServerSession)
  val getOperationTime: SessionIO[BsonTimestamp] =
    FF.liftF(GetOperationTime)
  def advanceOperationTime(operationTime: BsonTimestamp): SessionIO[Unit] =
    FF.liftF(AdvanceOperationTime(operationTime))
  def advanceClusterTime(clusterTime: BsonDocument): SessionIO[Unit] =
    FF.liftF(AdvanceClusterTime(clusterTime))
  val getClusterTime: SessionIO[BsonDocument] =
    FF.liftF(GetClusterTime)
  val hasActiveTransaction: SessionIO[Boolean] =
    FF.liftF(HasActiveTransaction)
  val notifyMessageSent: SessionIO[Boolean] =
    FF.liftF(NotifyMessageSent)
  val getTransactionOptions: SessionIO[TransactionOptions] =
    FF.liftF(GetTransactionOptions)
  val startTransaction: SessionIO[Unit] =
    FF.liftF(StartTransaction)
  def startTransaction(options: TransactionOptions): SessionIO[Unit] =
    FF.liftF(StartTransactionWithOptions(options))
  val commitTransaction: SessionIO[Unit] =
    FF.liftF(CommitTransaction)
  val abortTransaction: SessionIO[Unit] =
    FF.liftF(AbortTransaction)
  val close: SessionIO[Unit] =
    FF.liftF(Close)

  // Smart constructors for MongoClient specific operations
  def getDatabase(databaseName: String): SessionIO[MongoDatabase] =
    FF.liftF(GetDatabase(databaseName))
  val listDatabaseNames: SessionIO[MongoIterable[String]] =
    FF.liftF(ListDatabaseNames)
  val watch: SessionIO[ChangeStreamIterable[Document]] =
    FF.liftF(Watch)
  def watch(pipeline: Seq[_ <: Bson]): SessionIO[ChangeStreamIterable[Document]] =
    FF.liftF(WatchPipeline(pipeline))

  implicit val SessionOpEmbeddable: Embeddable[SessionOp, ClientSession] =
    new Embeddable[SessionOp, ClientSession] {
      def embed[A](j: ClientSession, fa: SessionIO[A]): Embedded[A] = Embedded.Session(j, fa)
    }

  implicit val AsyncSessionIO: Async[SessionIO] =
    new Async[SessionIO] {
      val asyncM = FF.catsFreeMonadForFree[SessionOp]
      def bracketCase[A, B](acquire: SessionIO[A])(use: A => SessionIO[B])(
        release: (A, ExitCase[Throwable]) => SessionIO[Unit]
      ): SessionIO[B] = module.bracketCase(acquire)(use)(release)
      def pure[A](x: A): SessionIO[A] = asyncM.pure(x)
      def handleErrorWith[A](fa: SessionIO[A])(f: Throwable => SessionIO[A]): SessionIO[A] =
        module.handleErrorWith(fa, f)
      def raiseError[A](e: Throwable): SessionIO[A] = module.raiseError(e)
      def async[A](k: (Either[Throwable, A] => Unit) => Unit): SessionIO[A] = module.async(k)
      def asyncF[A](k: (Either[Throwable, A] => Unit) => SessionIO[Unit]): SessionIO[A] = module.asyncF(k)
      def flatMap[A, B](fa: SessionIO[A])(f: A => SessionIO[B]): SessionIO[B] = asyncM.flatMap(fa)(f)
      def tailRecM[A, B](a: A)(f: A => SessionIO[Either[A, B]]): SessionIO[B] = asyncM.tailRecM(a)(f)
      def suspend[A](thunk: => SessionIO[A]): SessionIO[A] = asyncM.flatten(module.delay(thunk))
    }

  implicit val contextShiftSessionIO: ContextShift[SessionIO] =
    new ContextShift[SessionIO] {
      override def shift: SessionIO[Unit] = module.shift
      override def evalOn[A](ec: ExecutionContext)(fa: SessionIO[A]): SessionIO[A] = module.evalOn(ec)(fa)
    }
}
