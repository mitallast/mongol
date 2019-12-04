package org.github.mitallast.mongol.free

import cats.~>
import cats.effect.{Async, ContextShift, ExitCase}
import cats.free.{Free => FF}
import com.mongodb.ClientSessionOptions
import com.mongodb.client.{ChangeStreamIterable, ClientSession, MongoClient, MongoDatabase, MongoIterable}
import com.mongodb.connection.ClusterDescription
import org.bson.Document
import org.bson.conversions.Bson

import scala.concurrent.ExecutionContext

object client { module =>

  sealed trait ClientOp[A] {
    def visit[F[_]](v: ClientOp.Visitor[F]): F[A]
  }

  type ClientIO[A] = FF[ClientOp, A]

  object ClientOp {
    trait Visitor[F[_]] extends (ClientOp ~> F) {
      final def apply[A](fa: ClientOp[A]): F[A] = fa.visit(this)

      // Common
      def embed[A](e: Embedded[A]): F[A]
      def delay[A](a: => A): F[A]
      def handleErrorWith[A](fa: ClientIO[A], f: Throwable => ClientIO[A]): F[A]
      def raiseError[A](e: Throwable): F[A]
      def async[A](k: (Either[Throwable, A] => Unit) => Unit): F[A]
      def asyncF[A](k: (Either[Throwable, A] => Unit) => ClientIO[Unit]): F[A]
      def bracketCase[A, B](acquire: ClientIO[A])(use: A => ClientIO[B])(
        release: (A, ExitCase[Throwable]) => ClientIO[Unit]
      ): F[B]
      def shift: F[Unit]
      def evalOn[A](ec: ExecutionContext)(fa: ClientIO[A]): F[A]

      // Specific
      def getDatabase(databaseName: String): F[MongoDatabase]
      def startSession(): F[ClientSession]
      def startSession(options: ClientSessionOptions): F[ClientSession]
      def close(): F[Unit]
      def listDatabaseNames(): F[MongoIterable[String]]
      def watch(): F[ChangeStreamIterable[Document]]
      def watch(pipeline: Seq[_ <: Bson]): F[ChangeStreamIterable[Document]]
      def getClusterDescription: F[ClusterDescription]
    }

    // Common operations for all algebras
    final case class Embed[A](e: Embedded[A]) extends ClientOp[A] {
      override def visit[F[_]](v: ClientOp.Visitor[F]): F[A] = v.embed(e)
    }
    final case class Delay[A](a: () => A) extends ClientOp[A] {
      override def visit[F[_]](v: ClientOp.Visitor[F]): F[A] = v.delay(a())
    }
    final case class HandleErrorWith[A](fa: ClientIO[A], f: Throwable => ClientIO[A]) extends ClientOp[A] {
      override def visit[F[_]](v: ClientOp.Visitor[F]): F[A] = v.handleErrorWith(fa, f)
    }
    final case class RaiseError[A](e: Throwable) extends ClientOp[A] {
      override def visit[F[_]](v: ClientOp.Visitor[F]): F[A] = v.raiseError(e)
    }
    final case class Async1[A](k: (Either[Throwable, A] => Unit) => Unit) extends ClientOp[A] {
      override def visit[F[_]](v: ClientOp.Visitor[F]): F[A] = v.async(k)
    }
    final case class AsyncF[A](k: (Either[Throwable, A] => Unit) => ClientIO[Unit]) extends ClientOp[A] {
      override def visit[F[_]](v: ClientOp.Visitor[F]): F[A] = v.asyncF(k)
    }
    final case class BracketCase[A, B](
      acquire: ClientIO[A],
      use: A => ClientIO[B],
      release: (A, ExitCase[Throwable]) => ClientIO[Unit]
    ) extends ClientOp[B] {
      override def visit[F[_]](v: ClientOp.Visitor[F]): F[B] = v.bracketCase(acquire)(use)(release)
    }
    final case object Shift extends ClientOp[Unit] {
      override def visit[F[_]](v: ClientOp.Visitor[F]): F[Unit] = v.shift
    }
    final case class EvalOn[A](ec: ExecutionContext, fa: ClientIO[A]) extends ClientOp[A] {
      override def visit[F[_]](v: ClientOp.Visitor[F]): F[A] = v.evalOn(ec)(fa)
    }

    // Client specific
    final case class GetDatabase(databaseName: String) extends ClientOp[MongoDatabase] {
      override def visit[F[_]](v: ClientOp.Visitor[F]): F[MongoDatabase] = v.getDatabase(databaseName)
    }
    final case object StartSession extends ClientOp[ClientSession] {
      override def visit[F[_]](v: ClientOp.Visitor[F]): F[ClientSession] = v.startSession()
    }
    final case class StartSessionWithOptions(options: ClientSessionOptions) extends ClientOp[ClientSession] {
      override def visit[F[_]](v: ClientOp.Visitor[F]): F[ClientSession] = v.startSession(options)
    }
    final case object Close extends ClientOp[Unit] {
      override def visit[F[_]](v: ClientOp.Visitor[F]): F[Unit] = v.close()
    }
    final case object ListDatabaseNames extends ClientOp[MongoIterable[String]] {
      override def visit[F[_]](v: ClientOp.Visitor[F]): F[MongoIterable[String]] = v.listDatabaseNames()
    }
    final case object Watch extends ClientOp[ChangeStreamIterable[Document]] {
      override def visit[F[_]](v: ClientOp.Visitor[F]): F[ChangeStreamIterable[Document]] = v.watch()
    }
    final case class WatchWithPipeline(pipeline: Seq[_ <: Bson]) extends ClientOp[ChangeStreamIterable[Document]] {
      override def visit[F[_]](v: ClientOp.Visitor[F]): F[ChangeStreamIterable[Document]] = v.watch(pipeline)
    }
    final case object GetClusterDescription extends ClientOp[ClusterDescription] {
      override def visit[F[_]](v: ClientOp.Visitor[F]): F[ClusterDescription] = v.getClusterDescription
    }
  }

  import ClientOp._

  // Smart constructors for operations common to all algebras
  val unit: ClientIO[Unit] =
    FF.pure(())
  def pure[A](a: A): ClientIO[A] =
    FF.pure(a)
  def embed[F[_], J, A](j: J, fa: FF[F, A])(implicit ev: Embeddable[F, J]): ClientIO[A] =
    FF.liftF(Embed(ev.embed(j, fa)))
  def delay[A](a: => A): ClientIO[A] =
    FF.liftF(Delay(() => a))
  def handleErrorWith[A](fa: ClientIO[A], f: Throwable => ClientIO[A]): ClientIO[A] =
    FF.liftF(HandleErrorWith(fa, f))
  def raiseError[A](err: Throwable): ClientIO[A] =
    FF.liftF(RaiseError(err))
  def async[A](k: (Either[Throwable, A] => Unit) => Unit): ClientIO[A] =
    FF.liftF(Async1(k))
  def asyncF[A](k: (Either[Throwable, A] => Unit) => ClientIO[Unit]): ClientIO[A] =
    FF.liftF(AsyncF(k))
  def bracketCase[A, B](
    acquire: ClientIO[A]
  )(use: A => ClientIO[B])(release: (A, ExitCase[Throwable]) => ClientIO[Unit]): ClientIO[B] =
    FF.liftF(BracketCase(acquire, use, release))
  val shift: ClientIO[Unit] =
    FF.liftF(Shift)
  def evalOn[A](ec: ExecutionContext)(fa: ClientIO[A]): ClientIO[A] =
    FF.liftF(EvalOn(ec, fa))

  // Smart constructors for ClientSession specific operations
  def getDatabase(databaseName: String): ClientIO[MongoDatabase] =
    FF.liftF(GetDatabase(databaseName))
  val startSession: ClientIO[ClientSession] =
    FF.liftF(StartSession)
  def startSession(options: ClientSessionOptions): ClientIO[ClientSession] =
    FF.liftF(StartSessionWithOptions(options))

  def transaction[A](fa: SessionIO[A]): ClientIO[A] =
    for {
      session <- startSession
      a <- embed(session, CS.transaction(fa))
    } yield a

  val close: ClientIO[Unit] =
    FF.liftF(Close)
  val listDatabaseNames: ClientIO[MongoIterable[String]] =
    FF.liftF(ListDatabaseNames)
  val watch: ClientIO[ChangeStreamIterable[Document]] =
    FF.liftF(Watch)
  def watch(pipeline: Seq[_ <: Bson]): ClientIO[ChangeStreamIterable[Document]] =
    FF.liftF(WatchWithPipeline(pipeline))
  val getClusterDescription: ClientIO[ClusterDescription] =
    FF.liftF(GetClusterDescription)

  implicit val AsyncClientIO: Async[ClientIO] =
    new Async[ClientIO] {
      val asyncM = FF.catsFreeMonadForFree[ClientOp]
      def bracketCase[A, B](acquire: ClientIO[A])(use: A => ClientIO[B])(
        release: (A, ExitCase[Throwable]) => ClientIO[Unit]
      ): ClientIO[B] = module.bracketCase(acquire)(use)(release)
      def pure[A](x: A): ClientIO[A] = asyncM.pure(x)
      def handleErrorWith[A](fa: ClientIO[A])(f: Throwable => ClientIO[A]): ClientIO[A] =
        module.handleErrorWith(fa, f)
      def raiseError[A](e: Throwable): ClientIO[A] = module.raiseError(e)
      def async[A](k: (Either[Throwable, A] => Unit) => Unit): ClientIO[A] = module.async(k)
      def asyncF[A](k: (Either[Throwable, A] => Unit) => ClientIO[Unit]): ClientIO[A] = module.asyncF(k)
      def flatMap[A, B](fa: ClientIO[A])(f: A => ClientIO[B]): ClientIO[B] = asyncM.flatMap(fa)(f)
      def tailRecM[A, B](a: A)(f: A => ClientIO[Either[A, B]]): ClientIO[B] = asyncM.tailRecM(a)(f)
      def suspend[A](thunk: => ClientIO[A]): ClientIO[A] = asyncM.flatten(module.delay(thunk))
    }

  implicit val contextShiftClientIO: ContextShift[ClientIO] =
    new ContextShift[ClientIO] {
      override def shift: ClientIO[Unit] = module.shift
      override def evalOn[A](ec: ExecutionContext)(fa: ClientIO[A]): ClientIO[A] = module.evalOn(ec)(fa)
    }
}
