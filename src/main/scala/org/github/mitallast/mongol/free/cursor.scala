package org.github.mitallast.mongol.free

import cats.~>
import cats.effect.{Async, ContextShift, ExitCase}
import cats.free.{Free => FF}
import com.mongodb.client.MongoCursor

import scala.concurrent.ExecutionContext

object cursor { module =>
  sealed trait CursorOp[A] {
    def visit[F[_]](v: CursorOp.Visitor[F]): F[A]
  }

  type CursorIO[A] = FF[CursorOp, A]

  object CursorOp {
    trait Visitor[F[_]] extends (CursorOp ~> F) {
      final def apply[A](fa: CursorOp[A]): F[A] = fa.visit(this)

      // Common
      def raw[A, B](f: MongoCursor[A] => B): F[B]
      def embed[A](e: Embedded[A]): F[A]
      def delay[A](a: => A): F[A]
      def handleErrorWith[A](fa: CursorIO[A], f: Throwable => CursorIO[A]): F[A]
      def raiseError[A](e: Throwable): F[A]
      def async[A](k: (Either[Throwable, A] => Unit) => Unit): F[A]
      def asyncF[A](k: (Either[Throwable, A] => Unit) => CursorIO[Unit]): F[A]
      def bracketCase[A, B](acquire: CursorIO[A])(use: A => CursorIO[B])(
        release: (A, ExitCase[Throwable]) => CursorIO[Unit]
      ): F[B]
      def shift: F[Unit]
      def evalOn[A](ec: ExecutionContext)(fa: CursorIO[A]): F[A]

      // Cursor-specific operations
      def hasNext: F[Boolean]
      def next[A]: F[A]
      def tryNext[A]: F[Option[A]]
      def close(): F[Unit]
    }

    // Common operations for all algebras
    final case class Raw[A, B](f: MongoCursor[A] => B) extends CursorOp[B] {
      override def visit[F[_]](v: CursorOp.Visitor[F]): F[B] = v.raw(f)
    }
    final case class Embed[A](e: Embedded[A]) extends CursorOp[A] {
      override def visit[F[_]](v: CursorOp.Visitor[F]): F[A] = v.embed(e)
    }
    final case class Delay[A](a: () => A) extends CursorOp[A] {
      override def visit[F[_]](v: CursorOp.Visitor[F]): F[A] = v.delay(a())
    }
    final case class HandleErrorWith[A](fa: CursorIO[A], f: Throwable => CursorIO[A]) extends CursorOp[A] {
      override def visit[F[_]](v: CursorOp.Visitor[F]): F[A] = v.handleErrorWith(fa, f)
    }
    final case class RaiseError[A](e: Throwable) extends CursorOp[A] {
      override def visit[F[_]](v: CursorOp.Visitor[F]): F[A] = v.raiseError(e)
    }
    final case class Async1[A](k: (Either[Throwable, A] => Unit) => Unit) extends CursorOp[A] {
      override def visit[F[_]](v: CursorOp.Visitor[F]): F[A] = v.async(k)
    }
    final case class AsyncF[A](k: (Either[Throwable, A] => Unit) => CursorIO[Unit]) extends CursorOp[A] {
      override def visit[F[_]](v: CursorOp.Visitor[F]): F[A] = v.asyncF(k)
    }
    final case class BracketCase[A, B](
      acquire: CursorIO[A],
      use: A => CursorIO[B],
      release: (A, ExitCase[Throwable]) => CursorIO[Unit]
    ) extends CursorOp[B] {
      override def visit[F[_]](v: CursorOp.Visitor[F]): F[B] = v.bracketCase(acquire)(use)(release)
    }
    final case object Shift extends CursorOp[Unit] {
      override def visit[F[_]](v: CursorOp.Visitor[F]): F[Unit] = v.shift
    }
    final case class EvalOn[A](ec: ExecutionContext, fa: CursorIO[A]) extends CursorOp[A] {
      override def visit[F[_]](v: CursorOp.Visitor[F]): F[A] = v.evalOn(ec)(fa)
    }

    // Cursor-specific operations
    final case object HasNext extends CursorOp[Boolean] {
      override def visit[F[_]](v: CursorOp.Visitor[F]): F[Boolean] = v.hasNext
    }
    final case class Next[A]() extends CursorOp[A] {
      override def visit[F[_]](v: CursorOp.Visitor[F]): F[A] = v.next
    }
    final case class TryNext[A]() extends CursorOp[Option[A]] {
      override def visit[F[_]](v: CursorOp.Visitor[F]): F[Option[A]] = v.tryNext
    }
    final case object Close extends CursorOp[Unit] {
      override def visit[F[_]](v: CursorOp.Visitor[F]): F[Unit] = v.close()
    }
  }

  import CursorOp._

  // Smart constructors for operations common to all algebras
  val unit: CursorIO[Unit] =
    FF.pure(())
  def pure[A](a: A): CursorIO[A] =
    FF.pure(a)
  def raw[A, B](f: MongoCursor[A] => B): CursorIO[B] =
    FF.liftF(Raw(f))
  def embed[F[_], J, A](j: J, fa: FF[F, A])(implicit ev: Embeddable[F, J]): CursorIO[A] =
    FF.liftF(Embed(ev.embed(j, fa)))
  def delay[A](a: => A): CursorIO[A] =
    FF.liftF(Delay(() => a))
  def handleErrorWith[A](fa: CursorIO[A], f: Throwable => CursorIO[A]): CursorIO[A] =
    FF.liftF(HandleErrorWith(fa, f))
  def raiseError[A](err: Throwable): CursorIO[A] =
    FF.liftF(RaiseError(err))
  def async[A](k: (Either[Throwable, A] => Unit) => Unit): CursorIO[A] =
    FF.liftF(Async1(k))
  def asyncF[A](k: (Either[Throwable, A] => Unit) => CursorIO[Unit]): CursorIO[A] =
    FF.liftF(AsyncF(k))
  def bracketCase[A, B](
    acquire: CursorIO[A]
  )(use: A => CursorIO[B])(release: (A, ExitCase[Throwable]) => CursorIO[Unit]): CursorIO[B] =
    FF.liftF(BracketCase(acquire, use, release))
  val shift: CursorIO[Unit] =
    FF.liftF(Shift)
  def evalOn[A](ec: ExecutionContext)(fa: CursorIO[A]): CursorIO[A] =
    FF.liftF(EvalOn(ec, fa))

  // Cursor-specific operations
  val hasNext: CursorIO[Boolean] = FF.liftF(HasNext)
  def next[A]: CursorIO[A] = FF.liftF(Next[A]())
  def tryNext[A]: CursorIO[Option[A]] = FF.liftF[CursorOp, Option[A]](TryNext())
  val close: CursorIO[Unit] = FF.liftF(Close)

  implicit val CursorOpEmbeddable: Embeddable[CursorOp, MongoCursor[_]] =
    new Embeddable[CursorOp, MongoCursor[_]] {
      def embed[A](j: MongoCursor[_], fa: CursorIO[A]): Embedded[A] = Embedded.Cursor[A](j, fa)
    }

  implicit val AsyncCursorIO: Async[CursorIO] =
    new Async[CursorIO] {
      val asyncM = FF.catsFreeMonadForFree[CursorOp]
      def bracketCase[A, B](acquire: CursorIO[A])(use: A => CursorIO[B])(
        release: (A, ExitCase[Throwable]) => CursorIO[Unit]
      ): CursorIO[B] = module.bracketCase(acquire)(use)(release)
      def pure[A](x: A): CursorIO[A] = asyncM.pure(x)
      def handleErrorWith[A](fa: CursorIO[A])(f: Throwable => CursorIO[A]): CursorIO[A] =
        module.handleErrorWith(fa, f)
      def raiseError[A](e: Throwable): CursorIO[A] = module.raiseError(e)
      def async[A](k: (Either[Throwable, A] => Unit) => Unit): CursorIO[A] = module.async(k)
      def asyncF[A](k: (Either[Throwable, A] => Unit) => CursorIO[Unit]): CursorIO[A] = module.asyncF(k)
      def flatMap[A, B](fa: CursorIO[A])(f: A => CursorIO[B]): CursorIO[B] = asyncM.flatMap(fa)(f)
      def tailRecM[A, B](a: A)(f: A => CursorIO[Either[A, B]]): CursorIO[B] = asyncM.tailRecM(a)(f)
      def suspend[A](thunk: => CursorIO[A]): CursorIO[A] = asyncM.flatten(module.delay(thunk))
    }

  implicit val contextShiftCursorIO: ContextShift[CursorIO] =
    new ContextShift[CursorIO] {
      override def shift: CursorIO[Unit] = module.shift
      override def evalOn[A](ec: ExecutionContext)(fa: CursorIO[A]): CursorIO[A] = module.evalOn(ec)(fa)
    }
}
