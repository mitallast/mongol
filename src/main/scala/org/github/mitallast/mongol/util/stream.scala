package org.github.mitallast.mongol.util

import fs2.{Chunk, Stream}

object stream {
  def repeatEvalChunks[F[_], A](fa: F[Chunk[A]]): Stream[F, A] =
    Stream.repeatEval(fa).takeWhile(_.nonEmpty).flatMap(Stream.chunk)
}
