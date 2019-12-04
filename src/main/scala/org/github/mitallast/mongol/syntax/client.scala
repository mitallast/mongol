package org.github.mitallast.mongol.syntax

import cats.Monad
import org.github.mitallast.mongol.free.client.ClientIO
import org.github.mitallast.mongol.util.transactor.Transactor

class ClientIOOps[A](ma: ClientIO[A]) {
  def transact[M[_]](xa: Transactor[M])(implicit ev: Monad[M]): M[A] = xa.trans.apply(ma)
}

trait ToClientIOOpsOps {
  implicit def toClientIOOpsOps[A](ma: ClientIO[A]): ClientIOOps[A] = new ClientIOOps(ma)
}
