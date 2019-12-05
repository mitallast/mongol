package org.github.mitallast.mongol.hi

//noinspection TypeAnnotation
trait Modules {
  lazy val HC = client
  lazy val HS = session
  lazy val HDB = database
  lazy val HCR = cursor
}
