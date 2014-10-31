package org.apache.gearpump

import java.io.{ByteArrayOutputStream, File, FileInputStream}

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer

package object streaming {
  private val LOG: Logger = LoggerFactory.getLogger("org.apache.gearpump.streaming")

  type TaskGroup = Int
  type TaskIndex = Int

  implicit def fileToTaskJar(file: File): Option[TaskJar] = {
    if(file.exists) {
      val fis = new FileInputStream(file)
      val bos: ByteArrayOutputStream = new ByteArrayOutputStream()
      val buf = ListBuffer[Byte]()
      var b = fis.read()
      while (b != -1) {
        buf.append(b.byteValue)
        b = fis.read()
      }
      Option(TaskJar(file.getName, buf.toArray))
    } else {
      LOG.error(s"Could not open ${file.getName}")
      None
    }
  }
  /* Not used - experimenting with Monads
trait Monoid[T] {
  self =>
  def zero: T
  def append(t1: T, t2: => T): T
}
trait Functor[M[_]] {
  def fmap[A, B](f: A => B): M[A] => M[B]
}
object Functorise {
  implicit def enrichWithFunctor[M[_], A](m: M[A]) = new {
    def mapWith[B](f: A => B)(implicit functor: Functor[M]): M[B] = functor.fmap(f)(m)
  }
}
trait Monad[M[_]] {
  def apply[T<%java.io.Serializable](t: => T): M[T]
  def flatMap[T, U](m: M[T])(fn: T => M[U]): M[U]
}
object MessageMonad extends Monad[Message] {
  def apply[T<%java.io.Serializable](t: => T): Message[T] = Message(t)
  def flatMap[T, U](m: Message[T])(fn: T => Message[U]): Message[U] = {
    fn(m.msg)
  }
}
object Monadize {
  implicit def enrichWithMonad[M[_], A](m: M[A]) = new {
    def mapWith[B](f: A => M[B])(implicit monad: Monad[M]): M[B] = monad.flatMap[A,B](m)(f)
  }
}
case class State[S, A](run: S => (A, S)) {
  def map[B](f: A => B): State[S, B] =
    State(s => {
      val (a, t) = run(s)
      (f(a), t)
    })
  def flatMap[B](f: A => State[S, B]): State[S, B] =
    State(s => {
      val (a, t) = run(s)
      f(a) run t
    })
  def eval(s: S): A =
    run(s)._1
}
object State {
  def insert[S, A](a: A): State[S, A] =
    State(s => (a, s))
  def get[S, A](f: S => A): State[S, A] =
    State(s => (f(s), s))
  def mod[S](f: S => S): State[S, Unit] =
    State(s => ((), f(s)))
}
   */
}
