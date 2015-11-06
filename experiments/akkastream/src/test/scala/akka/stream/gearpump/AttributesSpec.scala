package akka.stream.gearpump

import akka.stream.Attributes
import org.scalatest.{FlatSpec, Matchers, WordSpec}

/**
 * Created by xzhong10 on 2015/11/6.
 */
class AttributesSpec extends FlatSpec with Matchers {
  it should "merge the attributes together" in {
    val a = Attributes.name("aa")
    val b = Attributes.name("bb")

    val c = a and b

    println("DD:" + c.nameOrDefault())
  }

}
