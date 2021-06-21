package by.kozlov.iba

import by.kozlov.iba.DataLoad.connectURL
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable.ArrayBuffer

class DataLoadTest extends AnyFlatSpec with Matchers {
  behavior of "DataLoad.productIDDefinition"

  import DataLoad.productIDDefinition

  it should "start ID with 0" in {
    productIDDefinition().apply(0) shouldBe 0
    productIDDefinition().apply(5) shouldBe 5
  }
  behavior of "DataLoad.tableCreation"

  import DataLoad.tableCreation

  it should " not be empty" in {
    tableCreation(connectURL(), ArrayBuffer(0), ArrayBuffer(0), ArrayBuffer(0), ArrayBuffer(0), ArrayBuffer(0), ArrayBuffer(0),
      ArrayBuffer(0), ArrayBuffer(0), ArrayBuffer(0), ArrayBuffer(0), ArrayBuffer(0), ArrayBuffer(0), ArrayBuffer(0), ArrayBuffer(0),
      ArrayBuffer(0),0).isEmpty shouldBe false
  }

}

