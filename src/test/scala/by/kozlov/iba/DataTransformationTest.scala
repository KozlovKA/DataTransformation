package by.kozlov.iba

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class DataTransformationTest extends AnyFlatSpec with Matchers {
  behavior of "DataTransformation.dataTransformation"

  import DataTransformation.dataTransformation

  it should " not be empty" in {
    dataTransformation(Configuration.inputDB).isEmpty shouldBe false
  }
}
