/**
  * Created by zhaokangpan on 2016/12/19.
  */
object ProcessTest {

  def test(pp : PP) : Unit ={
    pp.a += 2
    pp.b += 2
  }

  def main(args : Array[String]): Unit ={
    val a = new PP()
    test(a)
    println("a : " + a.a)
    println("b : " + a.b)
  }
}
