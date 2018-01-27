/**
  * Created by sq on 2017/11/19.
  */
object TestIterator {
  def main(args: Array[String]) {

   // val iter =  Iterator (List(1,2,3))
   val iter =  Iterator (List(1,2,3))
    val iter2 =  Iterator (1,2,3)
    while (iter.hasNext){
      //iter.length 使iter 已经到达末尾了
      //print(iter.length)
      iter.next()
      println(iter.length)

    }
    val iter3 =  Iterator (1,2,3)
    iter3.foreach(print)
    println(iter3.length)

    while (iter2.hasNext){
      //iter.length 使iter 已经到达末尾了
      //print(iter.length)
      iter2.next()
      print(iter2.length)

    }
  }
}
