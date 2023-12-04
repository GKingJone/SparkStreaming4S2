import scala.collection.mutable.ArrayBuffer

import org.apache.avro.ipc.specific.Person


object Test11 {
  
  var name:String = _
  
  case class Person(age : Int, name : String)
  
  def main(args: Array[String]) {
    
    var s = "20160708121212"
    
    var map: ArrayBuffer[Person] =new  ArrayBuffer[Person]
    map += Person(44, "jack01")
    map += Person(44, "jack01")
    val caseClassDS = map.toArray
    
    println(caseClassDS.toSeq)
    
  }
  
  def setName(){
    name = "gao|lei|niu"
  }
}