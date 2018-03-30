package DAO

import java.io.{FileNotFoundException, IOException}

import scala.io.BufferedSource

class FileReader  extends Serializable{

  def readFile (firstTextFileName: String) :  BufferedSource = {
    val filename = scala.io.Source.fromFile(firstTextFileName)

    try {

      val filename = scala.io.Source.fromFile(firstTextFileName)

      return filename

    } catch {
      case e: FileNotFoundException => println("Couldn't find that file.")
      case e: IOException => println("Got an IOException!")
    }

    return null
  }
}
