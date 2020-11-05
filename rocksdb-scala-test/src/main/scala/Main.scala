import java.io.File
import org.rocksdb._
import org.rocksdb.util.SizeUnit
import scala.collection.JavaConversions._


object Hello {
    def main(args: Array[String]) = {

	val tmpFile = File.createTempFile("rocksdb", ".db")
	val tmpFileName = tmpFile.getAbsolutePath
	tmpFile.delete

	var options = new Options().setCreateIfMissing(true)
	RocksDB.loadLibrary()
        var  store = RocksDB.open(options, tmpFileName )

	var isOpen : Boolean = true
	val UTF8  : String = "UTF-8"
	def put( k:String , v:String) = {
		assert(isOpen)
		store.put(
		    k.getBytes(UTF8),
		    v.getBytes(UTF8)
		)
	}

        println("Hello, world")
    }
}



