package oobject

object Enums {
  enum  Permissions {
    case READ, WRITE, EXECUTE, NONE

    def openDocument(): Unit =
      if (this == READ) println("opening documento...")
      else println("reading not allowed.")
  }
  val somePermissoion: Permissions = Permissions.READ

  enum PermissionWithBits(bits: Int){
    case READ extends PermissionWithBits(4)
    case WRITE extends PermissionWithBits(2)
    case EXECUTE extends PermissionWithBits(1)
    case NONE extends PermissionWithBits(0)
  }
  object  PermissionWithBits{
    def fromBits(bits: Int): PermissionWithBits = {
      PermissionWithBits.NONE
    }
  }

  val somePermissionsOrdinal = somePermissoion.ordinal
  val allPermission = PermissionWithBits.values
  val readPermission: Permissions = Permissions.valueOf("READ")

  def main(args: Array[String]): Unit = {
    somePermissoion.openDocument()
    println(somePermissionsOrdinal)
  }
}
