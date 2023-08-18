import java.sql.{Connection, DriverManager,ResultSet}

class SQLServerConnection(jdbcUrl: String, username: String, password: String) {
  private var connection: Connection = _

  // Método para abrir a conexão
  def openConnection(): Unit = {
    try {
      Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
      val connection: Connection = DriverManager.getConnection(jdbcUrl, username, password)

      // Criar uma consulta SQL
      val query = """select * from INFORMATION_SCHEMA.TABLES"""

      val statement = connection.createStatement()
      // Executar a consulta e obter o ResultSet
      val resultSet: ResultSet = statement.executeQuery(query)
      // Processar os resultados
      while (resultSet.next()) {
        val TABLE_NAME = resultSet.getString("TABLE_NAME")
//        val TABLE_TYPE = resultSet.getString("TABLE_TYPE")
        println(s"TABLE_NAME: $TABLE_NAME")
      }

    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw new RuntimeException("Erro ao abrir a conexão com o SQL Server.")
    }
  }

  // Método para fechar a conexão
  def closeConnection(): Unit = {
    try {
      if (connection != null) {
        connection.close()
        println("Conexão com o SQL Server fechada.")
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw new RuntimeException("Erro ao fechar a conexão com o SQL Server.")
    }
  }
}

object Main {
  def main(args: Array[String]): Unit = {
    val jdbcUrl = "jdbc:sqlserver://localhost:1433;database=DBDWP511"
    val username = "sa"
    val password = "Numsey@Password!"

    val sqlServerConnection = new SQLServerConnection(jdbcUrl, username, password)
    sqlServerConnection.openConnection()

    // Realize operações no banco de dados aqui...

    sqlServerConnection.closeConnection()
  }
}
