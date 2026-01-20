
const database = 'Dev0559';
const collection = 'colaboradores';

// The current database to use.
use(database);

// Create a new collection.
db.createCollection(collection);

// insert na tabela

db.colaboradores.insertOne({
  nome: "Eduardo",
  cargo: "Analista de Dados",
  departamento: "TI",
  salario: 5000
});


