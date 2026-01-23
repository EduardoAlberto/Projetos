# Netflix Data Pipeline

## 🚀 Como Usar

### Pré-requisitos
- Python 3.9+
- PySpark instalado
- PostgreSQL rodando localmente

### Configuração do Ambiente

#### Opção 1: Variáveis de Ambiente (Recomendado)

```bash
# Defina as variáveis de ambiente
export NETFLIX_INPUT_PATH="/caminho/para/input"
export NETFLIX_CSV_FILE="netflix_titles_clean.csv"
export NETFLIX_OUTPUT_PATH="/caminho/para/output"

# Executar o pipeline
python3 main.py
```

#### Opção 2: Arquivo `.env`

1. Copie o arquivo de exemplo:
```bash
cp .env.example .env
```

2. Edite o `.env` com seus caminhos:
```
NETFLIX_INPUT_PATH=./data/input
NETFLIX_CSV_FILE=netflix_titles_clean.csv
NETFLIX_OUTPUT_PATH=./data/output
```

3. Instale `python-dotenv`:
```bash
pip install python-dotenv
```

4. Atualize o início do `main.py`:
```python
from dotenv import load_dotenv
load_dotenv()
```

#### Opção 3: Estrutura de Diretórios Padrão (Mais Simples)

Crie a estrutura:
```
projeto/
├── main.py
├── config/
├── src/
├── data/
│   ├── input/
│   │   └── netflix_titles_clean.csv
│   └── output/
```

Então execute:
```bash
python3 main.py
```

## 📊 Fluxo do Pipeline

1. **Validação**: Verifica se o arquivo CSV existe
2. **Ingestão**: Lê o CSV com Spark
3. **Processamento**: Aplica transformações
4. **Armazenamento**: Salva em Data Lake (Parquet) e PostgreSQL
5. **Qualidade**: Verifica frequência de dados

## ✅ Boas Práticas Implementadas

- ✓ Paths configuráveis sem hardcoding
- ✓ Variáveis de ambiente para flexibilidade
- ✓ Validação de arquivos antes de processar
- ✓ Criação automática de diretórios
- ✓ Mensagens de erro descritivas
- ✓ Tratamento de exceções apropriado
