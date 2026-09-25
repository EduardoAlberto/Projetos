# Dashboards CRAI

Dashboard Streamlit com gráficos Plotly originados diretamente do PostgreSQL.

## Estrutura

- `app.py`: interface e composição dos gráficos.
- `src/db.py`: configuração segura e pool de conexões.
- `src/queries.py`: consultas somente leitura da camada bronze.
- `.env.example`: modelo das variáveis de ambiente.

## Executar

```bash
cd dashboards
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Preencha `DB_USER` e `DB_PASSWORD` no `.env`. Depois execute:

```bash
streamlit run app.py
```

O endereço padrão será `http://localhost:8501`.

Também é possível executar diretamente pelo script:

```bash
./run_dashboard.sh
```

Argumentos adicionais são encaminhados ao Streamlit, por exemplo `./run_dashboard.sh --server.port 8502`.

## Boas práticas aplicadas

- Credenciais fora do código e fora do Git.
- Pool limitado de conexões PostgreSQL.
- Consultas SQL somente leitura e tabela definida internamente.
- Cache de dados com TTL de 5 minutos.
- Tratamento de erro sem expor detalhes da conexão na tela.
- Normalização explícita apenas para agrupamento visual; a origem não é alterada.
