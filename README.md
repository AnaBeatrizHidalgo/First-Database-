# First-Database

**Autores:**  
- Ana Beatriz Hidalgo  
- André Lucas Loubet Souza

**Disciplina:** MC536 – Banco de Dados (Unicamp, 1º semestre 2025)  
**Tema:** Banco de dados voltado para os ODS (Objetivos de Desenvolvimento Sustentável)  03 (Saúde e Bem-Estar), 07 (Energia Acessível e Limpa) e 13 (Ação Contra a Mudança Global do Clima).

## 🔍 Sobre

Este projeto foi desenvolvido como trabalho da disciplina MC536 – Banco de Dados da Unicamp, primeiro semestre de 2025, com foco em alinhamento aos ODS selecionados.

É implementado um banco de dados PostgreSQL para armazenar e analisar informações relacionadas aos ODS selecionados. A modelagem abrange desde o modelo conceitual até o modelo físico, e a população dos dados é feita via script Python.

---

## Sumário

[Visão Geral do Projeto](#visão-geral-do-projeto)
[Sobre](#sobre)  
[Objetivos](#objetivos)  
[Estrutura do Repositório](#estrutura-do-repositório)  
[Tecnologias Utilizadas](#tecnologias-utilizadas)
[Configuração e Execução](#configuração-e-execução)  
[Modelagem de Dados](#modelagem-de-dados)  
[População do Banco](#população-do-banco)  
[Consultas SQL](#consultas-sql)  
[Scripts Extras](#scripts-extras)  

---

## ✅ Objetivos

- Modelar conceitual e logicamente os dados referentes aos ODS 03, 07 e 13.  
- Implementar a estrutura física (tabelas, chaves primárias/estrangeiras) no PostgreSQL.  
- Criar scripts de carga automatizada dos dados.  
- Desenvolver consultas SQL para extrair insights relevantes.

---

## 📂 Estrutura do Repositório
First-Database/
├── Consultas/ # Arquivos .sql com consultas pré-definidas
├── Datasets/ # Conjuntos de dados brutos (CSV, etc.)
├── Modelos/ # Diagramas ER conceitual, lógico e físico
├── Scripts Extras/ # Scripts auxiliares (ex.: procedures, funções, índices)
├── populate_scripts/ # Script Python para criação e carga de tabelas
├── .gitignore
└── README.md

---

## 🛠️ Tecnologias Utilizadas

1. **PostgreSQL 17.4**: instalado e em execução.

2. **PgAdmin4 9.2**: para administração do banco.

3. **Python 3.8+** e **pip**.

---

## 🚀 Configuração e Execução

1. **Clone o repositório:**
```
git clone https://github.com/AnaBeatrizHidalgo/First-Database-.git
cd First-Database-
```

2. Instale dependências:
```
pip install -r requirements.txt
```

3. Criação de um arquivo `.env` na raiz com a variável:

```
DB_URL=postgresql://<usuário>:<senha>@<host>:<porta>/<nome_do_banco>
```

4. Execute o script de população:
```
python populate_scripts/populate_db.py
```

5. Valide o carregamento acessando o PostgreSQL ou usando ferramentas como pgAdmin.

## 📈 Modelagem de Dados

Os diagramas de modelo estão em `Modelos/`:

- `Conceitual.png` - Modelo Conceitual (ERD)
![Visão do Modelo Conceitual](\Modelos\modeloConceitual.drawio.png)
- `Logico.png` - Modelo Lógico (relacional)
![Visão do Modelo Logico](\Modelos\ModeloRelacional.png)
- `Fisico.png` - DDL com as instruções `CREATE TABLE`

## 📊 População do Banco

O script `populate_scripts/populate_db.py`:

    Conecta ao PostgreSQL utilizando `DB_URL` do `.env.

    Cria esquemas e tabelas (se ainda não existirem).

    Insere dados a partir dos CSVs em `Datasets/`.

## 📄 Consultas SQL

As consultas em SQL foram encapsuladas em Python. Basta ter estabelecido a conexão com o banco de dados anteriormente e executar os scripts abaixo.

Em `Consultas/ estão as consultas mais relevantes para análise, por exemplo:

- `/execute_firstquery.py`

- `/execute_secondquery.py`

- `/execute_thirdquery.py`

- `/execute_fourthquery.py`

- `/execute_fifthquery.py`

Certifique-se de substituir `<usuario>` e `<nome_do_banco>` pelos valores corretos.

## 🔧 Scripts Extras

Pasta com scripts adicionais para:

- Criação de índices otimizados.

- Funções armazenadas para cálculos recorrentes.

- Gatilhos (triggers) de auditoria.

Use conforme necessidade, consultando o cabeçalho de cada arquivo.
