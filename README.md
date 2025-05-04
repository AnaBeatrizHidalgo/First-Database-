# First-Database
Projeto criado por:
Ana Beatriz Hidalgo
André Lucas Loubet Souza

Este projeto foi impulsionado pela matéria de Banco de Dados, que solicitava a criação de um banco com a temática voltada para os ODS (Objetivos de Desenvolvimento Sustentável). Foram escolhidos os seguintes ODS:
03: Saúde e bem-estar
07: Energia Acessível e Limpa
13: Ação contra a mudança global do clima

Como executar o banco de dados:
1.Crie um banco vazio no seu sistema de gerenciamento (ex: PostgreSQL).
2.Rode o script populate_db.py (localizado na pasta populate_scripts).

CUIDADO!! Antes de executar o script, é necessário:
1.Criar um arquivo chamado .env
2.Configurá-lo com as credenciais do seu banco no .env no seguinte formato: 
    DB_URL=postgresql://User_DB:Password_DB@Host_DB:Port_DB/Name_DB
Onde:
User_DB: Nome do usuário do banco.
- Password_DB: Senha do usuário.
- Host_DB: Host do banco (use localhost se for local).
- Port_DB: Porta de conexão (padrão do PostgreSQL: 5432).
- Name_DB: Nome do banco criado (evite caracteres especiais ou espaços).

Depois de configurar o seu banco, você pode validar o funcionamento com as consultas disponíveis no projeto.
