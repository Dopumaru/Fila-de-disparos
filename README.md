📦 Fila de Disparos — Redis + BullMQ + Telegram (EasyPanel)

Este projeto implementa uma fila de disparos escalável usando Node.js, Redis e BullMQ, com envio de mensagens via Telegram Bot.
Preparado para rodar localmente e em VPS usando EasyPanel.

🚀 Objetivo

Processar milhares de disparos sem travar

Controlar velocidade (rate limit)

Evitar ban de API

Rodar 24/7 em produção

🧱 Stack

Node.js

Redis

BullMQ

node-telegram-bot-api

EasyPanel (produção)

🖥️ RODANDO NA VPS COM EASYPANEL (PASSO A PASSO)
1️⃣ Criar o serviço Redis no EasyPanel

Acesse o EasyPanel

Clique em Create → Service → Redis

Defina:

Nome do serviço (ex: redis-fila)

Porta padrão (6379)

Senha (opcional, mas recomendado)

📌 Guarde:

REDIS_HOST → normalmente o nome do serviço

REDIS_PORT → 6379

REDIS_PASSWORD → se configurada

2️⃣ Criar o App do Worker (consumidor da fila)

Create → App

Escolha App from Git

Conecte seu GitHub e selecione o repositório

Configure:

Install command:

npm install


Build command: (vazio)

Run command:

npm run start:worker

🔐 Variáveis de ambiente (ENV)

Adicionar no App:

REDIS_HOST=redis-fila
REDIS_PORT=6379
REDIS_PASSWORD=senha_se_existir
TELEGRAM_BOT_TOKEN=TOKEN_REAL_DO_BOT


Salvar e Deploy.

📌 O worker deve ficar rodando 24/7 aguardando jobs.

3️⃣ Criar o App do Producer (disparador)

Criar outro App, apontando para o mesmo repositório.

Run command:

npm run start:producer

🔐 Variáveis de ambiente (ENV)

Adicionar:

REDIS_HOST=redis-fila
REDIS_PORT=6379
REDIS_PASSWORD=senha_se_existir
TELEGRAM_BOT_TOKEN=TOKEN_REAL_DO_BOT
TELEGRAM_CHAT_ID=ID_DO_CHAT


Deploy.

⚠️ Atenção:
O producer adiciona jobs na fila. Cada deploy/execução gera novos disparos.

🔄 Fluxo correto de execução

Redis sempre ativo

Worker sempre ligado

Producer executado apenas quando necessário

📤 DISPAROS (PRODUCER)

O producer.js pode ser adaptado para:

leitura de banco de dados

leitura de CSV

campanhas agendadas

📌 Recomenda-se:

Executar o producer apenas quando necessário

Usar rate limit para evitar bloqueios

🧠 Boas práticas

❌ Nunca subir .env no Git

❌ Nunca subir node_modules

✅ Worker sempre antes do producer

✅ Usar variáveis de ambiente

✅ Monitorar logs pelo EasyPanel

🛑 Em caso de problema
Ver logs

Pelo Dashboard do App no EasyPanel

Reiniciar

Botão Restart App

Erros comuns

REDIS_HOST incorreto

Redis não iniciado

Worker e Producer apontando para Redis diferente
