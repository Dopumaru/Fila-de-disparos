📦 Fila de Disparos — Redis + BullMQ + Telegram

Este projeto implementa uma fila de disparos escalável usando Node.js, Redis e BullMQ, com envio de mensagens via Telegram Bot.
Preparado para rodar localmente e em VPS (DigitalOcean).

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

PM2 (produção)

🖥️ RODANDO NA VPS (PASSO A PASSO)
1️⃣ Acessar a VPS (DigitalOcean)

O responsável vai fornecer:

IP da VPS

Usuário (geralmente root)

Senha ou chave SSH

Exemplo:
ssh root@IP_DA_VPS

2️⃣ Atualizar o sistema
apt update && apt upgrade -y

3️⃣ Instalar dependências básicas
apt install git curl redis-server -y

4️⃣ Instalar Node.js (LTS)
curl -fsSL https://deb.nodesource.com/setup_20.x | bash -
apt install nodejs -y


Verificar:

node -v
npm -v

5️⃣ Clonar o repositório
git clone https://github.com/SEU_USUARIO/fila-disparos.git
cd fila-disparos

6️⃣ Criar o arquivo .env

⚠️ Esse arquivo NÃO vem do Git por segurança

nano .env


Conteúdo:

TELEGRAM_TOKEN=TOKEN_REAL_DO_BOT


Salvar:

CTRL + O

Enter

CTRL + X

7️⃣ Instalar dependências do projeto
npm install


Isso recria automaticamente o node_modules.

8️⃣ Garantir que o Redis está rodando
systemctl start redis-server
systemctl enable redis-server


Testar:

redis-cli ping


Resposta esperada:

PONG

9️⃣ Rodar o worker (teste rápido)
node worker.js


Se não der erro, está tudo certo.

Interromper:

CTRL + C

🔁 PRODUÇÃO (RODAR 24/7 COM PM2)
🔹 Instalar PM2
npm install -g pm2

🔹 Subir o worker
pm2 start worker.js --name fila-worker

🔹 Salvar configuração
pm2 save
pm2 startup


(O comando pm2 startup vai mostrar outro comando — copie e cole ele)

🔹 Ver status
pm2 status
pm2 logs fila-worker

📤 DISPAROS (PRODUCER)

⚠️ O producer deve ser executado com cuidado
Cada execução adiciona jobs à fila.

Exemplo:

node producer.js


Recomendado:

rodar uma vez

ou adaptar para leitura de banco / CSV

🧠 BOAS PRÁTICAS

Nunca subir .env no Git

Nunca subir node_modules

Worker sempre ligado antes do producer

Disparos grandes são lentos por segurança

Rate limit evita ban

🛑 EM CASO DE PROBLEMA

Ver logs:

pm2 logs fila-worker


Reiniciar worker:

pm2 restart fila-worker
