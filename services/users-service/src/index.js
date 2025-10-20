import express from 'express';
import morgan from 'morgan';
import { createChannel } from './amqp.js';
import { ROUTING_KEYS } from '../common/events.js';
import { PrismaClient } from '@prisma/client';

const app = express();
app.use(express.json());
app.use(morgan('dev'));

const PORT = process.env.PORT || 3001;
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';
const EXCHANGE = process.env.EXCHANGE || 'app.topic';

const prisma = new PrismaClient();

let amqp = null;
(async () => {
  try {
    amqp = await createChannel(RABBITMQ_URL, EXCHANGE);
    console.log('[users] AMQP connected');
  } catch (err) {
    console.error('[users] AMQP connection failed:', err.message);
  }
})();

// Health
app.get('/health', (req, res) => res.json({ ok: true, service: 'users' }));

// Listar usuários
app.get('/', async (req, res) => {
  try {
    const users = await prisma.user.findMany();
    res.json(users);
  } catch (err) {
    console.error('[users] findMany error:', err.message);
    res.status(500).json({ error: 'Erro interno' });
  }
});

// Buscar usuário por ID
app.get('/:id', async (req, res) => {
  try {
    const user = await prisma.user.findUnique({ where: { id: req.params.id } });
    if (!user) return res.status(404).json({ error: 'not found' });
    res.json(user);
  } catch (err) {
    console.error('[users] findUnique error:', err.message);
    res.status(500).json({ error: 'Erro interno' });
  }
});

// Criar usuário
app.post('/', async (req, res) => {
  const { name, email } = req.body;
  if (!name || !email) return res.status(400).json({ error: 'name and email are required' });

  try {
    const user = await prisma.user.create({ data: { name, email } });

    if (amqp?.ch) {
      amqp.ch.publish(EXCHANGE, ROUTING_KEYS.USER_CREATED, Buffer.from(JSON.stringify(user)), { persistent: true });
      console.log('[users] published event:', ROUTING_KEYS.USER_CREATED, user.id);
    }

    res.status(201).json(user);
  } catch (err) {
    if (err.code === 'P2002') return res.status(400).json({ error: 'Email já existe' });
    console.error('[users] create error:', err.message);
    res.status(500).json({ error: 'Erro interno' });
  }
});

// Atualizar usuário
app.put('/:id', async (req, res) => {
  const { name, email } = req.body;
  if (!name && !email) return res.status(400).json({ error: 'name or email required' });

  try {
    const updatedUser = await prisma.user.update({
      where: { id: req.params.id },
      data: { name, email, updatedAt: new Date() }
    });

    if (amqp?.ch) {
      amqp.ch.publish(EXCHANGE, ROUTING_KEYS.USER_UPDATED, Buffer.from(JSON.stringify(updatedUser)), { persistent: true });
      console.log('[users] published event:', ROUTING_KEYS.USER_UPDATED, updatedUser.id);
    }

    res.json(updatedUser);
  } catch (err) {
    if (err.code === 'P2025') return res.status(404).json({ error: 'not found' });
    console.error('[users] update error:', err.message);
    res.status(500).json({ error: 'Erro interno' });
  }
});

app.listen(PORT, () => console.log(`[users] listening on http://localhost:${PORT}`));
