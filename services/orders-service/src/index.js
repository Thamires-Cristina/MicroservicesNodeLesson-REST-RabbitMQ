import express from 'express';
import morgan from 'morgan';
import { nanoid } from 'nanoid';
import { createChannel } from './amqp.js';
import { ROUTING_KEYS } from '../common/events.js';
import { PrismaClient } from '@prisma/client';

const app = express();
app.use(express.json());
app.use(morgan('dev'));

const PORT = process.env.PORT || 3002;
const USERS_BASE_URL = process.env.USERS_BASE_URL || 'http://localhost:3001';
const HTTP_TIMEOUT_MS = Number(process.env.HTTP_TIMEOUT_MS || 2000);
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';
const EXCHANGE = process.env.EXCHANGE || 'app.topic';
const QUEUE = process.env.QUEUE || 'orders.q';
const ROUTING_KEY_USER_CREATED = process.env.ROUTING_KEY_USER_CREATED || ROUTING_KEYS.USER_CREATED;
const ROUTING_KEY_USER_UPDATED = process.env.ROUTING_KEY_USER_UPDATED || ROUTING_KEYS.USER_UPDATED;

const prisma = new PrismaClient();

// Cache de usuários populado por eventos
const userCache = new Map();

let amqp = null;
(async () => {
  try {
    amqp = await createChannel(RABBITMQ_URL, EXCHANGE);
    console.log('[orders] AMQP connected');

    await amqp.ch.assertQueue(QUEUE, { durable: true });
    await amqp.ch.bindQueue(QUEUE, EXCHANGE, ROUTING_KEY_USER_CREATED);
    await amqp.ch.bindQueue(QUEUE, EXCHANGE, ROUTING_KEY_USER_UPDATED);

    amqp.ch.consume(QUEUE, msg => {
      if (!msg) return;
      try {
        const payload = JSON.parse(msg.content.toString());
        const key = msg.fields.routingKey;
        if (key === ROUTING_KEYS.USER_CREATED || key === ROUTING_KEYS.USER_UPDATED) {
          userCache.set(payload.id, payload);
          console.log('[orders] consumed event', key, '-> cached', payload.id);
        }
        amqp.ch.ack(msg);
      } catch (err) {
        console.error('[orders] consume error:', err.message);
        amqp.ch.nack(msg, false, false);
      }
    });
  } catch (err) {
    console.error('[orders] AMQP connection failed:', err.message);
  }
})();

// Health
app.get('/health', (req, res) => res.json({ ok: true, service: 'orders' }));

// Listar pedidos
app.get('/', async (req, res) => {
  const orders = await prisma.order.findMany();
  res.json(orders.map(o => ({ ...o, items: JSON.parse(o.items) })));
});

// Função fetch com timeout
async function fetchWithTimeout(url, ms) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), ms);
  try {
    return await fetch(url, { signal: controller.signal });
  } finally {
    clearTimeout(id);
  }
}

// Criar pedido
app.post('/', async (req, res) => {
  const { userId, items, total } = req.body || {};
  if (!userId || !Array.isArray(items) || typeof total !== 'number') {
    return res.status(400).json({ error: 'userId, items[], total<number> são obrigatórios' });
  }

  try {
    const resp = await fetchWithTimeout(`${USERS_BASE_URL}/${userId}`, HTTP_TIMEOUT_MS);
    if (!resp.ok) return res.status(400).json({ error: 'usuário inválido' });
  } catch (err) {
    if (!userCache.has(userId)) return res.status(503).json({ error: 'users-service indisponível e usuário não encontrado no cache' });
  }

  try {
    const order = await prisma.order.create({
      data: {
        userId,
        items: JSON.stringify(items),
        total,
        status: 'created'
      }
    });

    if (amqp?.ch) {
      amqp.ch.publish(EXCHANGE, ROUTING_KEYS.ORDER_CREATED, Buffer.from(JSON.stringify({ ...order, items })), { persistent: true });
      console.log('[orders] published event:', ROUTING_KEYS.ORDER_CREATED, order.id);
    }

    res.status(201).json({ ...order, items });
  } catch (err) {
    console.error('[orders] create order error:', err.message);
    res.status(500).json({ error: 'Erro interno' });
  }
});

// Cancelar pedido
app.delete('/:id', async (req, res) => {
  const id = req.params.id;

  try {
    const existing = await prisma.order.findUnique({ where: { id } });
    if (!existing) return res.status(404).json({ error: 'not found' });

    const cancelledOrder = await prisma.order.update({
      where: { id },
      data: { status: 'cancelled', cancelledAt: new Date() }
    });

    if (amqp?.ch) {
      amqp.ch.publish(EXCHANGE, ROUTING_KEYS.ORDER_CANCELLED, Buffer.from(JSON.stringify({ ...cancelledOrder, items: JSON.parse(cancelledOrder.items) })), { persistent: true });
      console.log('[orders] published event:', ROUTING_KEYS.ORDER_CANCELLED, id);
    }

    res.json({ ...cancelledOrder, items: JSON.parse(cancelledOrder.items) });
  } catch (err) {
    console.error('[orders] cancel order error:', err.message);
    res.status(500).json({ error: 'Erro interno' });
  }
});

app.listen(PORT, () => console.log(`[orders] listening on http://localhost:${PORT}`));
