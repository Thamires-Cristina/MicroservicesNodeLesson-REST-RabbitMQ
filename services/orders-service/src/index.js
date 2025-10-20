import express from 'express';
import morgan from 'morgan';
import { nanoid } from 'nanoid';
import { createChannel } from './amqp.js';
import { ROUTING_KEYS } from '../common/events.js';

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

 // In-memory "DB"
const orders = new Map();
// In-memory cache de usuários (preenchido por eventos)
const userCache = new Map();

let amqp = null;
(async () => {
  try {
    amqp = await createChannel(RABBITMQ_URL, EXCHANGE);
    console.log('[orders] AMQP connected');

    // Bind de fila para consumir eventos user.created e user.updated
    await amqp.ch.assertQueue(QUEUE, { durable: true });
    await amqp.ch.bindQueue(QUEUE, EXCHANGE, ROUTING_KEY_USER_CREATED);
    await amqp.ch.bindQueue(QUEUE, EXCHANGE, ROUTING_KEY_USER_UPDATED);

    amqp.ch.consume(QUEUE, msg => {
      if (!msg) return;
      try {
        const payload = JSON.parse(msg.content.toString());
        const key = msg.fields.routingKey;
        if (key === ROUTING_KEYS.USER_CREATED || key === ROUTING_KEYS.USER_UPDATED) {
          // idempotência simples: atualiza/define
          userCache.set(payload.id, payload);
          console.log('[orders] consumed event', key, '-> cached', payload.id);
        }
        amqp.ch.ack(msg);
      } catch (err) {
        console.error('[orders] consume error:', err.message);
        amqp.ch.nack(msg, false, false); // descarta em caso de erro de parsing
      }
    });
  } catch (err) {
    console.error('[orders] AMQP connection failed:', err.message);
  }
})();

app.get('/health', (req, res) => res.json({ ok: true, service: 'orders' }));

app.get('/', (req, res) => {
  res.json(Array.from(orders.values()));
});

async function fetchWithTimeout(url, ms) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), ms);
  try {
    const res = await fetch(url, { signal: controller.signal });
    return res;
  } finally {
    clearTimeout(id);
  }
}

app.post('/', async (req, res) => {
  const { userId, items, total } = req.body || {};
  if (!userId || !Array.isArray(items) || typeof total !== 'number') {
    return res.status(400).json({ error: 'userId, items[], total<number> são obrigatórios' });
  }

  // 1) Validação síncrona (HTTP) no Users Service
  try {
    const resp = await fetchWithTimeout(`${USERS_BASE_URL}/${userId}`, HTTP_TIMEOUT_MS);
    if (!resp.ok) return res.status(400).json({ error: 'usuário inválido' });
  } catch (err) {
    console.warn('[orders] users-service timeout/failure, tentando cache...', err.message);
    // fallback: usar cache populado por eventos (assíncrono)
    if (!userCache.has(userId)) {
      return res.status(503).json({ error: 'users-service indisponível e usuário não encontrado no cache' });
    }
  }

  const id = `o_${nanoid(6)}`;
  const order = { id, userId, items, total, status: 'created', createdAt: new Date().toISOString() };
  orders.set(id, order);

  // publicar evento order.created
  try {
    if (amqp?.ch) {
      amqp.ch.publish(EXCHANGE, ROUTING_KEYS.ORDER_CREATED, Buffer.from(JSON.stringify(order)), { persistent: true });
      console.log('[orders] published event:', ROUTING_KEYS.ORDER_CREATED, order.id);
    }
  } catch (err) {
    console.error('[orders] publish error:', err.message);
  }

  res.status(201).json(order);
});

// NEW: cancelar pedido -> publicar order.cancelled
app.delete('/:id', (req, res) => {
  const id = req.params.id;
  const existing = orders.get(id);
  if (!existing) return res.status(404).json({ error: 'not found' });

  existing.status = 'cancelled';
  existing.cancelledAt = new Date().toISOString();
  orders.set(id, existing);

  // Publish event order.cancelled
  try {
    if (amqp?.ch) {
      amqp.ch.publish(EXCHANGE, ROUTING_KEYS.ORDER_CANCELLED, Buffer.from(JSON.stringify(existing)), { persistent: true });
      console.log('[orders] published event:', ROUTING_KEYS.ORDER_CANCELLED, id);
    }
  } catch (err) {
    console.error('[orders] publish error:', err.message);
  }

  res.json(existing);
});

app.listen(PORT, () => {
  console.log(`[orders] listening on http://localhost:${PORT}`);
  console.log(`[orders] users base url: ${USERS_BASE_URL}`);
});
