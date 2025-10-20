import express from 'express';
import morgan from 'morgan';
import { nanoid } from 'nanoid';
import { createChannel } from './amqp.js';
import { ROUTING_KEYS } from '../common/events.js';

const app = express();
app.use(express.json());
app.use(morgan('dev'));

const PORT = process.env.PORT || 3001;
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';
const EXCHANGE = process.env.EXCHANGE || 'app.topic';

// In-memory "DB"
const users = new Map();

let amqp = null;
(async () => {
  try {
    amqp = await createChannel(RABBITMQ_URL, EXCHANGE);
    console.log('[users] AMQP connected');
  } catch (err) {
    console.error('[users] AMQP connection failed:', err.message);
  }
})();

app.get('/health', (req, res) => res.json({ ok: true, service: 'users' }));

app.get('/', (req, res) => {
  res.json(Array.from(users.values()));
});

app.post('/', async (req, res) => {
  const { name, email } = req.body || {};
  if (!name || !email) return res.status(400).json({ error: 'name and email are required' });

  const id = `u_${nanoid(6)}`;
  const user = { id, name, email, createdAt: new Date().toISOString(), updatedAt: null };
  users.set(id, user);

  // Publish event user.created
  try {
    if (amqp?.ch) {
      const payload = Buffer.from(JSON.stringify(user));
      amqp.ch.publish(EXCHANGE, ROUTING_KEYS.USER_CREATED, payload, { persistent: true });
      console.log('[users] published event:', ROUTING_KEYS.USER_CREATED, user.id);
    }
  } catch (err) {
    console.error('[users] publish error:', err.message);
  }

  res.status(201).json(user);
});

// NEW: update user -> publish user.updated
app.put('/:id', async (req, res) => {
  const id = req.params.id;
  const existing = users.get(id);
  if (!existing) return res.status(404).json({ error: 'not found' });

  const { name, email } = req.body || {};
  if (!name && !email) return res.status(400).json({ error: 'name or email required to update' });

  const updated = { ...existing, name: name ?? existing.name, email: email ?? existing.email, updatedAt: new Date().toISOString() };
  users.set(id, updated);

  // Publish event user.updated
  try {
    if (amqp?.ch) {
      const payload = Buffer.from(JSON.stringify(updated));
      amqp.ch.publish(EXCHANGE, ROUTING_KEYS.USER_UPDATED, payload, { persistent: true });
      console.log('[users] published event:', ROUTING_KEYS.USER_UPDATED, id);
    }
  } catch (err) {
    console.error('[users] publish error:', err.message);
  }

  res.json(updated);
});

app.get('/:id', (req, res) => {
  const user = users.get(req.params.id);
  if (!user) return res.status(404).json({ error: 'not found' });
  res.json(user);
});

app.listen(PORT, () => {
  console.log(`[users] listening on http://localhost:${PORT}`);
});
