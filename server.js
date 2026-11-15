require('dotenv').config();
const express = require('express');
const path = require('path');
const WebSocket = require('ws');
const bcrypt = require('bcryptjs');
const pool = require('./db');

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'pages')));

const DEFAULT_ASSETS = [
  { symbol: 'BTC', name: 'Bitcoin', asset_type: 'CRYPTO' },
  { symbol: 'ETH', name: 'Ethereum', asset_type: 'CRYPTO' },
  { symbol: 'AAPL', name: 'Apple Inc.', asset_type: 'STOCK' },
  { symbol: 'GOOGL', name: 'Alphabet Inc.', asset_type: 'STOCK' },
  { symbol: 'MSFT', name: 'Microsoft Corp.', asset_type: 'STOCK' },
  { symbol: 'TSLA', name: 'Tesla Inc.', asset_type: 'STOCK' },
  { symbol: 'NVDA', name: 'NVIDIA Corp.', asset_type: 'STOCK' },
  { symbol: 'META', name: 'Meta Platforms Inc.', asset_type: 'STOCK' }
];

// In-memory state
const liveState = {};       // symbol -> { asset_id, symbol, name, asset_type, finnhub, price, prevClose, changePct, risk }
const finnhubToSymbol = {}; // 'BINANCE:BTCUSDT' -> 'BTC'
const betaCache = {};       // symbol -> 'LOW' | 'MEDIUM' | 'HIGH'
const candleCache = {};     // 'BTC:1D' -> { data, expiresAt }
const sseClients = new Set();

function toFinnhubSymbol(symbol, assetType) {
  return assetType === 'CRYPTO' ? `BINANCE:${symbol.toUpperCase()}USDT` : symbol.toUpperCase();
}

// Safely broadcast live Finnhub ticks to all connected SSE clients
function broadcastLiveTick(payload) {
  const data = `data: ${JSON.stringify(payload)}\n\n`;
  for (const client of sseClients) {
    if (client.writableEnded) {
      sseClients.delete(client);
      continue;
    }
    try {
      client.write(data);
    } catch (err) {
      sseClients.delete(client);
    }
  }
}

// Seed default assets only if the assets table is empty
async function seedDefaultAssetsIfEmpty() {
  const [[{ count }]] = await pool.query('SELECT COUNT(*) AS count FROM assets');
  if (Number(count) > 0) return;

  for (const item of DEFAULT_ASSETS) {
    const defaultRisk = item.asset_type === 'CRYPTO' ? 'HIGH' : 'MEDIUM';
    await pool.query(
      `INSERT INTO assets (symbol, name, asset_type, current_price, price_change, risk, is_active)
       VALUES (?, ?, ?, 0, 0, ?, TRUE)`,
      [item.symbol, item.name, item.asset_type, defaultRisk]
    );
  }
  console.log('Seeded initial 8 default assets.');
}

async function getAssetRisk(symbol, finnhubSymbol, assetType, apiKey) {
  if (assetType === 'CRYPTO') return 'HIGH';
  if (betaCache[symbol] !== undefined) return betaCache[symbol];

  try {
    const url = `https://finnhub.io/api/v1/stock/metric?symbol=${encodeURIComponent(finnhubSymbol)}&metric=all&token=${apiKey}`;
    const res = await fetch(url);
    const data = await res.json();
    const beta = data?.metric?.beta;

    let risk = 'MEDIUM';
    if (typeof beta === 'number') {
      if (beta < 1.1) risk = 'LOW';
      else if (beta > 1.5) risk = 'HIGH';
      else risk = 'MEDIUM';
    }

    betaCache[symbol] = risk;
    return risk;
  } catch (err) {
    console.error(`Failed to fetch beta for ${symbol}:`, err.message);
    return 'MEDIUM';
  }
}

// Fetch initial REST quote & beta for a single asset
async function bootstrapAssetFromRest(row, apiKey) {
  const finnhubSym = toFinnhubSymbol(row.symbol, row.asset_type);
  finnhubToSymbol[finnhubSym] = row.symbol;

  try {
    const url = `https://finnhub.io/api/v1/quote?symbol=${encodeURIComponent(finnhubSym)}&token=${apiKey}`;
    const res = await fetch(url);
    const quote = await res.json();
    const risk = await getAssetRisk(row.symbol, finnhubSym, row.asset_type, apiKey);

    const hasValidQuote = quote && typeof quote.c === 'number' && quote.c > 0;
    const price = hasValidQuote ? quote.c : Number(row.current_price || 0);
    const prevClose = hasValidQuote && typeof quote.pc === 'number' && quote.pc > 0 ? quote.pc : price;
    const changePct = hasValidQuote && typeof quote.dp === 'number' ? quote.dp : Number(row.price_change || 0);

    liveState[row.symbol] = {
      asset_id: row.asset_id,
      symbol: row.symbol,
      name: row.name,
      asset_type: row.asset_type,
      finnhub: finnhubSym,
      price,
      prevClose,
      changePct,
      risk
    };
  } catch (err) {
    console.error(`REST bootstrap failed for ${row.symbol}:`, err.message);
  }
}

// Finnhub WebSocket Manager
let finnhubWs = null;
const subscribedSymbols = new Set();

function subscribeWsSymbol(finnhubSym) {
  if (subscribedSymbols.has(finnhubSym)) return;
  subscribedSymbols.add(finnhubSym);
  if (finnhubWs && finnhubWs.readyState === WebSocket.OPEN) {
    finnhubWs.send(JSON.stringify({ type: 'subscribe', symbol: finnhubSym }));
  }
}

function connectFinnhubWebSocket(apiKey) {
  finnhubWs = new WebSocket(`wss://ws.finnhub.io?token=${apiKey}`);

  finnhubWs.on('open', () => {
    console.log('Connected to Finnhub Live WebSocket.');
    for (const sym of subscribedSymbols) {
      finnhubWs.send(JSON.stringify({ type: 'subscribe', symbol: sym }));
    }
  });

  finnhubWs.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw);
      if (msg.type !== 'trade' || !Array.isArray(msg.data)) return;

      for (const trade of msg.data) {
        const symbol = finnhubToSymbol[trade.s];
        if (!symbol || !liveState[symbol]) continue;

        const state = liveState[symbol];
        state.price = Number(trade.p);
        if (state.prevClose > 0) {
          state.changePct = ((state.price - state.prevClose) / state.prevClose) * 100;
        }

        broadcastLiveTick({
          asset_id: state.asset_id,
          symbol: state.symbol,
          current_price: state.price,
          price_change: Number(state.changePct.toFixed(4))
        });
      }
    } catch (err) {
      // Ignore non-JSON ping frames
    }
  });

  finnhubWs.on('close', () => {
    console.warn('Finnhub WebSocket closed. Reconnecting in 5s...');
    setTimeout(() => connectFinnhubWebSocket(apiKey), 5000);
  });

  finnhubWs.on('error', (err) => {
    console.error('Finnhub WebSocket error:', err.message);
  });
}

// Save exact-instant snapshots to MySQL every 30s & subscribe to any newly added DB assets
async function syncSnapshotsToDb(apiKey) {
  try {
    const [activeRows] = await pool.query(
      'SELECT asset_id, symbol, name, asset_type, current_price, price_change, risk FROM assets WHERE is_active = 1'
    );

    for (const row of activeRows) {
      if (!liveState[row.symbol]) {
        await bootstrapAssetFromRest(row, apiKey);
        subscribeWsSymbol(toFinnhubSymbol(row.symbol, row.asset_type));
      }
    }

    let syncedCount = 0;
    for (const row of activeRows) {
      const state = liveState[row.symbol];
      if (!state || state.price <= 0) continue;

      await pool.query(
        `UPDATE assets
         SET current_price = ?, price_change = ?, risk = ?, updated_at = CURRENT_TIMESTAMP
         WHERE asset_id = ?`,
        [state.price, state.changePct, state.risk, row.asset_id]
      );

      await pool.query(
        'INSERT INTO asset_price_history (asset_id, price) VALUES (?, ?)',
        [row.asset_id, state.price]
      );
      syncedCount++;
    }

    console.log(`[${new Date().toLocaleTimeString()}] Saved ${syncedCount} live snapshots to MySQL.`);
  } catch (err) {
    console.error('Snapshot DB sync error:', err.message);
  }
}

// Redirect legacy auth pages to unified auth.html
app.get('/login.html', (req, res) => {
  res.redirect('/auth.html?mode=login');
});
app.get('/register.html', (req, res) => {
  res.redirect('/auth.html?mode=register');
});

// POST /api/auth/register — Atomic ACID signup (users + wallets + wallet_history)
app.post('/api/auth/register', async (req, res) => {
  const {
    username,
    first_name,
    middle_name,
    last_name,
    email,
    password
  } = req.body || {};

  const cleanUser = (username || '').trim();
  const cleanFirst = (first_name || '').trim();
  const cleanMiddle = (middle_name || '').trim() || null;
  const cleanLast = (last_name || '').trim();
  const cleanEmail = (email || '').trim().toLowerCase();

  if (!cleanUser || !cleanFirst || !cleanLast || !cleanEmail || !password) {
    return res.status(400).json({ error: 'All required fields must be filled.' });
  }
  if (password.length < 8) {
    return res.status(400).json({ error: 'Password must be at least 8 characters.' });
  }

  let conn;
  try {
    const passwordHash = await bcrypt.hash(password, 10);
    conn = await pool.getConnection();
    await conn.beginTransaction();

    const [userResult] = await conn.execute(
      `INSERT INTO users (username, first_name, middle_name, last_name, email, password_hash, role, account_status)
       VALUES (?, ?, ?, ?, ?, ?, 'USER', 'ACTIVE')`,
      [cleanUser, cleanFirst, cleanMiddle, cleanLast, cleanEmail, passwordHash]
    );
    const userId = userResult.insertId;

    const initialBalance = 10000.00;
    const [walletResult] = await conn.execute(
      `INSERT INTO wallets (user_id, currency, balance)
       VALUES (?, 'INR', ?)`,
      [userId, initialBalance]
    );
    const walletId = walletResult.insertId;

    await conn.execute(
      `INSERT INTO wallet_history (wallet_id, transaction_type, transacted_amount, balance_after, description)
       VALUES (?, 'DEPOSIT', ?, ?, 'Welcome bonus')`,
      [walletId, initialBalance, initialBalance]
    );

    await conn.commit();

    const fullName = [cleanFirst, cleanMiddle, cleanLast].filter(Boolean).join(' ');
    return res.status(201).json({
      user: {
        user_id: userId,
        username: cleanUser,
        first_name: cleanFirst,
        middle_name: cleanMiddle,
        last_name: cleanLast,
        full_name: fullName,
        email: cleanEmail,
        role: 'user',
        wallet_id: walletId,
        balance: initialBalance
      }
    });
  } catch (err) {
    if (conn) await conn.rollback();
    if (err.code === 'ER_DUP_ENTRY') {
      return res.status(409).json({ error: 'Username or email is already registered.' });
    }
    console.error('Register error:', err);
    return res.status(500).json({ error: 'Registration failed. Please try again.' });
  } finally {
    if (conn) conn.release();
  }
});

// POST /api/auth/login — Authenticate by username or email
app.post('/api/auth/login', async (req, res) => {
  const { identifier, password } = req.body || {};
  const cleanId = (identifier || '').trim();

  if (!cleanId || !password) {
    return res.status(400).json({ error: 'Enter your username/email and password.' });
  }

  try {
    const [rows] = await pool.execute(
      `SELECT u.user_id, u.username, u.first_name, u.middle_name, u.last_name,
              u.email, u.password_hash, u.role, u.account_status,
              w.wallet_id, w.balance
       FROM users u
       LEFT JOIN wallets w ON u.user_id = w.user_id
       WHERE u.username = ? OR u.email = ?
       LIMIT 1`,
      [cleanId, cleanId.toLowerCase()]
    );

    if (rows.length === 0) {
      return res.status(401).json({ error: 'Invalid credentials.' });
    }

    const row = rows[0];
    if (row.account_status !== 'ACTIVE') {
      return res.status(403).json({ error: `Account is ${row.account_status.toLowerCase()}. Contact an admin.` });
    }

    const valid = await bcrypt.compare(password, row.password_hash);
    if (!valid) {
      return res.status(401).json({ error: 'Invalid credentials.' });
    }

    const fullName = [row.first_name, row.middle_name, row.last_name].filter(Boolean).join(' ');
    return res.json({
      user: {
        user_id: row.user_id,
        username: row.username,
        first_name: row.first_name,
        middle_name: row.middle_name,
        last_name: row.last_name,
        full_name: fullName,
        email: row.email,
        role: row.role.toLowerCase(),
        wallet_id: row.wallet_id,
        balance: Number(row.balance || 0)
      }
    });
  } catch (err) {
    console.error('Login error:', err);
    return res.status(500).json({ error: 'Login failed. Please try again.' });
  }
});

// API: Server-Sent Events stream for real-time WebSocket ticks on frontend charts & tables
app.get('/api/stream', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  sseClients.add(res);

  const heartbeat = setInterval(() => {
    if (!res.writableEnded) {
      res.write(': ping\n\n');
    }
  }, 25000);

  req.on('close', () => {
    clearInterval(heartbeat);
    sseClients.delete(res);
    if (!res.writableEnded) {
      res.end();
    }
  });
});

// API: Get all active assets (blends live in-memory WebSocket prices with MySQL metadata)
app.get('/api/assets', async (req, res) => {
  try {
    const [rows] = await pool.query(
      'SELECT asset_id, symbol, name, asset_type, current_price, price_change, risk FROM assets WHERE is_active = 1 ORDER BY asset_id ASC'
    );

    const enriched = rows.map((r) => {
      const live = liveState[r.symbol];
      return {
        ...r,
        current_price: live && live.price > 0 ? live.price : Number(r.current_price),
        price_change: live ? Number(live.changePct.toFixed(4)) : Number(r.price_change),
        risk: live ? live.risk : r.risk
      };
    });

    res.json(enriched);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// API: Get 1D BTC chart data from Binance (cached 2m) + live right-edge price so index.html needs no changes
app.get('/api/hero-history', async (req, res) => {
  try {
    const cacheKey = 'BTC:1D';
    const now = Date.now();
    let labels = [];
    let prices = [];

    if (candleCache[cacheKey] && candleCache[cacheKey].expiresAt > now) {
      labels = [...candleCache[cacheKey].data.labels];
      prices = [...candleCache[cacheKey].data.prices];
    } else {
      const url = 'https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=15m&limit=60';
      const r = await fetch(url);
      const klines = await r.json();

      if (Array.isArray(klines) && klines.length > 0) {
        labels = klines.map((k) =>
          new Date(k[0]).toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit' })
        );
        prices = klines.map((k) => Number(k[4]));

        candleCache[cacheKey] = {
          data: { symbol: 'BTC', range: '1D', labels: [...labels], prices: [...prices] },
          expiresAt: now + 2 * 60 * 1000
        };
      }
    }

    // Fallback to MySQL recorded ticks if Binance is unreachable
    if (prices.length === 0) {
      const [rows] = await pool.query(
        `SELECT h.price, h.timestamp
         FROM asset_price_history h
         JOIN assets a ON h.asset_id = a.asset_id
         WHERE a.symbol = 'BTC'
         ORDER BY h.timestamp DESC
         LIMIT 60`
      );
      const ordered = rows.reverse();
      labels = ordered.map((r) =>
        new Date(r.timestamp).toLocaleTimeString('en-IN', {
          hour: '2-digit',
          minute: '2-digit',
          second: '2-digit'
        })
      );
      prices = ordered.map((r) => Number(r.price));
    }

    // Append current live Finnhub price as the rightmost tip for /api/stream updates
    if (liveState.BTC && liveState.BTC.price > 0) {
      labels.push(new Date().toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit' }));
      prices.push(liveState.BTC.price);
    }

    res.json({ labels, prices });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// API: Get cached historical candles (1D, 1W, 1M, 1Y) from Binance (Crypto) or Yahoo Finance (Stocks)
app.get('/api/assets/:id/history', async (req, res) => {
  const range = (req.query.range || '1D').toUpperCase();
  const validRanges = ['1D', '1W', '1M', '1Y'];
  const selectedRange = validRanges.includes(range) ? range : '1D';

  try {
    const [rows] = await pool.query(
      'SELECT asset_id, symbol, asset_type FROM assets WHERE asset_id = ?',
      [req.params.id]
    );
    if (rows.length === 0) return res.status(404).json({ error: 'Asset not found' });

    const asset = rows[0];
    const cacheKey = `${asset.symbol}:${selectedRange}`;
    const now = Date.now();

    if (candleCache[cacheKey] && candleCache[cacheKey].expiresAt > now) {
      return res.json(candleCache[cacheKey].data);
    }

    let labels = [];
    let prices = [];

    if (asset.asset_type === 'CRYPTO') {
      const map = {
        '1D': { interval: '15m', limit: 96 },
        '1W': { interval: '1h', limit: 168 },
        '1M': { interval: '4h', limit: 180 },
        '1Y': { interval: '1d', limit: 365 }
      };
      const cfg = map[selectedRange];
      const url = `https://api.binance.com/api/v3/klines?symbol=${encodeURIComponent(asset.symbol)}USDT&interval=${cfg.interval}&limit=${cfg.limit}`;
      const r = await fetch(url);
      const klines = await r.json();

      if (Array.isArray(klines)) {
        labels = klines.map((k) => {
          const d = new Date(k[0]);
          return selectedRange === '1D'
            ? d.toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit' })
            : d.toLocaleDateString('en-IN', { day: '2-digit', month: 'short' });
        });
        prices = klines.map((k) => Number(k[4]));
      }
    } else {
      const map = {
        '1D': { range: '1d', interval: '5m' },
        '1W': { range: '5d', interval: '1h' },
        '1M': { range: '1mo', interval: '1d' },
        '1Y': { range: '1y', interval: '1d' }
      };
      const cfg = map[selectedRange];
      const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(asset.symbol)}?range=${cfg.range}&interval=${cfg.interval}`;
      const r = await fetch(url, {
        headers: { 'User-Agent': 'Mozilla/5.0' }
      });
      const json = await r.json();
      const result = json?.chart?.result?.[0];

      if (result && Array.isArray(result.timestamp)) {
        const closes = result.indicators?.quote?.[0]?.close || [];
        result.timestamp.forEach((ts, idx) => {
          if (typeof closes[idx] === 'number') {
            const d = new Date(ts * 1000);
            labels.push(
              selectedRange === '1D'
                ? d.toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit' })
                : d.toLocaleDateString('en-IN', { day: '2-digit', month: 'short' })
            );
            prices.push(Number(closes[idx].toFixed(2)));
          }
        });
      }
    }

    if (prices.length === 0) {
      const [dbRows] = await pool.query(
        'SELECT price, timestamp FROM asset_price_history WHERE asset_id = ? ORDER BY timestamp DESC LIMIT 100',
        [asset.asset_id]
      );
      const ordered = dbRows.reverse();
      labels = ordered.map((d) => new Date(d.timestamp).toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit' }));
      prices = ordered.map((d) => Number(d.price));
    }

    const payload = { symbol: asset.symbol, range: selectedRange, labels, prices };
    const ttlMs = selectedRange === '1D' ? 2 * 60 * 1000 : 15 * 60 * 1000;
    candleCache[cacheKey] = { data: payload, expiresAt: now + ttlMs };

    res.json(payload);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// API: Get live platform statistics for landing page section-band
app.get('/api/stats', async (req, res) => {
  try {
    const [[{ traders }]] = await pool.query('SELECT COUNT(*) AS traders FROM users');
    const [[{ assets }]] = await pool.query('SELECT COUNT(*) AS assets FROM assets WHERE is_active = 1');
    const [[{ trades, volume }]] = await pool.query(
      'SELECT COUNT(*) AS trades, COALESCE(SUM(total_amount), 0) AS volume FROM trade'
    );

    res.json({
      traders: Number(traders),
      assets: Number(assets),
      trades: Number(trades),
      volume: Number(volume)
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, async () => {
  console.log(`BATUA-Edu running on http://localhost:${PORT}`);

  const apiKey = process.env.FINNHUB_API_KEY;
  if (!apiKey) {
    console.error('Missing FINNHUB_API_KEY in .env');
    return;
  }

  await seedDefaultAssetsIfEmpty();

  const [activeAssets] = await pool.query(
    'SELECT asset_id, symbol, name, asset_type, current_price, price_change, risk FROM assets WHERE is_active = 1'
  );

  for (const row of activeAssets) {
    await bootstrapAssetFromRest(row, apiKey);
    subscribeWsSymbol(toFinnhubSymbol(row.symbol, row.asset_type));
  }

  await syncSnapshotsToDb(apiKey);
  connectFinnhubWebSocket(apiKey);

  setInterval(() => syncSnapshotsToDb(apiKey), 30000);
});