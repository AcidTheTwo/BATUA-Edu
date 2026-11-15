require('dotenv').config();
const express = require('express');
const path = require('path');
const WebSocket = require('ws');
const bcrypt = require('bcryptjs');
const pool = require('./db');

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'pages')));

// In-memory state populated dynamically from the MySQL `assets` table
const liveState = {};       // symbol -> { asset_id, symbol, name, asset_type, finnhub, price, prevClose, changePct, risk }
const finnhubToSymbol = {}; // 'BINANCE:BTCUSDT' -> 'BTC'
const betaCache = {};       // symbol -> 'LOW' | 'MEDIUM' | 'HIGH'
const candleCache = {};     // 'BTC:1D' -> { data, expiresAt }
const sseClients = new Set();

function toFinnhubSymbol(symbol, assetType) {
  const s = symbol.toUpperCase();
  if (assetType === 'CRYPTO') {
    return s === 'USDT' ? 'BINANCE:USDCUSDT' : `BINANCE:${s}USDT`;
  }
  return s;
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

async function getAssetRisk(symbol, finnhubSymbol, assetType, apiKey) {
  if (symbol.toUpperCase() === 'USDT') return 'LOW';
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

// Fetch initial REST quote & beta for a DB asset (uses Yahoo Finance fallback for .NS stocks)
async function bootstrapAssetFromRest(row, apiKey) {
  const finnhubSym = toFinnhubSymbol(row.symbol, row.asset_type);
  finnhubToSymbol[finnhubSym] = row.symbol;

  try {
    if (row.symbol.toUpperCase().endsWith('.NS')) {
      const yUrl = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(row.symbol)}?range=1d&interval=5m`;
      const yRes = await fetch(yUrl, { headers: { 'User-Agent': 'Mozilla/5.0' } });
      const yJson = await yRes.json();
      const meta = yJson?.chart?.result?.[0]?.meta;

      const price = typeof meta?.regularMarketPrice === 'number' ? meta.regularMarketPrice : Number(row.current_price || 0);
      const prevClose = typeof meta?.chartPreviousClose === 'number' && meta.chartPreviousClose > 0 ? meta.chartPreviousClose : price;
      const changePct = prevClose > 0 ? ((price - prevClose) / prevClose) * 100 : Number(row.price_change || 0);

      liveState[row.symbol] = {
        asset_id: row.asset_id,
        symbol: row.symbol,
        name: row.name,
        asset_type: row.asset_type,
        finnhub: finnhubSym,
        price,
        prevClose,
        changePct,
        risk: row.risk || 'MEDIUM'
      };
      return;
    }

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

    // Pick up any newly inserted rows in MySQL `assets` table automatically
    for (const row of activeRows) {
      if (!liveState[row.symbol]) {
        await bootstrapAssetFromRest(row, apiKey);
        if (!row.symbol.toUpperCase().endsWith('.NS')) {
          subscribeWsSymbol(toFinnhubSymbol(row.symbol, row.asset_type));
        }
      }
    }

    let syncedCount = 0;
    for (const row of activeRows) {
      const state = liveState[row.symbol];
      if (!state || state.price <= 0) continue;

      // Refresh .NS stocks via Yahoo Finance during sync since Finnhub free WS doesn't stream NSE
      if (row.symbol.toUpperCase().endsWith('.NS')) {
        await bootstrapAssetFromRest(row, apiKey);
      }

      await pool.query(
        `UPDATE assets
         SET current_price = ?, price_change = ?, risk = ?
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

// API: Get all active assets from MySQL blended with live in-memory WebSocket prices
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

// API: Lookup or add a non-featured ticker on demand
app.post('/api/assets/lookup', async (req, res) => {
  const rawSymbol = (req.body?.symbol || '').trim().toUpperCase();
  const assetType = req.body?.asset_type === 'CRYPTO' ? 'CRYPTO' : 'STOCK';
  if (!rawSymbol || !/^[A-Z0-9.-]{1,15}$/.test(rawSymbol)) {
    return res.status(400).json({ error: 'Enter a valid ticker symbol (e.g. SOL, AMZN, NFLX).' });
  }

  try {
    const [existing] = await pool.query(
      'SELECT asset_id, symbol, name, asset_type, current_price, price_change, risk, is_active FROM assets WHERE symbol = ? LIMIT 1',
      [rawSymbol]
    );
    if (existing.length > 0) {
      const row = existing[0];
      if (!row.is_active) {
        await pool.query('UPDATE assets SET is_active = 1 WHERE asset_id = ?', [row.asset_id]);
      }
      const live = liveState[row.symbol];
      return res.json({
        asset: {
          ...row,
          current_price: live && live.price > 0 ? live.price : Number(row.current_price),
          price_change: live ? Number(live.changePct.toFixed(4)) : Number(row.price_change),
          risk: live ? live.risk : row.risk
        }
      });
    }

    const apiKey = process.env.FINNHUB_API_KEY;
    const finnhubSym = toFinnhubSymbol(rawSymbol, assetType);

    if (rawSymbol.endsWith('.NS')) {
      const yUrl = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(rawSymbol)}?range=1d&interval=5m`;
      const yRes = await fetch(yUrl, { headers: { 'User-Agent': 'Mozilla/5.0' } });
      const yJson = await yRes.json();
      const meta = yJson?.chart?.result?.[0]?.meta;

      if (!meta || typeof meta.regularMarketPrice !== 'number' || meta.regularMarketPrice <= 0) {
        return res.status(404).json({ error: `NSE ticker "${rawSymbol}" not found.` });
      }

      const price = meta.regularMarketPrice;
      const prevClose = typeof meta.chartPreviousClose === 'number' && meta.chartPreviousClose > 0 ? meta.chartPreviousClose : price;
      const changePct = prevClose > 0 ? ((price - prevClose) / prevClose) * 100 : 0;
      const assetName = meta.longName || meta.shortName || rawSymbol;

      const [ins] = await pool.query(
        `INSERT INTO assets (symbol, name, asset_type, current_price, price_change, risk, is_active)
         VALUES (?, ?, 'STOCK', ?, ?, 'MEDIUM', TRUE)`,
        [rawSymbol, assetName, price, changePct]
      );

      const newRow = {
        asset_id: ins.insertId,
        symbol: rawSymbol,
        name: assetName,
        asset_type: 'STOCK',
        current_price: price,
        price_change: changePct,
        risk: 'MEDIUM'
      };
      liveState[rawSymbol] = { ...newRow, finnhub: finnhubSym, price, prevClose, changePct };
      return res.status(201).json({ asset: newRow });
    }

    const quoteRes = await fetch(`https://finnhub.io/api/v1/quote?symbol=${encodeURIComponent(finnhubSym)}&token=${apiKey}`);
    const quote = await quoteRes.json();

    if (!quote || typeof quote.c !== 'number' || quote.c <= 0) {
      return res.status(404).json({ error: `Ticker "${rawSymbol}" (${assetType}) not found on market feed.` });
    }

    let assetName = rawSymbol;
    if (assetType === 'STOCK') {
      try {
        const profRes = await fetch(`https://finnhub.io/api/v1/stock/profile2?symbol=${encodeURIComponent(rawSymbol)}&token=${apiKey}`);
        const prof = await profRes.json();
        if (prof && prof.name) assetName = prof.name;
      } catch (_) {}
    } else {
      assetName = `${rawSymbol} / USDT`;
    }

    const risk = await getAssetRisk(rawSymbol, finnhubSym, assetType, apiKey);
    const changePct = typeof quote.dp === 'number' ? quote.dp : 0;

    const [ins] = await pool.query(
      `INSERT INTO assets (symbol, name, asset_type, current_price, price_change, risk, is_active)
       VALUES (?, ?, ?, ?, ?, ?, TRUE)`,
      [rawSymbol, assetName, assetType, quote.c, changePct, risk]
    );

    const newRow = {
      asset_id: ins.insertId,
      symbol: rawSymbol,
      name: assetName,
      asset_type: assetType,
      current_price: quote.c,
      price_change: changePct,
      risk
    };

    finnhubToSymbol[finnhubSym] = rawSymbol;
    liveState[rawSymbol] = {
      ...newRow,
      finnhub: finnhubSym,
      price: quote.c,
      prevClose: quote.pc || quote.c,
      changePct
    };
    subscribeWsSymbol(finnhubSym);

    res.status(201).json({ asset: newRow });
  } catch (err) {
    console.error('Asset lookup error:', err);
    res.status(500).json({ error: 'Failed to look up ticker.' });
  }
});

// API: Get 1D BTC chart data from Binance (cached 2m) + live right-edge price
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
      const binancePair = asset.symbol.toUpperCase() === 'USDT' ? 'USDCUSDT' : `${asset.symbol.toUpperCase()}USDT`;
      const url = `https://api.binance.com/api/v3/klines?symbol=${encodeURIComponent(binancePair)}&interval=${cfg.interval}&limit=${cfg.limit}`;
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

// API: Get full live dashboard payload for a user
app.get('/api/dashboard/:userId', async (req, res) => {
  const userId = Number(req.params.userId);
  if (!userId) return res.status(400).json({ error: 'Invalid user ID.' });

  try {
    const [[walletRow]] = await pool.query(
      'SELECT wallet_id, balance FROM wallets WHERE user_id = ? LIMIT 1',
      [userId]
    );
    const walletBalance = Number(walletRow?.balance || 0);

    const [holdingRows] = await pool.query(
      `SELECT asset_id, asset_name, symbol, asset_type, total_fraction_owned,
              total_amount_invested, average_purchase_price, current_price
       FROM portfolio_valuation_view
       WHERE user_id = ?
       ORDER BY total_amount_invested DESC`,
      [userId]
    );

    let invested = 0;
    let portfolioValue = 0;

    const holdings = holdingRows.map((h) => {
      const live = liveState[h.symbol];
      const unitPrice = live && live.price > 0 ? live.price : Number(h.current_price);
      const qty = Number(h.total_fraction_owned);
      const cost = Number(h.total_amount_invested);
      const value = qty * unitPrice;
      const pl = value - cost;

      invested += cost;
      portfolioValue += value;

      return {
        asset_id: h.asset_id,
        symbol: h.symbol,
        name: h.asset_name,
        type: h.asset_type,
        qty,
        unit_price: unitPrice,
        invested: cost,
        value,
        pl
      };
    });

    const [assetRows] = await pool.query(
      'SELECT asset_id, symbol, name, asset_type, current_price, price_change FROM assets WHERE is_active = 1'
    );
    const gainers = assetRows
      .map((a) => {
        const live = liveState[a.symbol];
        return {
          id: a.asset_id,
          symbol: a.symbol,
          name: a.name,
          price: live && live.price > 0 ? live.price : Number(a.current_price),
          chg: live ? Number(live.changePct) : Number(a.price_change || 0)
        };
      })
      .sort((a, b) => b.chg - a.chg)
      .slice(0, 4);

    const [tradeRows] = await pool.query(
      `SELECT t.trade_id, t.buyer_user_id, t.seller_user_id,
              t.buyer_asset_fraction, t.seller_asset_fraction,
              t.total_amount, t.timestamp,
              ba.symbol AS buyer_symbol, sa.symbol AS seller_symbol
       FROM trade t
       JOIN assets ba ON t.buyer_asset_id = ba.asset_id
       LEFT JOIN assets sa ON t.seller_asset_id = sa.asset_id
       WHERE (t.buyer_user_id = ? OR t.seller_user_id = ?)
         AND t.trade_status = 'COMPLETED'
       ORDER BY t.timestamp DESC
       LIMIT 5`,
      [userId, userId]
    );

    const recent = tradeRows.map((t) => {
      const isBuy = Number(t.buyer_user_id) === userId;
      return {
        type: isBuy ? 'BUY' : 'SELL',
        sym: isBuy ? t.buyer_symbol : (t.seller_symbol || t.buyer_symbol),
        qty: Number(isBuy ? t.buyer_asset_fraction : (t.seller_asset_fraction || t.buyer_asset_fraction)),
        amt: Number(t.total_amount),
        timestamp: t.timestamp
      };
    });

    const [allUsers] = await pool.query(
      `SELECT u.user_id, COALESCE(w.balance, 0) AS cash
       FROM users u
       LEFT JOIN wallets w ON u.user_id = w.user_id
       WHERE u.account_status = 'ACTIVE'`
    );
    const [allHoldings] = await pool.query(
      `SELECT p.user_id, p.total_fraction_owned, a.symbol, a.current_price
       FROM portfolio p
       JOIN assets a ON p.asset_id = a.asset_id
       WHERE p.total_fraction_owned > 0`
    );
    const [activeLoans] = await pool.query(
      `SELECT borrower_id, COALESCE(SUM(total_repayment - amount_paid), 0) AS debt
       FROM loans
       WHERE loan_status IN ('APPROVED', 'ACTIVE')
       GROUP BY borrower_id`
    );

    const holdingsMap = {};
    for (const row of allHoldings) {
      const live = liveState[row.symbol];
      const px = live && live.price > 0 ? live.price : Number(row.current_price);
      holdingsMap[row.user_id] = (holdingsMap[row.user_id] || 0) + Number(row.total_fraction_owned) * px;
    }

    const debtMap = {};
    for (const row of activeLoans) {
      debtMap[row.borrower_id] = Number(row.debt || 0);
    }

    const standings = allUsers
      .map((u) => ({
        user_id: u.user_id,
        net_worth: Number(u.cash) + (holdingsMap[u.user_id] || 0) - (debtMap[u.user_id] || 0)
      }))
      .sort((a, b) => b.net_worth - a.net_worth);

    const totalTraders = standings.length || 1;
    const myIdx = standings.findIndex((s) => Number(s.user_id) === userId);
    const rank = myIdx >= 0 ? myIdx + 1 : totalTraders;
    const netWorth = myIdx >= 0 ? standings[myIdx].net_worth : (walletBalance + portfolioValue);

    const [lbRows] = await pool.query(
      `SELECT snapshot_date, balance, gain_loss
       FROM user_leaderboard
       WHERE user_id = ?
       ORDER BY snapshot_date DESC
       LIMIT 7`,
      [userId]
    );

    let series = [];
    if (lbRows.length >= 2) {
      series = lbRows.reverse().map((r) => ({
        label: new Date(r.snapshot_date).toLocaleDateString('en-IN', { day: '2-digit', month: 'short' }),
        value: Number(r.balance)
      }));
      series[series.length - 1].value = Number(netWorth.toFixed(2));
    } else {
      for (let i = 6; i >= 0; i--) {
        const d = new Date(Date.now() - i * 86400000);
        series.push({
          label: d.toLocaleDateString('en-IN', { day: '2-digit', month: 'short' }),
          value: i === 0 ? Number(netWorth.toFixed(2)) : 10000.00
        });
      }
    }

    const prevNet = series.length >= 2 ? series[series.length - 2].value : 10000.00;
    const dailyPl = netWorth - prevNet;

    res.json({
      wallet: walletBalance,
      invested,
      portfolio_value: portfolioValue,
      net_worth: netWorth,
      daily_pl: dailyPl,
      rank,
      total_traders: totalTraders,
      holdings,
      gainers,
      recent,
      series
    });
  } catch (err) {
    console.error('Dashboard API error:', err);
    res.status(500).json({ error: 'Failed to load dashboard.' });
  }
});

// API: Get user's wallet balance + existing position in a single asset (for Markets drawer)
app.get('/api/portfolio/:userId/asset/:assetId', async (req, res) => {
  const userId = Number(req.params.userId);
  const assetId = Number(req.params.assetId);
  if (!userId || !assetId) return res.status(400).json({ error: 'Invalid IDs.' });

  try {
    const [[walletRow]] = await pool.query(
      'SELECT balance FROM wallets WHERE user_id = ? LIMIT 1',
      [userId]
    );
    const [posRows] = await pool.query(
      `SELECT p.total_fraction_owned, p.total_amount_invested, p.average_purchase_price,
              a.symbol, a.current_price
       FROM portfolio p
       JOIN assets a ON p.asset_id = a.asset_id
       WHERE p.user_id = ? AND p.asset_id = ?
       LIMIT 1`,
      [userId, assetId]
    );

    let position = null;
    if (posRows.length > 0 && Number(posRows[0].total_fraction_owned) > 0) {
      const p = posRows[0];
      const live = liveState[p.symbol];
      const unitPrice = live && live.price > 0 ? live.price : Number(p.current_price);
      const qty = Number(p.total_fraction_owned);
      const invested = Number(p.total_amount_invested);
      const currentValue = qty * unitPrice;

      position = {
        qty,
        invested,
        avg_price: Number(p.average_purchase_price),
        current_value: currentValue,
        pl: currentValue - invested
      };
    }

    res.json({
      wallet_balance: Number(walletRow?.balance || 0),
      position
    });
  } catch (err) {
    console.error('Position lookup error:', err);
    res.status(500).json({ error: 'Failed to load position.' });
  }
});

// POST /api/trades/buy — ACID fractional/whole asset purchase
app.post('/api/trades/buy', async (req, res) => {
  const userId = Number(req.body?.user_id);
  const assetId = Number(req.body?.asset_id);
  const amount = Number(req.body?.amount);

  if (!userId || !assetId || !Number.isFinite(amount) || amount <= 0) {
    return res.status(400).json({ error: 'Enter a valid investment amount.' });
  }

  let conn;
  try {
    conn = await pool.getConnection();
    await conn.beginTransaction();

    const [walletRows] = await conn.execute(
      'SELECT wallet_id, balance FROM wallets WHERE user_id = ? FOR UPDATE',
      [userId]
    );
    if (walletRows.length === 0) {
      await conn.rollback();
      return res.status(404).json({ error: 'Wallet not found.' });
    }
    const wallet = walletRows[0];
    const currentBalance = Number(wallet.balance);
    if (currentBalance < amount) {
      await conn.rollback();
      return res.status(400).json({ error: `Insufficient balance (${currentBalance.toFixed(2)} VC available).` });
    }

    const [assetRows] = await conn.execute(
      'SELECT asset_id, symbol, current_price, is_active FROM assets WHERE asset_id = ? LIMIT 1',
      [assetId]
    );
    if (assetRows.length === 0 || !assetRows[0].is_active) {
      await conn.rollback();
      return res.status(404).json({ error: 'Asset not available for trading.' });
    }
    const asset = assetRows[0];
    const live = liveState[asset.symbol];
    const unitPrice = live && live.price > 0 ? live.price : Number(asset.current_price);
    if (unitPrice <= 0) {
      await conn.rollback();
      return res.status(400).json({ error: 'Live price unavailable. Try again shortly.' });
    }

    const acquiredFraction = amount / unitPrice;
    const newBalance = Number((currentBalance - amount).toFixed(2));

    await conn.execute(
      'UPDATE wallets SET balance = ? WHERE wallet_id = ?',
      [newBalance, wallet.wallet_id]
    );

    const [portRows] = await conn.execute(
      'SELECT portfolio_id, total_fraction_owned, total_amount_invested FROM portfolio WHERE user_id = ? AND asset_id = ? FOR UPDATE',
      [userId, assetId]
    );

    if (portRows.length > 0) {
      const p = portRows[0];
      const newFraction = Number(p.total_fraction_owned) + acquiredFraction;
      const newInvested = Number(p.total_amount_invested) + amount;
      const newAvgPrice = newInvested / newFraction;

      await conn.execute(
        `UPDATE portfolio
         SET total_fraction_owned = ?, total_amount_invested = ?, average_purchase_price = ?
         WHERE user_id = ? AND portfolio_id = ?`,
        [newFraction, newInvested, newAvgPrice, userId, p.portfolio_id]
      );
    } else {
      const [[{ nextId }]] = await conn.execute(
        'SELECT COALESCE(MAX(portfolio_id), 0) + 1 AS nextId FROM portfolio WHERE user_id = ?',
        [userId]
      );
      await conn.execute(
        `INSERT INTO portfolio (user_id, portfolio_id, asset_id, total_fraction_owned, total_amount_invested, average_purchase_price)
         VALUES (?, ?, ?, ?, ?, ?)`,
        [userId, nextId, assetId, acquiredFraction, amount, unitPrice]
      );
    }

    await conn.execute(
      `INSERT INTO trade (buyer_user_id, seller_user_id, buyer_asset_id, seller_asset_id, buyer_asset_fraction, seller_asset_fraction, unit_price, total_amount, trade_status)
       VALUES (?, NULL, ?, NULL, ?, 0, ?, ?, 'COMPLETED')`,
      [userId, assetId, acquiredFraction, unitPrice, amount]
    );

    await conn.execute(
      `INSERT INTO wallet_history (wallet_id, transaction_type, transacted_amount, balance_after, description)
       VALUES (?, 'BUY', ?, ?, ?)`,
      [wallet.wallet_id, amount, newBalance, `Bought ${acquiredFraction.toFixed(6)} ${asset.symbol}`]
    );

    await conn.commit();
    res.json({
      message: `Bought ${acquiredFraction.toFixed(6)} ${asset.symbol} for ${amount.toFixed(2)} VC`,
      wallet_balance: newBalance
    });
  } catch (err) {
    if (conn) await conn.rollback();
    console.error('Buy trade error:', err);
    res.status(500).json({ error: 'Trade execution failed.' });
  } finally {
    if (conn) conn.release();
  }
});

// POST /api/trades/sell — ACID fractional/whole asset sale
app.post('/api/trades/sell', async (req, res) => {
  const userId = Number(req.body?.user_id);
  const assetId = Number(req.body?.asset_id);
  const sellQty = Number(req.body?.quantity);

  if (!userId || !assetId || !Number.isFinite(sellQty) || sellQty <= 0) {
    return res.status(400).json({ error: 'Enter a valid quantity to sell.' });
  }

  let conn;
  try {
    conn = await pool.getConnection();
    await conn.beginTransaction();

    const [portRows] = await conn.execute(
      'SELECT portfolio_id, total_fraction_owned, total_amount_invested, average_purchase_price FROM portfolio WHERE user_id = ? AND asset_id = ? FOR UPDATE',
      [userId, assetId]
    );
    if (portRows.length === 0) {
      await conn.rollback();
      return res.status(400).json({ error: 'You do not own this asset.' });
    }

    const pos = portRows[0];
    const ownedQty = Number(pos.total_fraction_owned);
    if (sellQty > ownedQty + 1e-12) {
      await conn.rollback();
      return res.status(400).json({ error: `Insufficient holdings (${ownedQty.toFixed(6)} units available).` });
    }

    const actualSellQty = Math.min(sellQty, ownedQty);

    const [assetRows] = await conn.execute(
      'SELECT asset_id, symbol, current_price FROM assets WHERE asset_id = ? LIMIT 1',
      [assetId]
    );
    const asset = assetRows[0];
    const live = liveState[asset.symbol];
    const unitPrice = live && live.price > 0 ? live.price : Number(asset.current_price);
    const proceeds = Number((actualSellQty * unitPrice).toFixed(2));

    const [walletRows] = await conn.execute(
      'SELECT wallet_id, balance FROM wallets WHERE user_id = ? FOR UPDATE',
      [userId]
    );
    const wallet = walletRows[0];
    const newBalance = Number((Number(wallet.balance) + proceeds).toFixed(2));

    await conn.execute(
      'UPDATE wallets SET balance = ? WHERE wallet_id = ?',
      [newBalance, wallet.wallet_id]
    );

    const remainingQty = Math.max(0, ownedQty - actualSellQty);
    const remainingRatio = ownedQty > 0 ? remainingQty / ownedQty : 0;
    const remainingInvested = remainingQty < 1e-12 ? 0 : Number((Number(pos.total_amount_invested) * remainingRatio).toFixed(2));
    const avgPrice = remainingQty < 1e-12 ? 0 : Number(pos.average_purchase_price);

    await conn.execute(
      `UPDATE portfolio
       SET total_fraction_owned = ?, total_amount_invested = ?, average_purchase_price = ?
       WHERE user_id = ? AND portfolio_id = ?`,
      [remainingQty < 1e-12 ? 0 : remainingQty, remainingInvested, avgPrice, userId, pos.portfolio_id]
    );

    await conn.execute(
      `INSERT INTO trade (buyer_user_id, seller_user_id, buyer_asset_id, seller_asset_id, buyer_asset_fraction, seller_asset_fraction, unit_price, total_amount, trade_status)
       VALUES (?, ?, ?, ?, 0, ?, ?, ?, 'COMPLETED')`,
      [userId, userId, assetId, assetId, actualSellQty, unitPrice, proceeds]
    );

    await conn.execute(
      `INSERT INTO wallet_history (wallet_id, transaction_type, transacted_amount, balance_after, description)
       VALUES (?, 'SELL', ?, ?, ?)`,
      [wallet.wallet_id, proceeds, newBalance, `Sold ${actualSellQty.toFixed(6)} ${asset.symbol}`]
    );

    await conn.commit();
    res.json({
      message: `Sold ${actualSellQty.toFixed(6)} ${asset.symbol} for ${proceeds.toFixed(2)} VC`,
      wallet_balance: newBalance
    });
  } catch (err) {
    if (conn) await conn.rollback();
    console.error('Sell trade error:', err);
    res.status(500).json({ error: 'Sell execution failed.' });
  } finally {
    if (conn) conn.release();
  }
});

// API: Single asset details + latest performance snapshot
// API: Single asset details + latest performance snapshot
app.get('/api/assets/:id', async (req, res) => {
  const assetId = Number(req.params.id);
  if (!assetId) return res.status(400).json({ error: 'Invalid asset ID.' });

  try {
    const [rows] = await pool.query(
      'SELECT asset_id, symbol, name, asset_type, current_price, price_change, risk, is_active FROM assets WHERE asset_id = ? LIMIT 1',
      [assetId]
    );
    if (rows.length === 0) return res.status(404).json({ error: 'Asset not found.' });

    const row = rows[0];
    const live = liveState[row.symbol];
    const price = live && live.price > 0 ? live.price : Number(row.current_price);
    const changePct = live ? Number(live.changePct.toFixed(4)) : Number(row.price_change);

    const [perfRows] = await pool.query(
      `SELECT snapshot_date, opening_value, closing_value, day_high, day_low, total_volume
       FROM asset_performance
       WHERE asset_id = ?
       ORDER BY snapshot_date DESC
       LIMIT 1`,
      [assetId]
    );

    res.json({
      asset_id: row.asset_id,
      symbol: row.symbol,
      name: row.name,
      asset_type: row.asset_type,
      current_price: price,
      price_change: changePct,
      risk: live ? live.risk : row.risk,
      prev_close: live?.prevClose || price,
      performance: perfRows[0] || null
    });
  } catch (err) {
    console.error('Asset detail error:', err);
    res.status(500).json({ error: 'Failed to load asset details.' });
  }
});

// API: Verify recipient username for P2P transfers
app.get('/api/users/verify/:username', async (req, res) => {
  const clean = (req.params.username || '').trim();
  const excludeId = Number(req.query.exclude || 0);
  if (!clean) return res.status(400).json({ error: 'Enter a username.' });

  try {
    const [rows] = await pool.query(
      `SELECT user_id, username, first_name, middle_name, last_name, account_status
       FROM users
       WHERE username = ?
       LIMIT 1`,
      [clean]
    );
    if (rows.length === 0) {
      return res.status(404).json({ error: `User "@${clean}" not found.` });
    }
    const u = rows[0];
    if (excludeId && Number(u.user_id) === excludeId) {
      return res.status(400).json({ error: 'You cannot transfer assets to yourself.' });
    }
    if (u.account_status !== 'ACTIVE') {
      return res.status(403).json({ error: 'Recipient account is not active.' });
    }

    const fullName = [u.first_name, u.middle_name, u.last_name].filter(Boolean).join(' ');
    res.json({ user_id: u.user_id, username: u.username, full_name: fullName });
  } catch (err) {
    console.error('Verify user error:', err);
    res.status(500).json({ error: 'Verification failed.' });
  }
});

// API: List P2P asset transfers for a user
app.get('/api/transfers/asset/:userId', async (req, res) => {
  const userId = Number(req.params.userId);
  if (!userId) return res.status(400).json({ error: 'Invalid user ID.' });

  try {
    const [rows] = await pool.query(
      `SELECT t.trade_id, t.buyer_user_id, t.seller_user_id,
              t.buyer_asset_fraction AS qty, t.unit_price, t.total_amount, t.timestamp,
              a.symbol, a.name AS asset_name,
              bu.username AS to_username, su.username AS from_username
       FROM trade t
       JOIN assets a ON t.buyer_asset_id = a.asset_id
       JOIN users bu ON t.buyer_user_id = bu.user_id
       JOIN users su ON t.seller_user_id = su.user_id
       WHERE t.buyer_user_id IS NOT NULL
         AND t.seller_user_id IS NOT NULL
         AND t.buyer_user_id <> t.seller_user_id
         AND (t.buyer_user_id = ? OR t.seller_user_id = ?)
         AND t.trade_status = 'COMPLETED'
       ORDER BY t.timestamp DESC
       LIMIT 20`,
      [userId, userId]
    );

    const formatted = rows.map((r) => {
      const isOut = Number(r.seller_user_id) === userId;
      return {
        trade_id: r.trade_id,
        dir: isOut ? 'OUT' : 'IN',
        symbol: r.symbol,
        asset_name: r.asset_name,
        qty: Number(r.qty),
        unit_price: Number(r.unit_price),
        total_amount: Number(r.total_amount),
        counterparty: isOut ? r.to_username : r.from_username,
        timestamp: r.timestamp
      };
    });

    res.json(formatted);
  } catch (err) {
    console.error('Asset transfer history error:', err);
    res.status(500).json({ error: 'Failed to load P2P asset transfers.' });
  }
});

// POST /api/transfers/asset — ACID Peer-to-Peer Asset Transfer
app.post('/api/transfers/asset', async (req, res) => {
  const senderId = Number(req.body?.sender_id);
  const recipientUsername = (req.body?.recipient_username || '').trim();
  const assetId = Number(req.body?.asset_id);
  const qty = Number(req.body?.quantity);

  if (!senderId || !recipientUsername || !assetId || !Number.isFinite(qty) || qty <= 0) {
    return res.status(400).json({ error: 'Provide recipient username, asset, and valid quantity.' });
  }

  let conn;
  try {
    conn = await pool.getConnection();
    await conn.beginTransaction();

    // 1. Validate recipient
    const [recRows] = await conn.execute(
      'SELECT user_id, username, account_status FROM users WHERE username = ? LIMIT 1',
      [recipientUsername]
    );
    if (recRows.length === 0) {
      await conn.rollback();
      return res.status(404).json({ error: `User "@${recipientUsername}" not found.` });
    }
    const recipient = recRows[0];
    if (Number(recipient.user_id) === senderId) {
      await conn.rollback();
      return res.status(400).json({ error: 'Cannot transfer assets to yourself.' });
    }
    if (recipient.account_status !== 'ACTIVE') {
      await conn.rollback();
      return res.status(403).json({ error: 'Recipient account is suspended or inactive.' });
    }

    // 2. Lock sender's portfolio holding
    const [senderPort] = await conn.execute(
      'SELECT portfolio_id, total_fraction_owned, total_amount_invested, average_purchase_price FROM portfolio WHERE user_id = ? AND asset_id = ? FOR UPDATE',
      [senderId, assetId]
    );
    if (senderPort.length === 0 || Number(senderPort[0].total_fraction_owned) < qty - 1e-12) {
      await conn.rollback();
      return res.status(400).json({ error: 'Insufficient asset holdings to transfer.' });
    }

    const sp = senderPort[0];
    const ownedQty = Number(sp.total_fraction_owned);
    const actualQty = Math.min(qty, ownedQty);

    // 3. Get live valuation for ledger record
    const [assetRows] = await conn.execute(
      'SELECT asset_id, symbol, current_price FROM assets WHERE asset_id = ? LIMIT 1',
      [assetId]
    );
    const asset = assetRows[0];
    const live = liveState[asset.symbol];
    const unitPrice = live && live.price > 0 ? live.price : Number(asset.current_price);
    const notionalValue = Number((actualQty * unitPrice).toFixed(2));

    // Proportional cost basis moved from sender to recipient
    const transferRatio = ownedQty > 0 ? actualQty / ownedQty : 0;
    const transferredBasis = Number((Number(sp.total_amount_invested) * transferRatio).toFixed(2));

    // 4. Deduct from sender's portfolio
    const remQty = Math.max(0, ownedQty - actualQty);
    const remInvested = remQty < 1e-12 ? 0 : Math.max(0, Number((Number(sp.total_amount_invested) - transferredBasis).toFixed(2)));
    const remAvg = remQty < 1e-12 ? 0 : Number(sp.average_purchase_price);

    await conn.execute(
      `UPDATE portfolio
       SET total_fraction_owned = ?, total_amount_invested = ?, average_purchase_price = ?
       WHERE user_id = ? AND portfolio_id = ?`,
      [remQty < 1e-12 ? 0 : remQty, remInvested, remAvg, senderId, sp.portfolio_id]
    );

    // 5. Credit recipient's portfolio (upsert)
    const [recPort] = await conn.execute(
      'SELECT portfolio_id, total_fraction_owned, total_amount_invested FROM portfolio WHERE user_id = ? AND asset_id = ? FOR UPDATE',
      [recipient.user_id, assetId]
    );

    if (recPort.length > 0) {
      const rp = recPort[0];
      const newRecQty = Number(rp.total_fraction_owned) + actualQty;
      const newRecInv = Number(rp.total_amount_invested) + transferredBasis;
      const newRecAvg = newRecQty > 0 ? newRecInv / newRecQty : unitPrice;

      await conn.execute(
        `UPDATE portfolio
         SET total_fraction_owned = ?, total_amount_invested = ?, average_purchase_price = ?
         WHERE user_id = ? AND portfolio_id = ?`,
        [newRecQty, newRecInv, newRecAvg, recipient.user_id, rp.portfolio_id]
      );
    } else {
      const [[{ nextId }]] = await conn.execute(
        'SELECT COALESCE(MAX(portfolio_id), 0) + 1 AS nextId FROM portfolio WHERE user_id = ?',
        [recipient.user_id]
      );
      const initAvg = actualQty > 0 ? transferredBasis / actualQty : unitPrice;
      await conn.execute(
        `INSERT INTO portfolio (user_id, portfolio_id, asset_id, total_fraction_owned, total_amount_invested, average_purchase_price)
         VALUES (?, ?, ?, ?, ?, ?)`,
        [recipient.user_id, nextId, assetId, actualQty, transferredBasis, initAvg]
      );
    }

    // 6. Log P2P transfer in immutable `trade` ledger
    await conn.execute(
      `INSERT INTO trade (buyer_user_id, seller_user_id, buyer_asset_id, seller_asset_id, buyer_asset_fraction, seller_asset_fraction, unit_price, total_amount, trade_status)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'COMPLETED')`,
      [recipient.user_id, senderId, assetId, assetId, actualQty, actualQty, unitPrice, notionalValue]
    );

    await conn.commit();
    res.json({
      message: `Transferred ${actualQty.toFixed(6)} ${asset.symbol} to @${recipient.username}`
    });
  } catch (err) {
    if (conn) await conn.rollback();
    console.error('Asset transfer error:', err);
    res.status(500).json({ error: 'Asset transfer failed.' });
  } finally {
    if (conn) conn.release();
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

  const [activeAssets] = await pool.query(
    'SELECT asset_id, symbol, name, asset_type, current_price, price_change, risk FROM assets WHERE is_active = 1'
  );

  for (const row of activeAssets) {
    await bootstrapAssetFromRest(row, apiKey);
    if (!row.symbol.toUpperCase().endsWith('.NS')) {
      subscribeWsSymbol(toFinnhubSymbol(row.symbol, row.asset_type));
    }
  }

  await syncSnapshotsToDb(apiKey);
  connectFinnhubWebSocket(apiKey);

  setInterval(() => syncSnapshotsToDb(apiKey), 30000);
});