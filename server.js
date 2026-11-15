require('dotenv').config();
const express = require('express');
const path = require('path');
const crypto = require('crypto');
const WebSocket = require('ws');
const bcrypt = require('bcryptjs');
const pool = require('./db');

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'pages')));

// =========================================================
// AAANNNNNNN ID VALIDATION & GENERATION HELPERS
// =========================================================
function parsePrefixedId(val, expectedPrefix) {
  const s = String(val ?? '').trim().toUpperCase();
  const re = new RegExp(`^${expectedPrefix}[0-9]{7}$`);
  return re.test(s) ? s : null;
}

// Ordered IDs: USR0000001, TRD0000001 (must be called inside an active transaction)
async function nextSequentialId(conn, tableName, columnName, prefix) {
  const [rows] = await conn.execute(
    `SELECT COALESCE(MAX(CAST(SUBSTRING(${columnName}, 4) AS UNSIGNED)), 0) + 1 AS nextNum
     FROM ${tableName}
     WHERE ${columnName} REGEXP ?
     FOR UPDATE`,
    [`^${prefix}[0-9]{7}$`]
  );
  const nextNum = Number(rows[0]?.nextNum || 1);
  if (nextNum > 9999999) {
    throw new Error(`Maximum ID capacity reached for ${prefix}`);
  }
  return `${prefix}${String(nextNum).padStart(7, '0')}`;
}

// Random 7-digit IDs with DB collision check: WLT, AST, LND, LON, PAY
async function generateUniqueRandomId(executor, tableName, columnName, prefix) {
  for (let attempt = 0; attempt < 25; attempt++) {
    const num = crypto.randomInt(0, 10000000);
    const candidate = `${prefix}${String(num).padStart(7, '0')}`;
    const [existing] = await executor.execute(
      `SELECT 1 FROM ${tableName} WHERE ${columnName} = ? LIMIT 1`,
      [candidate]
    );
    if (existing.length === 0) {
      return candidate;
    }
  }
  throw new Error(`Unable to generate unique ${prefix} ID after multiple attempts.`);
}

// In-memory state populated dynamically from the MySQL `assets` table
const liveState = {};       // symbol -> { asset_id, symbol, name, asset_type, finnhub, price, prevClose, changePct, risk }
const finnhubToSymbol = {}; // 'BINANCE:BTCUSDT' -> 'BTC'
const betaCache = {};       // symbol -> 'LOW' | 'MEDIUM' | 'HIGH'
const candleCache = {};     // 'BTC:1D' -> { data, expiresAt }
const sseClients = new Set();

function broadcastP2PEvent(room) {
  if (typeof sseClients === 'undefined') return;
  const payload = `event: p2p\ndata: ${JSON.stringify(room)}\n\n`;
  for (const client of sseClients) {
    try {
      client.write(payload);
    } catch (_) {}
  }
}

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

// POST /api/auth/register — Atomic ACID signup (USR0000001 + WLTNNNNNNN + wallet_history)
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

    const userId = await nextSequentialId(conn, 'users', 'user_id', 'USR');

    await conn.execute(
      `INSERT INTO users (user_id, username, first_name, middle_name, last_name, email, password_hash, role, account_status)
       VALUES (?, ?, ?, ?, ?, ?, ?, 'USER', 'ACTIVE')`,
      [userId, cleanUser, cleanFirst, cleanMiddle, cleanLast, cleanEmail, passwordHash]
    );

    const initialBalance = 10000.00;
    const walletId = await generateUniqueRandomId(conn, 'wallets', 'wallet_id', 'WLT');

    await conn.execute(
      `INSERT INTO wallets (wallet_id, user_id, currency, balance)
       VALUES (?, ?, 'INR', ?)`,
      [walletId, userId, initialBalance]
    );

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

// API: Lookup or add a non-featured ticker on demand (generates random ASTNNNNNNN)
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
      const assetId = await generateUniqueRandomId(pool, 'assets', 'asset_id', 'AST');

      await pool.query(
        `INSERT INTO assets (asset_id, symbol, name, asset_type, current_price, price_change, risk, is_active)
         VALUES (?, ?, ?, 'STOCK', ?, ?, 'MEDIUM', TRUE)`,
        [assetId, rawSymbol, assetName, price, changePct]
      );

      const newRow = {
        asset_id: assetId,
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
    const assetId = await generateUniqueRandomId(pool, 'assets', 'asset_id', 'AST');

    await pool.query(
      `INSERT INTO assets (asset_id, symbol, name, asset_type, current_price, price_change, risk, is_active)
       VALUES (?, ?, ?, ?, ?, ?, ?, TRUE)`,
      [assetId, rawSymbol, assetName, assetType, quote.c, changePct, risk]
    );

    const newRow = {
      asset_id: assetId,
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
  const assetId = parsePrefixedId(req.params.id, 'AST');
  if (!assetId) return res.status(400).json({ error: 'Invalid asset ID.' });

  const range = (req.query.range || '1D').toUpperCase();
  const validRanges = ['1D', '1W', '1M', '1Y'];
  const selectedRange = validRanges.includes(range) ? range : '1D';

  try {
    const [rows] = await pool.query(
      'SELECT asset_id, symbol, asset_type FROM assets WHERE asset_id = ?',
      [assetId]
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
  const userId = parsePrefixedId(req.params.userId, 'USR');
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
      const isBuy = t.buyer_user_id === userId;
      return {
        trade_id: t.trade_id,
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
    const myIdx = standings.findIndex((s) => s.user_id === userId);
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
  const userId = parsePrefixedId(req.params.userId, 'USR');
  const assetId = parsePrefixedId(req.params.assetId, 'AST');
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

// POST /api/trades/buy — ACID fractional/whole asset purchase (ordered TRD0000001)
app.post('/api/trades/buy', async (req, res) => {
  const userId = parsePrefixedId(req.body?.user_id, 'USR');
  const assetId = parsePrefixedId(req.body?.asset_id, 'AST');
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

    const tradeId = await nextSequentialId(conn, 'trade', 'trade_id', 'TRD');

    await conn.execute(
      `INSERT INTO trade (trade_id, buyer_user_id, seller_user_id, buyer_asset_id, seller_asset_id, buyer_asset_fraction, seller_asset_fraction, unit_price, total_amount, trade_status)
       VALUES (?, ?, NULL, ?, NULL, ?, 0, ?, ?, 'COMPLETED')`,
      [tradeId, userId, assetId, acquiredFraction, unitPrice, amount]
    );

    await conn.execute(
      `INSERT INTO wallet_history (wallet_id, transaction_type, transacted_amount, balance_after, description)
       VALUES (?, 'BUY', ?, ?, ?)`,
      [wallet.wallet_id, amount, newBalance, `Bought ${acquiredFraction.toFixed(6)} ${asset.symbol}`]
    );

    await conn.commit();
    res.json({
      trade_id: tradeId,
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

// POST /api/trades/sell — ACID fractional/whole asset sale (ordered TRD0000001)
app.post('/api/trades/sell', async (req, res) => {
  const userId = parsePrefixedId(req.body?.user_id, 'USR');
  const assetId = parsePrefixedId(req.body?.asset_id, 'AST');
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

    const tradeId = await nextSequentialId(conn, 'trade', 'trade_id', 'TRD');

    await conn.execute(
      `INSERT INTO trade (trade_id, buyer_user_id, seller_user_id, buyer_asset_id, seller_asset_id, buyer_asset_fraction, seller_asset_fraction, unit_price, total_amount, trade_status)
       VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?, 'COMPLETED')`,
      [tradeId, userId, userId, assetId, assetId, actualSellQty, unitPrice, proceeds]
    );

    await conn.execute(
      `INSERT INTO wallet_history (wallet_id, transaction_type, transacted_amount, balance_after, description)
       VALUES (?, 'SELL', ?, ?, ?)`,
      [wallet.wallet_id, proceeds, newBalance, `Sold ${actualSellQty.toFixed(6)} ${asset.symbol}`]
    );

    await conn.commit();
    res.json({
      trade_id: tradeId,
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
app.get('/api/assets/:id', async (req, res) => {
  const assetId = parsePrefixedId(req.params.id, 'AST');
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
  const excludeId = parsePrefixedId(req.query.exclude, 'USR');
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
    if (excludeId && u.user_id === excludeId) {
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

// =========================================================
// OPTION 2: SINGLE-ASSET STEP-BY-STEP P2P NEGOTIATION ROOM
// =========================================================
const p2pRooms = new Map();
let p2pRoomSeq = 100;

// GET /api/p2p/rooms/:userId — Active Negotiation Room + Settled Single-Asset Swap History
app.get('/api/p2p/rooms/:userId', async (req, res) => {
  const userId = parsePrefixedId(req.params.userId, 'USR');
  if (!userId) return res.status(400).json({ error: 'Invalid user ID.' });

  try {
    let activeRoom = null;
    for (const room of p2pRooms.values()) {
      if ((room.maker.user_id === userId || room.taker.user_id === userId) && !['SETTLED', 'CLOSED'].includes(room.status)) {
        activeRoom = room;
        break;
      }
    }

    const [rows] = await pool.query(
      `SELECT t.trade_id, t.buyer_user_id, t.seller_user_id,
              t.buyer_asset_fraction AS maker_gets_qty,
              t.seller_asset_fraction AS maker_gives_qty,
              t.total_amount, t.timestamp,
              ba.symbol AS maker_gets_symbol,
              sa.symbol AS maker_gives_symbol,
              bu.username AS maker_username,
              su.username AS taker_username
       FROM trade t
       JOIN assets ba ON t.buyer_asset_id = ba.asset_id
       JOIN assets sa ON t.seller_asset_id = sa.asset_id
       JOIN users bu ON t.buyer_user_id = bu.user_id
       JOIN users su ON t.seller_user_id = su.user_id
       WHERE t.seller_user_id IS NOT NULL
         AND t.buyer_user_id IS NOT NULL
         AND t.seller_user_id <> t.buyer_user_id
         AND (t.seller_user_id = ? OR t.buyer_user_id = ?)
         AND t.trade_status = 'COMPLETED'
       ORDER BY t.timestamp DESC
       LIMIT 25`,
      [userId, userId]
    );

    const history = rows.map((r) => {
      const isMaker = r.buyer_user_id === userId;
      return {
        trade_id: r.trade_id,
        gave_qty: Number(isMaker ? r.maker_gives_qty : r.maker_gets_qty),
        gave_symbol: isMaker ? r.maker_gives_symbol : r.maker_gets_symbol,
        got_qty: Number(isMaker ? r.maker_gets_qty : r.maker_gives_qty),
        got_symbol: isMaker ? r.maker_gets_symbol : r.maker_gives_symbol,
        counterparty: isMaker ? r.taker_username : r.maker_username,
        total_amount: Number(r.total_amount),
        timestamp: r.timestamp
      };
    });

    res.json({ active_room: activeRoom, history });
  } catch (err) {
    console.error('P2P room fetch error:', err);
    res.status(500).json({ error: 'Failed to load P2P room.' });
  }
});

// POST /api/p2p/invite — Step 1: Maker invites Taker
app.post('/api/p2p/invite', async (req, res) => {
  const makerId = parsePrefixedId(req.body?.maker_id, 'USR');
  const takerUsername = (req.body?.taker_username || '').trim();
  if (!makerId || !takerUsername) return res.status(400).json({ error: 'Enter recipient username.' });

  try {
    const [[maker]] = await pool.query('SELECT user_id, username FROM users WHERE user_id = ?', [makerId]);
    const [tRows] = await pool.query('SELECT user_id, username, account_status FROM users WHERE username = ?', [takerUsername]);

    if (!maker || tRows.length === 0) return res.status(404).json({ error: `User "@${takerUsername}" not found.` });
    const taker = tRows[0];
    if (taker.user_id === makerId) return res.status(400).json({ error: 'Cannot trade with yourself.' });
    if (taker.account_status !== 'ACTIVE') return res.status(403).json({ error: 'Recipient account is not active.' });

    for (const r of p2pRooms.values()) {
      if (!['SETTLED', 'CLOSED'].includes(r.status)) {
        if (r.maker.user_id === makerId || r.taker.user_id === makerId) {
          return res.status(400).json({ error: 'You already have an active P2P trade room open.' });
        }
        if (r.maker.user_id === taker.user_id || r.taker.user_id === taker.user_id) {
          return res.status(400).json({ error: `@${taker.username} is currently in another active P2P trade.` });
        }
      }
    }

    const roomId = ++p2pRoomSeq;
    const room = {
      room_id: roomId,
      status: 'INVITED', // INVITED -> A BUILDING -> B REVIEWING -> B BUILDING -> A REVIEWING -> SETTLED | CLOSED
      maker: { user_id: maker.user_id, username: maker.username },
      taker: { user_id: taker.user_id, username: taker.username },
      frozen_prices: {},
      offer_a: null, // { asset_id, symbol, qty, unit_price, total_vc }
      offer_b: null,
      last_note: `@${maker.username} invited @${taker.username} to a single-asset trade.`
    };

    p2pRooms.set(roomId, room);
    broadcastP2PEvent(room);
    res.json({ message: `Trade request sent to @${taker.username}`, room });
  } catch (err) {
    console.error('P2P invite error:', err);
    res.status(500).json({ error: 'Failed to send trade request.' });
  }
});

// POST /api/p2p/:roomId/respond-invite — Step 2: Taker accepts (locks live prices) or declines
app.post('/api/p2p/:roomId/respond-invite', async (req, res) => {
  const roomId = Number(req.params.roomId);
  const userId = parsePrefixedId(req.body?.user_id, 'USR');
  const accept = Boolean(req.body?.accept);

  const room = p2pRooms.get(roomId);
  if (!room || room.status !== 'INVITED') return res.status(404).json({ error: 'Invite not found or expired.' });
  if (room.taker.user_id !== userId) return res.status(403).json({ error: 'Only the invited trader can respond.' });

  if (!accept) {
    room.status = 'CLOSED';
    room.last_note = `@${room.taker.username} declined the trade request.`;
    broadcastP2PEvent(room);
    p2pRooms.delete(roomId);
    return res.json({ message: 'Trade request declined.', room });
  }

  try {
    const [assets] = await pool.query('SELECT asset_id, symbol, current_price FROM assets WHERE is_active = 1');
    const frozen = {};
    for (const a of assets) {
      const live = liveState[a.symbol];
      frozen[a.symbol] = live && live.price > 0 ? Number(live.price) : Number(a.current_price);
    }

    room.frozen_prices = frozen;
    room.status = 'A BUILDING';
    room.last_note = `@${room.taker.username} accepted! Prices locked — waiting on @${room.maker.username}'s single-asset offer.`;
    broadcastP2PEvent(room);
    res.json({ message: 'Invite accepted! Market prices are now locked.', room });
  } catch (err) {
    res.status(500).json({ error: 'Failed to lock market prices.' });
  }
});

// POST /api/p2p/:roomId/submit-offer — Step 3 / Step 5: Submit 1 asset + qty
app.post('/api/p2p/:roomId/submit-offer', async (req, res) => {
  const roomId = Number(req.params.roomId);
  const userId = parsePrefixedId(req.body?.user_id, 'USR');
  const assetId = parsePrefixedId(req.body?.asset_id, 'AST');
  const qty = Number(req.body?.qty);

  const room = p2pRooms.get(roomId);
  if (!room) return res.status(404).json({ error: 'Trade room not found.' });

  const isMaker = room.maker.user_id === userId && room.status === 'A BUILDING';
  const isTaker = room.taker.user_id === userId && room.status === 'B BUILDING';
  if (!isMaker && !isTaker) return res.status(403).json({ error: 'It is not your turn to submit an offer.' });

  if (!assetId || !Number.isFinite(qty) || qty <= 0) {
    return res.status(400).json({ error: 'Select 1 asset and enter a valid quantity.' });
  }
  if (isTaker && room.offer_a && room.offer_a.asset_id === assetId) {
    return res.status(400).json({ error: `Select a different asset than ${room.offer_a.symbol}.` });
  }

  try {
    const [pRows] = await pool.query(
      `SELECT p.total_fraction_owned, a.symbol
       FROM portfolio p
       JOIN assets a ON p.asset_id = a.asset_id
       WHERE p.user_id = ? AND p.asset_id = ? LIMIT 1`,
      [userId, assetId]
    );

    if (pRows.length === 0 || Number(pRows[0].total_fraction_owned) < qty - 1e-12) {
      return res.status(400).json({ error: 'Insufficient holdings for that asset.' });
    }

    const symbol = pRows[0].symbol;
    const lockedPx = Number(room.frozen_prices[symbol] || 0);
    if (lockedPx <= 0) return res.status(400).json({ error: 'Locked price unavailable.' });

    const cleanQty = Math.min(qty, Number(pRows[0].total_fraction_owned));
    const totalVc = Number((cleanQty * lockedPx).toFixed(2));

    // When Taker submits counter-offer, require matched locked value within ±0.50 VC
    if (isTaker && room.offer_a) {
      const diff = Math.abs(totalVc - room.offer_a.total_vc);
      if (diff > 0.50) {
        return res.status(400).json({
          error: `Offer (${totalVc.toFixed(2)} VC) must match @${room.maker.username}'s locked value (${room.offer_a.total_vc.toFixed(2)} VC ±0.50 VC).`
        });
      }
    }

    const offerObj = {
      asset_id: assetId,
      symbol,
      qty: cleanQty,
      unit_price: lockedPx,
      total_vc: totalVc
    };

    if (isMaker) {
      room.offer_a = offerObj;
      room.status = 'B REVIEWING';
      room.last_note = `@${room.maker.username} offered ${cleanQty.toFixed(6)} ${symbol} (${totalVc.toFixed(2)} VC). Waiting on @${room.taker.username}.`;
    } else {
      room.offer_b = offerObj;
      room.status = 'A REVIEWING';
      room.last_note = `@${room.taker.username} countered with ${cleanQty.toFixed(6)} ${symbol} (${totalVc.toFixed(2)} VC). Waiting on @${room.maker.username}.`;
    }

    broadcastP2PEvent(room);
    res.json({ message: 'Offer submitted and locked!', room });
  } catch (err) {
    console.error('Submit offer error:', err);
    res.status(500).json({ error: 'Failed to submit offer.' });
  }
});

// POST /api/p2p/:roomId/review — Step 4 / Step 6 & 7: Accept, Reject, or Leave
app.post('/api/p2p/:roomId/review', async (req, res) => {
  const roomId = Number(req.params.roomId);
  const userId = parsePrefixedId(req.body?.user_id, 'USR');
  const decision = req.body?.decision; // 'ACCEPT' | 'REJECT' | 'LEAVE'

  const room = p2pRooms.get(roomId);
  if (!room) return res.status(404).json({ error: 'Trade room not found.' });

  const isMaker = room.maker.user_id === userId;
  const isTaker = room.taker.user_id === userId;
  if (!isMaker && !isTaker) return res.status(403).json({ error: 'Not a participant in this room.' });

  const actor = isMaker ? room.maker.username : room.taker.username;

  if (decision === 'LEAVE') {
    room.status = 'CLOSED';
    room.last_note = `@${actor} left the trade room. Trade cancelled.`;
    broadcastP2PEvent(room);
    p2pRooms.delete(roomId);
    return res.json({ message: 'You left the trade room.', room });
  }

  if (decision === 'REJECT') {
    if (isTaker && room.status === 'B REVIEWING') {
      room.offer_a = null;
      room.status = 'A BUILDING';
      room.last_note = `@${actor} rejected the offer — @${room.maker.username} may revise or leave.`;
    } else if (isMaker && room.status === 'A REVIEWING') {
      room.offer_b = null;
      room.status = 'B BUILDING';
      room.last_note = `@${actor} rejected the counter-offer — @${room.taker.username} may revise or leave.`;
    } else {
      return res.status(400).json({ error: 'Cannot reject at this stage.' });
    }
    broadcastP2PEvent(room);
    return res.json({ message: 'Offer rejected.', room });
  }

  if (decision !== 'ACCEPT') return res.status(400).json({ error: 'Invalid decision.' });

  // Step 4: Taker accepts Maker's offer -> Taker builds single-asset counter-offer
  if (isTaker && room.status === 'B REVIEWING') {
    room.status = 'B BUILDING';
    room.last_note = `@${actor} accepted! Now selecting 1 asset worth ${room.offer_a.total_vc.toFixed(2)} VC.`;
    broadcastP2PEvent(room);
    return res.json({ message: 'Offer accepted! Now select your single asset to match.', room });
  }

  // Step 6 & 7: Maker accepts Taker's counter-offer -> ATOMIC 1-ROW SWAP SETTLEMENT (ordered TRD0000001)
  if (!(isMaker && room.status === 'A REVIEWING')) {
    return res.status(400).json({ error: 'Not ready for final settlement.' });
  }

  let conn;
  try {
    conn = await pool.getConnection();
    await conn.beginTransaction();

    const makerId = room.maker.user_id;
    const takerId = room.taker.user_id;
    const offA = room.offer_a;
    const offB = room.offer_b;

    const [mRows] = await conn.execute(
      'SELECT portfolio_id, total_fraction_owned, total_amount_invested, average_purchase_price FROM portfolio WHERE user_id = ? AND asset_id = ? FOR UPDATE',
      [makerId, offA.asset_id]
    );
    if (mRows.length === 0 || Number(mRows[0].total_fraction_owned) < offA.qty - 1e-12) {
      await conn.rollback();
      return res.status(400).json({ error: `@${room.maker.username} no longer holds enough ${offA.symbol}.` });
    }

    const [tRows] = await conn.execute(
      'SELECT portfolio_id, total_fraction_owned, total_amount_invested, average_purchase_price FROM portfolio WHERE user_id = ? AND asset_id = ? FOR UPDATE',
      [takerId, offB.asset_id]
    );
    if (tRows.length === 0 || Number(tRows[0].total_fraction_owned) < offB.qty - 1e-12) {
      await conn.rollback();
      return res.status(400).json({ error: `@${room.taker.username} no longer holds enough ${offB.symbol}.` });
    }

    async function debitPortfolio(row, uid, qtyToDeduct) {
      const owned = Number(row.total_fraction_owned);
      const actual = Math.min(qtyToDeduct, owned);
      const ratio = owned > 0 ? actual / owned : 0;
      const remQty = Math.max(0, owned - actual);
      const remInv = remQty < 1e-12 ? 0 : Number((Number(row.total_amount_invested) * (1 - ratio)).toFixed(2));
      const remAvg = remQty < 1e-12 ? 0 : Number(row.average_purchase_price);

      await conn.execute(
        'UPDATE portfolio SET total_fraction_owned = ?, total_amount_invested = ?, average_purchase_price = ? WHERE user_id = ? AND portfolio_id = ?',
        [remQty < 1e-12 ? 0 : remQty, remInv, remAvg, uid, row.portfolio_id]
      );
      return actual;
    }

    async function creditPortfolio(uid, assetId, qtyToAdd, unitPrice, notional) {
      const [existing] = await conn.execute(
        'SELECT portfolio_id, total_fraction_owned, total_amount_invested FROM portfolio WHERE user_id = ? AND asset_id = ? FOR UPDATE',
        [uid, assetId]
      );
      if (existing.length > 0) {
        const p = existing[0];
        const nQty = Number(p.total_fraction_owned) + qtyToAdd;
        const nInv = Number((Number(p.total_amount_invested) + notional).toFixed(2));
        const nAvg = nQty > 0 ? Number((nInv / nQty).toFixed(4)) : unitPrice;
        await conn.execute(
          'UPDATE portfolio SET total_fraction_owned = ?, total_amount_invested = ?, average_purchase_price = ? WHERE user_id = ? AND portfolio_id = ?',
          [nQty, nInv, nAvg, uid, p.portfolio_id]
        );
      } else {
        const [[{ nextId }]] = await conn.execute(
          'SELECT COALESCE(MAX(portfolio_id), 0) + 1 AS nextId FROM portfolio WHERE user_id = ?',
          [uid]
        );
        await conn.execute(
          'INSERT INTO portfolio (user_id, portfolio_id, asset_id, total_fraction_owned, total_amount_invested, average_purchase_price) VALUES (?, ?, ?, ?, ?, ?)',
          [uid, nextId, assetId, qtyToAdd, notional, unitPrice]
        );
      }
    }

    const actualMakerGave = await debitPortfolio(mRows[0], makerId, offA.qty);
    const actualTakerGave = await debitPortfolio(tRows[0], takerId, offB.qty);

    await creditPortfolio(takerId, offA.asset_id, actualMakerGave, offA.unit_price, offA.total_vc);
    await creditPortfolio(makerId, offB.asset_id, actualTakerGave, offB.unit_price, offB.total_vc);

    const tradeId = await nextSequentialId(conn, 'trade', 'trade_id', 'TRD');

    await conn.execute(
      `INSERT INTO trade (trade_id, buyer_user_id, seller_user_id, buyer_asset_id, seller_asset_id, buyer_asset_fraction, seller_asset_fraction, unit_price, total_amount, trade_status)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'COMPLETED')`,
      [tradeId, makerId, takerId, offB.asset_id, offA.asset_id, actualTakerGave, actualMakerGave, offB.unit_price, offA.total_vc]
    );

    // Log P2P asset swap in both users' wallet_history so it appears on Wallets ledger
    const [[mWallet]] = await conn.execute('SELECT wallet_id, balance FROM wallets WHERE user_id = ?', [makerId]);
    const [[tWallet]] = await conn.execute('SELECT wallet_id, balance FROM wallets WHERE user_id = ?', [takerId]);

    if (mWallet) {
      await conn.execute(
        `INSERT INTO wallet_history (wallet_id, transaction_type, transacted_amount, balance_after, description)
         VALUES (?, 'TRANSFER_OUT', 0.00, ?, ?)`,
        [mWallet.wallet_id, Number(mWallet.balance), `Swap @${room.taker.username.split('@')[0]}: ${Number(actualMakerGave.toFixed(4))} ${offA.symbol} ⇄ ${Number(actualTakerGave.toFixed(4))} ${offB.symbol} (${offA.total_vc.toFixed(2)} VC)`]
      );
    }
    if (tWallet) {
      await conn.execute(
        `INSERT INTO wallet_history (wallet_id, transaction_type, transacted_amount, balance_after, description)
         VALUES (?, 'TRANSFER_IN', 0.00, ?, ?)`,
        [tWallet.wallet_id, Number(tWallet.balance), `Swap @${room.maker.username.split('@')[0]}: ${Number(actualTakerGave.toFixed(4))} ${offB.symbol} ⇄ ${Number(actualMakerGave.toFixed(4))} ${offA.symbol} (${offA.total_vc.toFixed(2)} VC)`]
      );
    }

    await conn.commit();

    room.status = 'SETTLED';
    room.last_note = `Swap settled (${tradeId}): ${actualMakerGave.toFixed(6)} ${offA.symbol} ⇄ ${actualTakerGave.toFixed(6)} ${offB.symbol}`;
    broadcastP2PEvent(room);
    p2pRooms.delete(roomId);

    res.json({ trade_id: tradeId, message: 'Single-asset P2P swap settled!', room });
  } catch (err) {
    if (conn) await conn.rollback();
    console.error('P2P settle error:', err);
    res.status(500).json({ error: 'Swap settlement failed.' });
  } finally {
    if (conn) conn.release();
  }
});

// API: Full portfolio holdings, valuation summary, and 7D series
app.get('/api/portfolio/:userId', async (req, res) => {
  const userId = parsePrefixedId(req.params.userId, 'USR');
  if (!userId) return res.status(400).json({ error: 'Invalid user ID.' });

  try {
    const [[walletRow]] = await pool.query(
      'SELECT balance FROM wallets WHERE user_id = ? LIMIT 1',
      [userId]
    );

    const [rows] = await pool.query(
      `SELECT asset_id, asset_name, symbol, asset_type, risk,
              total_fraction_owned, total_amount_invested,
              average_purchase_price, current_price
       FROM portfolio_valuation_view
       WHERE user_id = ?
       ORDER BY total_amount_invested DESC`,
      [userId]
    );

    let totalInvested = 0;
    let totalValue = 0;

    const holdings = rows.map((r) => {
      const live = liveState[r.symbol];
      const unitPrice = live && live.price > 0 ? live.price : Number(r.current_price);
      const qty = Number(r.total_fraction_owned);
      const invested = Number(r.total_amount_invested);
      const avgPrice = Number(r.average_purchase_price);
      const marketValue = qty * unitPrice;
      const pl = marketValue - invested;
      const returnPct = invested > 0 ? (pl / invested) * 100 : 0;

      totalInvested += invested;
      totalValue += marketValue;

      return {
        asset_id: r.asset_id,
        symbol: r.symbol,
        name: r.asset_name,
        asset_type: r.asset_type,
        risk: live ? live.risk : r.risk,
        qty,
        avg_price: avgPrice,
        unit_price: unitPrice,
        invested,
        market_value: marketValue,
        pl,
        return_pct: returnPct
      };
    });

    const [lbRows] = await pool.query(
      `SELECT snapshot_date, balance
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
    } else {
      const netWorth = Number(walletRow?.balance || 0) + totalValue;
      for (let i = 6; i >= 0; i--) {
        const d = new Date(Date.now() - i * 86400000);
        series.push({
          label: d.toLocaleDateString('en-IN', { day: '2-digit', month: 'short' }),
          value: i === 0 ? Number(netWorth.toFixed(2)) : 10000.00
        });
      }
    }

    res.json({
      wallet_balance: Number(walletRow?.balance || 0),
      total_invested: totalInvested,
      total_value: totalValue,
      total_pl: totalValue - totalInvested,
      holdings,
      series
    });
  } catch (err) {
    console.error('Portfolio load error:', err);
    res.status(500).json({ error: 'Failed to load portfolio.' });
  }
});

// API: Wallets, Central Credit Pool & Immutable Ledger Overview
app.get('/api/wallets/:userId/overview', async (req, res) => {
  const userId = parsePrefixedId(req.params.userId, 'USR');
  if (!userId) return res.status(400).json({ error: 'Invalid user ID.' });

  try {
    const [[wallet]] = await pool.query(
      'SELECT wallet_id, balance, last_updated FROM wallets WHERE user_id = ? LIMIT 1',
      [userId]
    );
    if (!wallet) return res.status(404).json({ error: 'Wallet not found.' });

    // Portfolio valuation for 50% LTV borrow capacity
    const [portRows] = await pool.query(
      `SELECT symbol, total_fraction_owned, total_amount_invested, current_price
       FROM portfolio_valuation_view
       WHERE user_id = ?`,
      [userId]
    );

    let portfolioValue = 0;
    let totalInvested = 0;
    for (const r of portRows) {
      const live = liveState[r.symbol];
      const px = live && live.price > 0 ? live.price : Number(r.current_price);
      portfolioValue += Number(r.total_fraction_owned) * px;
      totalInvested += Number(r.total_amount_invested);
    }

    // Total central pool liquidity available to borrow
    const [[poolRow]] = await pool.query(
      `SELECT COALESCE(SUM(available_amount), 0) AS pool_liquidity
       FROM lending
       WHERE lending_status IN ('OPEN', 'PARTIALLY_FUNDED')`
    );

    // User's active lending positions
    const [myLends] = await pool.query(
      `SELECT lending_id, principal_amount, available_amount, interest_rate, term_days, lending_status, created_at
       FROM lending
       WHERE lender_id = ? AND lending_status IN ('OPEN', 'PARTIALLY_FUNDED', 'FULLY_FUNDED')
       ORDER BY created_at DESC`,
      [userId]
    );

    // User's active loans
    const [myLoans] = await pool.query(
      `SELECT loan_id, lender_id, lending_id, principal_amount, interest_rate, term_days,
              total_repayment, amount_paid, loan_status, start_date, due_date, created_at
       FROM loans
       WHERE borrower_id = ? AND loan_status IN ('APPROVED', 'ACTIVE')
       ORDER BY created_at DESC`,
      [userId]
    );

    const activeDebt = myLoans.reduce(
      (sum, l) => sum + Math.max(0, Number(l.principal_amount) - Number(l.amount_paid)),
      0
    );
    const activeLent = myLends.reduce((sum, l) => sum + Number(l.principal_amount), 0);
    const borrowCapacity = Math.max(0, portfolioValue * 0.5 - activeDebt);

    // Immutable wallet history
    const [history] = await pool.query(
      `SELECT timestamp, transaction_type, transacted_amount, balance_after, description
       FROM wallet_history
       WHERE wallet_id = ?
       ORDER BY timestamp DESC
       LIMIT 100`,
      [wallet.wallet_id]
    );

    res.json({
      wallet_id: wallet.wallet_id,
      balance: Number(wallet.balance),
      last_updated: wallet.last_updated,
      total_invested: Number(totalInvested.toFixed(2)),
      portfolio_value: Number(portfolioValue.toFixed(2)),
      pool_liquidity: Number(poolRow.pool_liquidity || 0),
      active_debt: Number(activeDebt.toFixed(2)),
      active_lent: Number(activeLent.toFixed(2)),
      borrow_capacity: Number(borrowCapacity.toFixed(2)),
      lends: myLends.map((l) => ({
        lending_id: l.lending_id,
        principal_amount: Number(l.principal_amount),
        available_amount: Number(l.available_amount),
        interest_rate: Number(l.interest_rate),
        term_days: l.term_days,
        lending_status: l.lending_status,
        created_at: l.created_at
      })),
      loans: myLoans.map((l) => ({
        loan_id: l.loan_id,
        principal_amount: Number(l.principal_amount),
        interest_rate: Number(l.interest_rate),
        term_days: l.term_days,
        total_repayment: Number(l.total_repayment),
        amount_paid: Number(l.amount_paid),
        loan_status: l.loan_status,
        start_date: l.start_date,
        due_date: l.due_date,
        created_at: l.created_at
      })),
      history: history.map((h) => ({
        timestamp: h.timestamp,
        transaction_type: h.transaction_type,
        transacted_amount: Number(h.transacted_amount),
        balance_after: h.balance_after !== null ? Number(h.balance_after) : null,
        description: h.description
      }))
    });
  } catch (err) {
    console.error('Wallets overview error:', err);
    res.status(500).json({ error: 'Failed to load wallet overview.' });
  }
});

// POST /api/transfers/cash — ACID Peer-to-Peer VC Transfer
app.post('/api/transfers/cash', async (req, res) => {
  const senderId = parsePrefixedId(req.body?.sender_id, 'USR');
  const recipientUsername = (req.body?.recipient_username || '').trim();
  const amount = Number(req.body?.amount);
  const note = (req.body?.note || '').trim();

  if (!senderId || !recipientUsername || !Number.isFinite(amount) || amount <= 0) {
    return res.status(400).json({ error: 'Provide recipient username and a valid VC amount.' });
  }

  const cleanAmt = Number(amount.toFixed(2));
  if (cleanAmt <= 0) return res.status(400).json({ error: 'Amount must be at least 0.01 VC.' });

  let conn;
  try {
    conn = await pool.getConnection();
    await conn.beginTransaction();

    const [senderRows] = await conn.execute(
      'SELECT user_id, username FROM users WHERE user_id = ? LIMIT 1',
      [senderId]
    );
    const [recRows] = await conn.execute(
      'SELECT user_id, username, account_status FROM users WHERE username = ? LIMIT 1',
      [recipientUsername]
    );

    if (senderRows.length === 0 || recRows.length === 0) {
      await conn.rollback();
      return res.status(404).json({ error: `User "@${recipientUsername}" not found.` });
    }
    const sender = senderRows[0];
    const recipient = recRows[0];

    if (recipient.user_id === senderId) {
      await conn.rollback();
      return res.status(400).json({ error: 'Cannot transfer VC to yourself.' });
    }
    if (recipient.account_status !== 'ACTIVE') {
      await conn.rollback();
      return res.status(403).json({ error: 'Recipient account is not active.' });
    }

    // Lock both wallets in deterministic lexicographical user_id order to prevent deadlocks
    const [firstId, secondId] = [senderId, recipient.user_id].sort((a, b) => a.localeCompare(b));
    const [walletRows] = await conn.execute(
      'SELECT wallet_id, user_id, balance FROM wallets WHERE user_id IN (?, ?) ORDER BY user_id ASC FOR UPDATE',
      [firstId, secondId]
    );

    const senderWallet = walletRows.find((w) => w.user_id === senderId);
    const recWallet = walletRows.find((w) => w.user_id === recipient.user_id);

    if (!senderWallet || !recWallet) {
      await conn.rollback();
      return res.status(404).json({ error: 'Wallet record missing.' });
    }
    if (Number(senderWallet.balance) < cleanAmt) {
      await conn.rollback();
      return res.status(400).json({ error: 'Insufficient wallet balance.' });
    }

    const newSenderBal = Number((Number(senderWallet.balance) - cleanAmt).toFixed(2));
    const newRecBal = Number((Number(recWallet.balance) + cleanAmt).toFixed(2));

    await conn.execute('UPDATE wallets SET balance = ? WHERE wallet_id = ?', [newSenderBal, senderWallet.wallet_id]);
    await conn.execute('UPDATE wallets SET balance = ? WHERE wallet_id = ?', [newRecBal, recWallet.wallet_id]);

    const suffix = note ? ` (${note})` : '';
    await conn.execute(
      `INSERT INTO wallet_history (wallet_id, transaction_type, transacted_amount, balance_after, description)
       VALUES (?, 'TRANSFER_OUT', ?, ?, ?)`,
      [senderWallet.wallet_id, cleanAmt, newSenderBal, `Sent ${cleanAmt.toFixed(2)} VC to @${recipient.username}${suffix}`]
    );
    await conn.execute(
      `INSERT INTO wallet_history (wallet_id, transaction_type, transacted_amount, balance_after, description)
       VALUES (?, 'TRANSFER_IN', ?, ?, ?)`,
      [recWallet.wallet_id, cleanAmt, newRecBal, `Received ${cleanAmt.toFixed(2)} VC from @${sender.username}${suffix}`]
    );

    await conn.commit();
    res.json({
      message: `Transferred ${cleanAmt.toFixed(2)} VC to @${recipient.username}`,
      new_balance: newSenderBal
    });
  } catch (err) {
    if (conn) await conn.rollback();
    console.error('Cash transfer error:', err);
    res.status(500).json({ error: 'Cash transfer failed.' });
  } finally {
    if (conn) conn.release();
  }
});

// POST /api/pool/lend — Supply idle VC to Central Credit Pool (6% p.a., random LNDNNNNNNN)
app.post('/api/pool/lend', async (req, res) => {
  const userId = parsePrefixedId(req.body?.user_id, 'USR');
  const amount = Number(req.body?.amount);
  if (!userId || !Number.isFinite(amount) || amount <= 0) {
    return res.status(400).json({ error: 'Enter a valid amount to lend.' });
  }

  const cleanAmt = Number(amount.toFixed(2));
  let conn;
  try {
    conn = await pool.getConnection();
    await conn.beginTransaction();

    const [wRows] = await conn.execute(
      'SELECT wallet_id, balance FROM wallets WHERE user_id = ? FOR UPDATE',
      [userId]
    );
    if (wRows.length === 0 || Number(wRows[0].balance) < cleanAmt) {
      await conn.rollback();
      return res.status(400).json({ error: 'Insufficient wallet balance.' });
    }

    const wallet = wRows[0];
    const newBal = Number((Number(wallet.balance) - cleanAmt).toFixed(2));
    const lendingId = await generateUniqueRandomId(conn, 'lending', 'lending_id', 'LND');

    await conn.execute('UPDATE wallets SET balance = ? WHERE wallet_id = ?', [newBal, wallet.wallet_id]);
    await conn.execute(
      `INSERT INTO lending (lending_id, lender_id, principal_amount, available_amount, interest_rate, term_days, lending_status, description)
       VALUES (?, ?, ?, ?, 6.000, 30, 'OPEN', 'Central Credit Pool Supply')`,
      [lendingId, userId, cleanAmt, cleanAmt]
    );
    await conn.execute(
      `INSERT INTO wallet_history (wallet_id, transaction_type, transacted_amount, balance_after, description)
       VALUES (?, 'LENDING', ?, ?, ?)`,
      [wallet.wallet_id, cleanAmt, newBal, `Supplied ${cleanAmt.toFixed(2)} VC to Central Credit Pool @ 6% p.a.`]
    );

    await conn.commit();
    res.json({
      lending_id: lendingId,
      message: `Supplied ${cleanAmt.toFixed(2)} VC to Central Credit Pool at 6% p.a.`,
      new_balance: newBal
    });
  } catch (err) {
    if (conn) await conn.rollback();
    console.error('Pool lend error:', err);
    res.status(500).json({ error: 'Failed to supply liquidity.' });
  } finally {
    if (conn) conn.release();
  }
});

// POST /api/pool/redeem — Redeem unborrowed pool liquidity + accrued interest
app.post('/api/pool/redeem', async (req, res) => {
  const userId = parsePrefixedId(req.body?.user_id, 'USR');
  const lendingId = parsePrefixedId(req.body?.lending_id, 'LND');
  if (!userId || !lendingId) return res.status(400).json({ error: 'Invalid redeem request.' });

  let conn;
  try {
    conn = await pool.getConnection();
    await conn.beginTransaction();

    const [lRows] = await conn.execute(
      'SELECT lending_id, lender_id, principal_amount, available_amount, interest_rate, lending_status, created_at FROM lending WHERE lending_id = ? FOR UPDATE',
      [lendingId]
    );
    if (lRows.length === 0 || lRows[0].lender_id !== userId) {
      await conn.rollback();
      return res.status(404).json({ error: 'Lending position not found.' });
    }

    const lend = lRows[0];
    const avail = Number(lend.available_amount);
    if (avail <= 0 || !['OPEN', 'PARTIALLY_FUNDED'].includes(lend.lending_status)) {
      await conn.rollback();
      return res.status(400).json({ error: 'Funds are currently borrowed and awaiting repayment.' });
    }

    const days = Math.max(0, (Date.now() - new Date(lend.created_at).getTime()) / 86400000);
    const interest = avail * (Number(lend.interest_rate) / 100) * (days / 365);
    const payout = Number((avail + interest).toFixed(2));

    const [wRows] = await conn.execute('SELECT wallet_id, balance FROM wallets WHERE user_id = ? FOR UPDATE', [userId]);
    const wallet = wRows[0];
    const newBal = Number((Number(wallet.balance) + payout).toFixed(2));

    const [[activeSlice]] = await conn.execute(
      `SELECT COUNT(*) AS cnt FROM loans WHERE lending_id = ? AND loan_status IN ('APPROVED', 'ACTIVE')`,
      [lendingId]
    );
    const nextStatus = Number(activeSlice.cnt) > 0 ? 'FULLY_FUNDED' : 'CLOSED';
    await conn.execute('UPDATE lending SET available_amount = 0, lending_status = ? WHERE lending_id = ?', [nextStatus, lendingId]);
    await conn.execute('UPDATE wallets SET balance = ? WHERE wallet_id = ?', [newBal, wallet.wallet_id]);
    await conn.execute(
      `INSERT INTO wallet_history (wallet_id, transaction_type, transacted_amount, balance_after, description)
       VALUES (?, 'DEPOSIT', ?, ?, ?)`,
      [wallet.wallet_id, payout, newBal, `Redeemed ${payout.toFixed(2)} VC from Credit Pool (incl. interest)`]
    );

    await conn.commit();
    res.json({ message: `Redeemed ${payout.toFixed(2)} VC from Central Credit Pool`, new_balance: newBal });
  } catch (err) {
    if (conn) await conn.rollback();
    console.error('Pool redeem error:', err);
    res.status(500).json({ error: 'Failed to redeem liquidity.' });
  } finally {
    if (conn) conn.release();
  }
});

// POST /api/pool/borrow — Borrow from Central Credit Pool against 50% Portfolio LTV (12% p.a., random LONNNNNNNN)
app.post('/api/pool/borrow', async (req, res) => {
  const userId = parsePrefixedId(req.body?.user_id, 'USR');
  const amount = Number(req.body?.amount);
  if (!userId || !Number.isFinite(amount) || amount <= 0) {
    return res.status(400).json({ error: 'Enter a valid amount to borrow.' });
  }

  const reqAmt = Number(amount.toFixed(2));
  let conn;
  try {
    conn = await pool.getConnection();
    await conn.beginTransaction();

    // 1. Check 50% LTV limit against live portfolio valuation
    const [portRows] = await conn.execute(
      'SELECT symbol, total_fraction_owned, current_price FROM portfolio_valuation_view WHERE user_id = ?',
      [userId]
    );
    let portfolioValue = 0;
    for (const r of portRows) {
      const live = liveState[r.symbol];
      const px = live && live.price > 0 ? live.price : Number(r.current_price);
      portfolioValue += Number(r.total_fraction_owned) * px;
    }

    const [[debtRow]] = await conn.execute(
      `SELECT COALESCE(SUM(principal_amount - amount_paid), 0) AS active_debt
       FROM loans
       WHERE borrower_id = ? AND loan_status IN ('APPROVED', 'ACTIVE')`,
      [userId]
    );
    const maxCap = Math.max(0, portfolioValue * 0.5 - Number(debtRow.active_debt || 0));
    if (reqAmt > maxCap + 0.01) {
      await conn.rollback();
      return res.status(400).json({ error: `Exceeds 50% portfolio LTV limit. Max borrow: ${maxCap.toFixed(2)} VC.` });
    }

    // 2. Lock open pool offers (excluding borrower's own offers) in FIFO order
    const [offers] = await conn.execute(
      `SELECT lending_id, lender_id, available_amount, term_days
       FROM lending
       WHERE lending_status IN ('OPEN', 'PARTIALLY_FUNDED')
         AND available_amount > 0
         AND (lender_id <> ? OR description = 'Protocol Reserve Spread')
       ORDER BY created_at ASC
       FOR UPDATE`,
      [userId]
    );

    const totalAvail = offers.reduce((s, o) => s + Number(o.available_amount), 0);
    if (totalAvail < reqAmt - 0.01) {
      await conn.rollback();
      return res.status(400).json({ error: `Insufficient external pool liquidity (${totalAvail.toFixed(2)} VC available).` });
    }

    let remaining = reqAmt;
    for (const offer of offers) {
      if (remaining <= 0.001) break;
      const take = Math.min(remaining, Number(offer.available_amount));
      const newAvail = Number((Number(offer.available_amount) - take).toFixed(2));
      const newStatus = newAvail <= 0 ? 'FULLY_FUNDED' : 'PARTIALLY_FUNDED';

      await conn.execute(
        'UPDATE lending SET available_amount = ?, lending_status = ? WHERE lending_id = ?',
        [newAvail, newStatus, offer.lending_id]
      );

      const termDays = offer.term_days || 30;
      const rate = 12.0;
      const totalRepay = Number((take * (1 + (rate / 100) * (termDays / 365))).toFixed(2));
      const loanId = await generateUniqueRandomId(conn, 'loans', 'loan_id', 'LON');

      await conn.execute(
        `INSERT INTO loans (loan_id, borrower_id, lender_id, lending_id, principal_amount, interest_rate, term_days, total_repayment, amount_paid, loan_status, start_date, due_date)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0.00, 'ACTIVE', CURDATE(), DATE_ADD(CURDATE(), INTERVAL ? DAY))`,
        [loanId, userId, offer.lender_id, offer.lending_id, take, rate, termDays, totalRepay, termDays]
      );

      remaining = Number((remaining - take).toFixed(2));
    }

    // 3. Credit borrower's wallet
    const [wRows] = await conn.execute('SELECT wallet_id, balance FROM wallets WHERE user_id = ? FOR UPDATE', [userId]);
    const wallet = wRows[0];
    const newBal = Number((Number(wallet.balance) + reqAmt).toFixed(2));

    await conn.execute('UPDATE wallets SET balance = ? WHERE wallet_id = ?', [newBal, wallet.wallet_id]);
    await conn.execute(
      `INSERT INTO wallet_history (wallet_id, transaction_type, transacted_amount, balance_after, description)
       VALUES (?, 'LOAN_DISBURSEMENT', ?, ?, ?)`,
      [wallet.wallet_id, reqAmt, newBal, `Borrowed ${reqAmt.toFixed(2)} VC from Central Credit Pool @ 12% p.a.`]
    );

    await conn.commit();
    res.json({ message: `Loan approved: ${reqAmt.toFixed(2)} VC disbursed at 12% p.a.`, new_balance: newBal });
  } catch (err) {
    if (conn) await conn.rollback();
    console.error('Pool borrow error:', err);
    res.status(500).json({ error: 'Loan request failed.' });
  } finally {
    if (conn) conn.release();
  }
});

// POST /api/pool/repay — Repay loan (12% p.a.): 6% yield to lender, 6% spread retained in Credit Pool reserve
app.post('/api/pool/repay', async (req, res) => {
  const userId = parsePrefixedId(req.body?.user_id, 'USR');
  const loanId = parsePrefixedId(req.body?.loan_id, 'LON');
  if (!userId || !loanId) return res.status(400).json({ error: 'Invalid repayment request.' });

  let conn;
  try {
    conn = await pool.getConnection();
    await conn.beginTransaction();

    const [loanRows] = await conn.execute(
      `SELECT l.loan_id, l.borrower_id, l.lender_id, l.lending_id, l.principal_amount, l.interest_rate,
              l.total_repayment, l.amount_paid, l.loan_status, l.created_at,
              ld.description AS lending_desc
       FROM loans l
       LEFT JOIN lending ld ON l.lending_id = ld.lending_id
       WHERE l.loan_id = ?
       FOR UPDATE`,
      [loanId]
    );
    if (loanRows.length === 0 || loanRows[0].borrower_id !== userId) {
      await conn.rollback();
      return res.status(404).json({ error: 'Active loan not found.' });
    }

    const loan = loanRows[0];
    if (!['APPROVED', 'ACTIVE'].includes(loan.loan_status)) {
      await conn.rollback();
      return res.status(400).json({ error: 'Loan is already settled.' });
    }

    const days = Math.max(0, (Date.now() - new Date(loan.created_at).getTime()) / 86400000);
    const principal = Number(loan.principal_amount);
    const borrowRate = Number(loan.interest_rate); // 12% p.a.
    const supplyRate = 6.0;                        // 6% p.a. lender yield

    // Total payoff due from borrower (principal + 12% p.a. pro-rated)
    const accruedTotal = Number((principal * (1 + (borrowRate / 100) * (days / 365)) - Number(loan.amount_paid)).toFixed(2));
    const totalDue = Math.max(0.01, Math.min(accruedTotal, Number((Number(loan.total_repayment) - Number(loan.amount_paid)).toFixed(2))));

    // Split into Lender payout (principal + 6% p.a.) and Collective Pool Reserve spread (remaining 6% p.a.)
    const lenderShareRaw = Number((principal * (1 + (supplyRate / 100) * (days / 365)) - Number(loan.amount_paid)).toFixed(2));
    const lenderPayout = Math.max(0, Math.min(totalDue, lenderShareRaw));
    const poolSpread = Number((totalDue - lenderPayout).toFixed(2));

    const isReserveSlice = loan.lending_desc === 'Protocol Reserve Spread';
    const lenderId = loan.lender_id || null;

    // Lock borrower wallet (and lender wallet if it's a user-supplied slice) in lexicographical user_id order
    const lockIds = (!isReserveSlice && lenderId && lenderId !== userId)
      ? [userId, lenderId].sort((a, b) => a.localeCompare(b))
      : [userId];

    const [wRows] = await conn.execute(
      `SELECT wallet_id, user_id, balance FROM wallets WHERE user_id IN (${lockIds.map(() => '?').join(',')}) ORDER BY user_id ASC FOR UPDATE`,
      lockIds
    );

    const bWallet = wRows.find((w) => w.user_id === userId);
    const lWallet = (!isReserveSlice && lenderId) ? wRows.find((w) => w.user_id === lenderId) : null;

    if (!bWallet || Number(bWallet.balance) < totalDue) {
      await conn.rollback();
      return res.status(400).json({ error: `Insufficient balance. Payoff due: ${totalDue.toFixed(2)} VC.` });
    }

    // 1. Deduct full repayment from borrower's wallet
    const newBorrowerBal = Number((Number(bWallet.balance) - totalDue).toFixed(2));
    await conn.execute('UPDATE wallets SET balance = ? WHERE wallet_id = ?', [newBorrowerBal, bWallet.wallet_id]);
    await conn.execute(
      `INSERT INTO wallet_history (wallet_id, transaction_type, transacted_amount, balance_after, description)
       VALUES (?, 'LOAN_REPAYMENT', ?, ?, ?)`,
      [bWallet.wallet_id, totalDue, newBorrowerBal, `Repaid Loan ${loanId} (${totalDue.toFixed(2)} VC incl. 12% p.a. interest)`]
    );

    // Resolve admin user_id to anchor collective reserve rows in `lending`
    const [[adminUser]] = await conn.execute(
      "SELECT user_id FROM users WHERE role = 'ADMIN' ORDER BY user_id ASC LIMIT 1"
    );
    const reserveHolderId = adminUser ? adminUser.user_id : (lenderId || userId);

    let collectiveReturnAmt = 0;

    if (isReserveSlice) {
      // Loan came from collective pool reserve -> return 100% of principal + 12% interest back to pool
      collectiveReturnAmt = totalDue;
    } else {
      // Loan came from a regular user -> credit principal + 6% yield to lender, keep 6% spread in pool
      if (lWallet && lenderPayout > 0) {
        const newLenderBal = Number((Number(lWallet.balance) + lenderPayout).toFixed(2));
        await conn.execute('UPDATE wallets SET balance = ? WHERE wallet_id = ?', [newLenderBal, lWallet.wallet_id]);
        await conn.execute(
          `INSERT INTO wallet_history (wallet_id, transaction_type, transacted_amount, balance_after, description)
           VALUES (?, 'DEPOSIT', ?, ?, ?)`,
          [lWallet.wallet_id, lenderPayout, newLenderBal, `Credit Pool repayment received (+${lenderPayout.toFixed(2)} VC incl. 6% yield)`]
        );
      }
      collectiveReturnAmt = poolSpread;
    }

    // 2. Deposit the spread (or full reserve payoff) back into Central Credit Pool as collective borrowable liquidity
    if (collectiveReturnAmt > 0) {
      const reserveLendingId = await generateUniqueRandomId(conn, 'lending', 'lending_id', 'LND');
      await conn.execute(
        `INSERT INTO lending (lending_id, lender_id, principal_amount, available_amount, interest_rate, term_days, lending_status, description)
         VALUES (?, ?, ?, ?, 6.000, 30, 'OPEN', 'Protocol Reserve Spread')`,
        [reserveLendingId, reserveHolderId, collectiveReturnAmt, collectiveReturnAmt]
      );
    }

    // 3. Mark loan as REPAID & record payment with random PAYNNNNNNN
    const newPaid = Number((Number(loan.amount_paid) + totalDue).toFixed(2));
    const paymentId = await generateUniqueRandomId(conn, 'loan_payments', 'payment_id', 'PAY');

    await conn.execute(
      "UPDATE loans SET amount_paid = ?, total_repayment = ?, loan_status = 'REPAID' WHERE loan_id = ?",
      [newPaid, newPaid, loanId]
    );
    await conn.execute(
      `INSERT INTO loan_payments (payment_id, loan_id, amount, payment_status)
       VALUES (?, ?, ?, 'COMPLETED')`,
      [paymentId, loanId, totalDue]
    );

    // 4. If the parent lending supply had 0 available_amount and no remaining active loans, mark it CLOSED
    if (loan.lending_id) {
      const [[parentLend]] = await conn.execute(
        'SELECT available_amount FROM lending WHERE lending_id = ?',
        [loan.lending_id]
      );
      const [[remLoans]] = await conn.execute(
        `SELECT COUNT(*) AS cnt FROM loans WHERE lending_id = ? AND loan_status IN ('APPROVED', 'ACTIVE')`,
        [loan.lending_id]
      );
      if (parentLend && Number(parentLend.available_amount) <= 0 && Number(remLoans.cnt) === 0) {
        await conn.execute(
          "UPDATE lending SET lending_status = 'CLOSED' WHERE lending_id = ?",
          [loan.lending_id]
        );
      }
    }

    await conn.commit();
    res.json({
      payment_id: paymentId,
      message: `Repaid ${totalDue.toFixed(2)} VC — Loan ${loanId} settled (${collectiveReturnAmt.toFixed(2)} VC added to Credit Pool reserve)`,
      new_balance: newBorrowerBal
    });
  } catch (err) {
    if (conn) await conn.rollback();
    console.error('Pool repay error:', err);
    res.status(500).json({ error: 'Loan repayment failed.' });
  } finally {
    if (conn) conn.release();
  }
});

// =========================================================
// PROFILE & ACCOUNT SETTINGS ENDPOINTS
// =========================================================

// GET /api/profile/check-username — Live availability check for username edits
app.get('/api/profile/check-username', async (req, res) => {
  const username = (req.query.username || '').trim();
  const excludeId = parsePrefixedId(req.query.exclude, 'USR') || '';

  if (!username || !/^[a-zA-Z0-9_.-]{3,30}$/.test(username)) {
    return res.json({ available: false, reason: '3–30 chars: letters, numbers, _, ., - only.' });
  }

  try {
    const [rows] = await pool.query(
      'SELECT user_id FROM users WHERE LOWER(username) = LOWER(?) AND user_id <> ? LIMIT 1',
      [username, excludeId]
    );
    res.json({
      available: rows.length === 0,
      reason: rows.length === 0 ? 'Username is available.' : 'Username is already taken.'
    });
  } catch (err) {
    res.status(500).json({ available: false, reason: 'Could not verify username.' });
  }
});

// GET /api/profile/:userId — Fetch real user metadata & trading summary
app.get('/api/profile/:userId', async (req, res) => {
  const userId = parsePrefixedId(req.params.userId, 'USR');
  if (!userId) return res.status(400).json({ error: 'Invalid user ID.' });

  try {
    const [uRows] = await pool.query(
      `SELECT u.user_id, u.username, u.first_name, u.middle_name, u.last_name,
              u.email, u.role, u.account_status, u.created_on AS created_at,
              w.wallet_id, w.balance
       FROM users u
       LEFT JOIN wallets w ON u.user_id = w.user_id
       WHERE u.user_id = ? LIMIT 1`,
      [userId]
    );
    if (uRows.length === 0) return res.status(404).json({ error: 'User not found.' });

    const u = uRows[0];
    const [[{ trade_count }]] = await pool.query(
      `SELECT COUNT(*) AS trade_count
       FROM trade
       WHERE (buyer_user_id = ? OR seller_user_id = ?) AND trade_status = 'COMPLETED'`,
      [userId, userId]
    );

    const [portRows] = await pool.query(
      `SELECT symbol, total_fraction_owned, current_price
       FROM portfolio_valuation_view
       WHERE user_id = ?`,
      [userId]
    );

    let portfolioValue = 0;
    for (const r of portRows) {
      const live = liveState[r.symbol];
      const px = live && live.price > 0 ? live.price : Number(r.current_price);
      portfolioValue += Number(r.total_fraction_owned) * px;
    }

    const fullName = [u.first_name, u.middle_name, u.last_name].filter(Boolean).join(' ');

    res.json({
      user: {
        user_id: u.user_id,
        username: u.username,
        first_name: u.first_name,
        middle_name: u.middle_name,
        last_name: u.last_name,
        full_name: fullName,
        email: u.email,
        role: u.role.toLowerCase(),
        account_status: u.account_status,
        created_at: u.created_at,
        wallet_id: u.wallet_id,
        balance: Number(u.balance || 0)
      },
      stats: {
        trades: Number(trade_count || 0),
        holdings_count: portRows.length,
        portfolio_value: Number(portfolioValue.toFixed(2)),
        net_worth: Number((Number(u.balance || 0) + portfolioValue).toFixed(2))
      }
    });
  } catch (err) {
    console.error('Profile fetch error:', err);
    res.status(500).json({ error: 'Failed to load profile.' });
  }
});

// PUT /api/profile/:userId — Update name, username (unique), and email (unique)
app.put('/api/profile/:userId', async (req, res) => {
  const userId = parsePrefixedId(req.params.userId, 'USR');
  const firstName = (req.body?.first_name || '').trim();
  const middleName = (req.body?.middle_name || '').trim() || null;
  const lastName = (req.body?.last_name || '').trim();
  const username = (req.body?.username || '').trim();
  const email = (req.body?.email || '').trim().toLowerCase();

  if (!userId || !firstName || !lastName || !username || !email) {
    return res.status(400).json({ error: 'First name, last name, username, and email are required.' });
  }
  if (!/^[a-zA-Z0-9_.-]{3,30}$/.test(username)) {
    return res.status(400).json({ error: 'Username must be 3–30 characters (letters, numbers, _, ., -).' });
  }
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
    return res.status(400).json({ error: 'Enter a valid email address.' });
  }

  try {
    // Explicit uniqueness check on both username and email excluding current user
    const [dups] = await pool.execute(
      `SELECT user_id, username, email
       FROM users
       WHERE user_id <> ? AND (LOWER(username) = LOWER(?) OR LOWER(email) = LOWER(?))
       LIMIT 1`,
      [userId, username, email]
    );

    if (dups.length > 0) {
      const conflict = dups[0];
      if (conflict.username.toLowerCase() === username.toLowerCase()) {
        return res.status(409).json({ error: `Username "@${username}" is already taken.` });
      }
      return res.status(409).json({ error: `Email "${email}" is already registered to another account.` });
    }

    await pool.execute(
      `UPDATE users
       SET first_name = ?, middle_name = ?, last_name = ?, username = ?, email = ?
       WHERE user_id = ?`,
      [firstName, middleName, lastName, username, email, userId]
    );

    const fullName = [firstName, middleName, lastName].filter(Boolean).join(' ');
    res.json({
      message: 'Profile details updated successfully.',
      first_name: firstName,
      middle_name: middleName,
      last_name: lastName,
      full_name: fullName,
      username,
      email
    });
  } catch (err) {
    if (err.code === 'ER_DUP_ENTRY') {
      return res.status(409).json({ error: 'Username or email is already in use.' });
    }
    console.error('Profile update error:', err);
    res.status(500).json({ error: 'Failed to update profile.' });
  }
});

// PUT /api/profile/:userId/password — Verify current bcrypt hash & update password
app.put('/api/profile/:userId/password', async (req, res) => {
  const userId = parsePrefixedId(req.params.userId, 'USR');
  const currentPassword = req.body?.current_password || '';
  const newPassword = req.body?.new_password || '';

  if (!userId || !currentPassword || newPassword.length < 8) {
    return res.status(400).json({ error: 'Provide current password and a new password (min 8 chars).' });
  }

  try {
    const [rows] = await pool.execute(
      'SELECT password_hash FROM users WHERE user_id = ? LIMIT 1',
      [userId]
    );
    if (rows.length === 0) return res.status(404).json({ error: 'User not found.' });

    const valid = await bcrypt.compare(currentPassword, rows[0].password_hash);
    if (!valid) {
      return res.status(401).json({ error: 'Current password is incorrect.' });
    }

    const newHash = await bcrypt.hash(newPassword, 10);
    await pool.execute(
      'UPDATE users SET password_hash = ? WHERE user_id = ?',
      [newHash, userId]
    );

    res.json({ message: 'Password updated successfully.' });
  } catch (err) {
    console.error('Password change error:', err);
    res.status(500).json({ error: 'Failed to update password.' });
  }
});

app.get('/api/leaderboard', async (req, res) => {
  try {
    const [rows] = await pool.query(`
      SELECT
        u.user_id,
        u.username,
        CONCAT_WS(' ', u.first_name, NULLIF(u.middle_name, ''), u.last_name) AS full_name,
        COALESCE(w.balance, 0) AS wallet_balance,
        COALESCE(p.portfolio_value, 0) AS portfolio_value,
        COALESCE(lnd.active_lent, 0) AS active_lent,
        COALESCE(ln.active_debt, 0) AS active_debt,
        (
          COALESCE(w.balance, 0) +
          COALESCE(p.portfolio_value, 0) +
          COALESCE(lnd.active_lent, 0) -
          COALESCE(ln.active_debt, 0)
        ) AS net_worth
      FROM users u
      LEFT JOIN wallets w ON w.user_id = u.user_id
      LEFT JOIN (
        SELECT
          user_id,
          SUM(current_value) AS portfolio_value
        FROM portfolio_valuation_view
        GROUP BY user_id
      ) p ON p.user_id = u.user_id
      LEFT JOIN (
        SELECT
          lender_id AS user_id,
          SUM(principal_amount) AS active_lent
        FROM lending
        WHERE lending_status IN ('OPEN', 'PARTIALLY_FUNDED', 'FULLY_FUNDED')
        GROUP BY lender_id
      ) lnd ON lnd.user_id = u.user_id
      LEFT JOIN (
        SELECT
          borrower_id AS user_id,
          SUM(GREATEST(0, principal_amount - COALESCE(amount_paid, 0))) AS active_debt
        FROM loans
        WHERE loan_status = 'ACTIVE'
        GROUP BY borrower_id
      ) ln ON ln.user_id = u.user_id
      WHERE u.account_status = 'ACTIVE'
      ORDER BY net_worth DESC
    `);

    res.json({ leaderboard: rows });
  } catch (err) {
    console.error('Leaderboard error:', err);
    res.status(500).json({ error: 'Failed to fetch leaderboard standings.' });
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