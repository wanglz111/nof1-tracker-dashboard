/**
 * /api/trades
 * 币安合约交易记录接口（REST 初始化 + WebSocket 实时更新 + 30分钟同步）
 */

const crypto = require('crypto');
const WebSocket = require('ws');

const BINANCE_FUTURES_URL = 'https://fapi.binance.com';
const BINANCE_STREAM_URL = 'wss://fstream.binance.com/stream';

const API_KEY = process.env.BINANCE_API_KEY;
const SECRET_KEY = process.env.BINANCE_SECRET_KEY;

let tradesCache = [];
let wsClient = null;
let initialized = false;

// ========================
// 签名函数
// ========================
function generateSignature(queryString, secretKey) {
  return crypto.createHmac('sha256', secretKey).update(queryString).digest('hex');
}

// ========================
// 获取历史交易记录（只在启动时 + 每30分钟同步一次）
// ========================
async function fetchTradesFromRest(limit = 500) {
  const timestamp = Date.now();
  const params = new URLSearchParams({
    limit: limit.toString(),
    timestamp: timestamp.toString(),
  });
  const signature = generateSignature(params.toString(), SECRET_KEY);

  const res = await fetch(`${BINANCE_FUTURES_URL}/fapi/v1/userTrades?${params}&signature=${signature}`, {
    headers: { 'X-MBX-APIKEY': API_KEY },
  });

  if (!res.ok) {
    // 如果419，则说明是请求过于频繁，直接返回空数组
    if (res.status === 419) {
      console.warn('⚠️ 请求过于频繁，直接返回空数组');
      tradesCache = [];
      return;
    }
    const err = await res.text();
    throw new Error(`获取历史交易失败: ${err}`);
  }

  const data = await res.json();
  // 按时间倒序排序
  tradesCache = data.sort((a, b) => b.time - a.time);
  console.log(`✅ 已同步历史交易 ${tradesCache.length} 条`);
}

// ========================
// 创建 listenKey
// ========================
async function createListenKey() {
  const res = await fetch(`${BINANCE_FUTURES_URL}/fapi/v1/listenKey`, {
    method: 'POST',
    headers: { 'X-MBX-APIKEY': API_KEY },
  });
  const data = await res.json();
  return data.listenKey;
}

// ========================
// 启动 WebSocket 用户数据流
// ========================
async function startWebSocket() {
  if (wsClient) return;

  const listenKey = await createListenKey();
  const ws = new WebSocket(`${BINANCE_STREAM_URL}?streams=${listenKey}`);
  wsClient = ws;

  ws.on('open', () => console.log('🔗 已连接币安 WebSocket 用户数据流'));

  ws.on('message', (raw) => {
    const msg = JSON.parse(raw);
    if (msg?.data?.e === 'ORDER_TRADE_UPDATE') {
      const o = msg.data.o;
      if (o.s === 'PUMPUSDT') return;

      const newTrade = {
        id: o.t,
        symbol: o.s,
        side: o.S,
        price: parseFloat(o.ap),
        qty: parseFloat(o.q),
        time: o.T,
        status: o.X,
      };

      tradesCache.unshift(newTrade);
      if (tradesCache.length > 1000) tradesCache = tradesCache.slice(0, 1000);
      console.log(`💥 新成交: ${o.s} ${o.S} ${o.q}@${o.ap}`);
    }
  });

  ws.on('close', () => {
    console.warn('⚠️ WebSocket 关闭，5秒后重连...');
    wsClient = null;
    setTimeout(startWebSocket, 5000);
  });

  ws.on('error', (err) => {
    console.error('❌ WebSocket 错误:', err.message);
    ws.close();
  });

  // 每30分钟续期 listenKey
  setInterval(async () => {
    await fetch(`${BINANCE_FUTURES_URL}/fapi/v1/listenKey`, {
      method: 'PUT',
      headers: { 'X-MBX-APIKEY': API_KEY },
    });
    console.log('🔄 已续期 listenKey');
  }, 30 * 60 * 1000);
}

// ========================
// 初始化逻辑（只执行一次）
// ========================
async function ensureInitialized() {
  if (initialized) return;
  if (!API_KEY || !SECRET_KEY) throw new Error('❌ 缺少 Binance API Key 或 Secret');

  // 第一次初始化：获取历史交易
  await fetchTradesFromRest();
  // 开启 WebSocket
  await startWebSocket();
  initialized = true;

  // 每30分钟同步一次 REST 数据，更新缓存
  setInterval(async () => {
    try {
      await fetchTradesFromRest();
      console.log('🔄 已通过 REST 同步缓存交易数据');
    } catch (err) {
      console.error('❌ 每30分钟同步失败:', err.message);
    }
  }, 30 * 60 * 1000);
}

// ========================
// API Handler
// ========================
module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });

  try {
    await ensureInitialized();
    const { limit = 25 } = req.query;
    const data = tradesCache
      .filter((t) => t.symbol !== 'PUMPUSDT')
      .sort((a, b) => b.time - a.time)
      .slice(0, parseInt(limit));

    res.status(200).json(data);
  } catch (err) {
    console.error('❌ /api/trades 错误:', err);
    res.status(500).json({ error: 'Internal server error', message: err.message });
  }
};
