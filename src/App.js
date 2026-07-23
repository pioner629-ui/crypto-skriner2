import React, { useState, useEffect, useMemo, useRef, useCallback } from 'react';
import { createChart, ColorType, CrosshairMode, LineStyle } from 'lightweight-charts';
import {
  RefreshCw, Search, Copy, Check, TrendingUp, ChevronLeft, ChevronRight,
  Clock, ChevronUp, ChevronDown, Grid3x3, LayoutGrid, BarChart3
} from 'lucide-react';

const MAX_CANDLE_COUNT = 500;
const DEFAULT_CANDLE_COUNT = 100;
const CHART_HEIGHT = 240;
const MIN_FETCH_INTERVAL = 2000;
const CONCURRENCY_LIMIT = 15;

// ── Батчинг тикерного WS ──────────────────────
const TICKER_BATCH_MS  = 300;
const KLINE_BATCH_MS   = 400;

// ── Цветовые градации ──────────────────────────
function volColor(v) {
  if (v < 50)  return '#64748b';
  if (v < 100) return '#38bdf8';
  if (v < 300) return '#818cf8';
  if (v < 700) return '#f59e0b';
  return '#f97316';
}
function ntrColor(n) {
  if (n < 0.5) return '#64748b';
  if (n < 1.0) return '#38bdf8';
  if (n < 2.0) return '#a3e635';
  if (n < 3.5) return '#f59e0b';
  return '#f43f5e';
}
function t15Color(t) {
  if (t < 500)   return '#64748b';
  if (t < 2000)  return '#38bdf8';
  if (t < 6000)  return '#a3e635';
  if (t < 15000) return '#f59e0b';
  return '#f43f5e';
}

const storage = {
  get: (key, def) => {
    try {
      const val = localStorage.getItem(`ntr_${key}`);
      if (val === null) return def;
      return JSON.parse(val);
    } catch { return def; }
  },
  set: (key, val) => {
    try { localStorage.setItem(`ntr_${key}`, JSON.stringify(val)); } catch {}
  },
};

async function pMap(items, fn, concurrency = CONCURRENCY_LIMIT) {
  const results = [];
  let index = 0;
  async function worker() {
    while (index < items.length) {
      const i = index++;
      results[i] = await fn(items[i], i);
    }
  }
  const workers = Array.from({ length: Math.min(concurrency, items.length) }, worker);
  await Promise.all(workers);
  return results;
}

async function fetchWithRetry(url, signal, retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      if (signal?.aborted) throw new DOMException('Aborted', 'AbortError');
      const res = await fetch(url, { signal });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return await res.json();
    } catch (err) {
      if (err.name === 'AbortError') throw err;
      if (i === retries - 1) throw err;
      await new Promise(r => setTimeout(r, 1000 * 2 ** i));
    }
  }
}

const validateKline = (k) =>
  Array.isArray(k) && k.length >= 9 &&
  [1, 2, 3, 4, 5].every(i => !isNaN(parseFloat(k[i])));

function calculateATR(klines, period = 14) {
  if (!klines || klines.length < period + 2) return 0;
  const start = klines.length - period;
  let sum = 0;
  for (let i = start; i < klines.length; i++) {
    const high = parseFloat(klines[i][2]);
    const low  = parseFloat(klines[i][3]);
    const prev = parseFloat(klines[i - 1][4]);
    sum += Math.max(high - low, Math.abs(high - prev), Math.abs(low - prev));
  }
  return sum / period;
}

function calculateTrades15min(klines) {
  if (!klines || klines.length < 3) return 0;
  return klines.slice(-3).reduce((s, c) => s + parseInt(c[8] || 0), 0);
}

function calculateChange1h(klines) {
  if (!klines || klines.length < 12) return 0;
  const sl = klines.slice(-12);
  const open = parseFloat(sl[0][1]);
  if (!open) return 0;
  return ((parseFloat(sl[sl.length - 1][4]) - open) / open) * 100;
}

function calculateChange30m(klines) {
  if (!klines || klines.length < 12) return 0;
  const sl = klines.slice(-12);
  const open = parseFloat(sl[0][1]);
  if (!open) return 0;
  return ((parseFloat(sl[sl.length - 1][4]) - open) / open) * 100;
}

class ChartErrorBoundary extends React.Component {
  constructor(props) { super(props); this.state = { hasError: false }; }
  static getDerivedStateFromError() { return { hasError: true }; }
  componentDidCatch(e, info) { console.error('Chart error:', e, info); }
  render() {
    if (this.state.hasError)
      return <div className="w-full h-full bg-[#0c0d0e] flex items-center justify-center text-red-500 text-[10px]">⚠️ Ошибка графика</div>;
    return this.props.children;
  }
}

function useKlineWebSockets(candleCount, setData) {
  const klineWs         = useRef({});
  const reconnectTimers = useRef({});
  const klineBatchRef   = useRef({});
  const klineBatchTimer = useRef(null);

  const connectKlineWS = useCallback((symbol) => {
    if (klineWs.current[symbol]) {
      try { klineWs.current[symbol].close(); } catch {}
    }
    try {
      const ws = new WebSocket(`wss://fstream.binance.com/ws/${symbol.toLowerCase()}@kline_5m`);
      ws.onmessage = (event) => {
        try {
          const { e, k } = JSON.parse(event.data);
          if (e !== 'kline') return;
          const newCandle = [k.t, k.o, k.h, k.l, k.c, k.v, k.T, k.q, k.n, k.V, k.Q, k.B];
          if (!validateKline(newCandle)) return;
          klineBatchRef.current[symbol] = newCandle;
          if (!klineBatchTimer.current) {
            klineBatchTimer.current = setTimeout(() => {
              klineBatchTimer.current = null;
              const batch = klineBatchRef.current;
              klineBatchRef.current = {};
              setData(prev => prev.map(coin => {
                const candle = batch[coin.symbol];
                if (!candle) return coin;
                const klines = [...(coin.klines || [])];
                const last = klines.length - 1;
                if (klines[last]?.[0] === candle[0]) klines[last] = candle;
                else {
                  klines.push(candle);
                  if (klines.length > candleCount) klines.shift();
                }
                const lastTs    = parseInt(klines[klines.length - 1][0]);
                const hourStart = Math.floor(lastTs / 3_600_000) * 3_600_000;
                const h1sl      = klines.filter(c => parseInt(c[0]) >= hourStart);
                const h1h = h1sl.length ? Math.max(...h1sl.map(c => parseFloat(c[2]))) : coin.levels?.h1?.h;
                const h1l = h1sl.length ? Math.min(...h1sl.map(c => parseFloat(c[3]))) : coin.levels?.h1?.l;
                const price = coin.lastPrice;
                return {
                  ...coin,
                  klines,
                  trades15min: calculateTrades15min(klines),
                  change30m:   calculateChange30m(klines),
                  levels: { ...coin.levels, h1: { h: h1h, l: h1l } },
                  dist_h1h: Math.abs(h1h - price) / price,
                  dist_h1l: Math.abs(h1l - price) / price,
                };
              }));
            }, KLINE_BATCH_MS);
          }
        } catch {}
      };
      ws.onerror = () => {};
      ws.onclose = () => {
        reconnectTimers.current[symbol] = setTimeout(() => {
          if (klineWs.current[symbol] !== undefined) connectKlineWS(symbol);
        }, 5000);
      };
      klineWs.current[symbol] = ws;
    } catch (e) {
      console.error(`WS connect failed for ${symbol}:`, e);
    }
  }, [candleCount, setData]);

  const updateVisible = useCallback((symbols) => {
    const next = new Set(symbols);
    Object.keys(klineWs.current).forEach(sym => {
      if (!next.has(sym)) {
        clearTimeout(reconnectTimers.current[sym]);
        delete reconnectTimers.current[sym];
        try { klineWs.current[sym]?.close(); } catch {}
        delete klineWs.current[sym];
      }
    });
    next.forEach(sym => {
      if (!klineWs.current[sym]) connectKlineWS(sym);
    });
  }, [connectKlineWS]);

  useEffect(() => {
    return () => {
      Object.values(reconnectTimers.current).forEach(clearTimeout);
      reconnectTimers.current = {};
      clearTimeout(klineBatchTimer.current);
      klineBatchTimer.current = null;
      klineBatchRef.current   = {};
      Object.keys(klineWs.current).forEach(sym => {
        try { klineWs.current[sym]?.close(); } catch {}
      });
      klineWs.current = {};
    };
  }, []);

  return { updateVisible };
}

function useMarketData(candleCount) {
  const [data, setData]           = useState([]);
  const [loading, setLoading]     = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const lastFetchRef = useRef(0);
  const abortRef     = useRef(null);

  const fetchData = useCallback(async (isInitial = false) => {
    const now = Date.now();
    if (!isInitial && now - lastFetchRef.current < MIN_FETCH_INTERVAL) return;
    lastFetchRef.current = now;

    if (abortRef.current) abortRef.current.abort();
    const controller = new AbortController();
    abortRef.current = controller;
    const { signal } = controller;

    try {
      if (isInitial) setLoading(true); else setRefreshing(true);

      const tickerData = await fetchWithRetry('https://fapi.binance.com/fapi/v1/ticker/24hr', signal);
      const usdtPairs = tickerData
        .filter(t => t.symbol.endsWith('USDT'))
        .map(t => ({ ...t, quoteVolume: parseFloat(t.quoteVolume) }))
        .sort((a, b) => b.quoteVolume - a.quoteVolume)
        .slice(0, 200);

      const results = await pMap(usdtPairs, async (coin) => {
        try {
          const [klines, klines1h, klines4h, klinesDay] = await Promise.all([
            fetchWithRetry(
              `https://fapi.binance.com/fapi/v1/klines?symbol=${coin.symbol}&interval=5m&limit=${candleCount}`,
              signal
            ),
            fetchWithRetry(
              `https://fapi.binance.com/fapi/v1/klines?symbol=${coin.symbol}&interval=1h&limit=1`,
              signal
            ),
            fetchWithRetry(
              `https://fapi.binance.com/fapi/v1/klines?symbol=${coin.symbol}&interval=4h&limit=1`,
              signal
            ),
            fetchWithRetry(
              `https://fapi.binance.com/fapi/v1/klines?symbol=${coin.symbol}&interval=1d&limit=1`,
              signal
            ),
          ]);

          if (!klines?.length) return null;
          const valid = klines.filter(validateKline);
          if (!valid.length) return null;

          const price = parseFloat(coin.lastPrice);

          const cur1h = klines1h?.[0];
          const h1h   = cur1h ? parseFloat(cur1h[2]) : Math.max(...valid.slice(-12).map(k => parseFloat(k[2])));
          const h1l   = cur1h ? parseFloat(cur1h[3]) : Math.min(...valid.slice(-12).map(k => parseFloat(k[3])));

          const cur4h = klines4h?.[0];
          const h4h   = cur4h ? parseFloat(cur4h[2]) : Math.max(...valid.slice(-48).map(k => parseFloat(k[2])));
          const h4l   = cur4h ? parseFloat(cur4h[3]) : Math.min(...valid.slice(-48).map(k => parseFloat(k[3])));

          const curDay = klinesDay?.[0];
          const h24    = curDay ? parseFloat(curDay[2]) : parseFloat(coin.highPrice);
          const l24    = curDay ? parseFloat(curDay[3]) : parseFloat(coin.lowPrice);

          const atr = calculateATR(valid, 14);

          return {
            symbol:       coin.symbol,
            lastPrice:    price,
            quoteVolume:  coin.quoteVolume / 1e6,
            ntr:          (atr / price) * 100,
            change24h:    parseFloat(coin.priceChangePercent),
            change1h:     calculateChange1h(valid),
            change30m:    calculateChange30m(valid),
            klines:       valid,
            trades15min:  calculateTrades15min(valid),
            levels:       { h1: { h: h1h, l: h1l }, h4: { h: h4h, l: h4l }, d24: { h: h24, l: l24 } },
            dist_h1h:  Math.abs(h1h  - price) / price,
            dist_h1l:  Math.abs(h1l  - price) / price,
            dist_h4h:  Math.abs(h4h  - price) / price,
            dist_h4l:  Math.abs(h4l  - price) / price,
            dist_d24h: Math.abs(h24  - price) / price,
            dist_d24l: Math.abs(l24  - price) / price,
          };
        } catch (err) {
          if (err.name === 'AbortError') throw err;
          return null;
        }
      }, CONCURRENCY_LIMIT);

      if (signal.aborted) return;
      const final = results.filter(Boolean);
      if (final.length) setData(final);
    } catch (err) {
      if (err.name !== 'AbortError') console.error('fetchData error:', err);
    } finally {
      if (!signal.aborted) { setLoading(false); setRefreshing(false); }
    }
  }, [candleCount]);

  useEffect(() => {
    fetchData(true);
    const iv = setInterval(() => fetchData(false), 30000);
    return () => {
      clearInterval(iv);
      abortRef.current?.abort();
    };
  }, [fetchData]);

  return { data, setData, loading, refreshing, fetchData };
}

function useTickerWebSocket(setData) {
  const [wsConnected, setWsConnected] = useState(false);
  const wsRef          = useRef(null);
  const reconnectTimer = useRef(null);
  const batchRef       = useRef({});
  const batchTimer     = useRef(null);
  const mountedRef     = useRef(true);
  const setDataRef     = useRef(setData);
  
  useEffect(() => { setDataRef.current = setData; }, [setData]);

  const flushBatch = useCallback(() => {
    batchTimer.current = null;
    if (!mountedRef.current) return;
    const batch = batchRef.current;
    if (Object.keys(batch).length === 0) return;
    batchRef.current = {};

    setDataRef.current(prev => prev.map(coin => {
      const t = batch[coin.symbol];
      if (!t) return coin;
      const price = parseFloat(t.c);
      const lv    = coin.levels;
      if (!lv?.h1 || !lv?.h4 || !lv?.d24) return coin;
      return {
        ...coin,
        lastPrice:    price,
        change24h:    parseFloat(t.P),
        quoteVolume:  parseFloat(t.q) / 1e6,
        dist_h1h:  Math.abs(lv.h1.h - price) / price,
        dist_h1l:  Math.abs(lv.h1.l - price) / price,
        dist_h4h:  Math.abs(lv.h4.h - price) / price,
        dist_h4l:  Math.abs(lv.h4.l - price) / price,
        dist_d24h: Math.abs(lv.d24.h - price) / price,
        dist_d24l: Math.abs(lv.d24.l - price) / price,
      };
    }));
  }, []);

  useEffect(() => {
    mountedRef.current = true;

    const connect = () => {
      if (wsRef.current) try { wsRef.current.close(); } catch {}
      try {
        const ws = new WebSocket('wss://fstream.binance.com/ws/!ticker@arr');
        ws.onopen  = () => setWsConnected(true);
        ws.onerror = () => setWsConnected(false);
        ws.onclose = () => {
          setWsConnected(false);
          reconnectTimer.current = setTimeout(connect, 5000);
        };
        ws.onmessage = (event) => {
          try {
            const tickers = JSON.parse(event.data);
            tickers.forEach(t => { batchRef.current[t.s] = t; });
            if (!batchTimer.current) {
              batchTimer.current = setTimeout(flushBatch, TICKER_BATCH_MS);
            }
          } catch {}
        };
        wsRef.current = ws;
      } catch {}
    };

    connect();

    return () => {
      mountedRef.current = false;
      clearTimeout(reconnectTimer.current);
      clearTimeout(batchTimer.current);
      batchTimer.current = null;
      batchRef.current   = {};
      try { wsRef.current?.close(); } catch {}
    };
  }, [flushBatch]);

  return { wsConnected };
}

function useRuler(outerRef, candleSeriesRef) {
  const [ruler, setRuler] = useState(null);
  const dragRef = useRef(null);
  const autoHideTimer = useRef(null);

  useEffect(() => {
    const el = outerRef.current;
    if (!el) return;

    const getPrice = (clientY) => {
      const series = candleSeriesRef.current;
      if (!series) return null;
      const rect = el.getBoundingClientRect();
      const y = clientY - rect.top;
      try { return series.coordinateToPrice(y); }
      catch { return null; }
    };

    const onMouseDown = (e) => {
      if (e.button !== 1) return;
      e.preventDefault();
      e.stopPropagation();
      clearTimeout(autoHideTimer.current);
      const rect = el.getBoundingClientRect();
      const startY = e.clientY - rect.top;
      const startPrice = getPrice(e.clientY);
      dragRef.current = { startY, startPrice };
      setRuler({ startY, endY: startY, startPrice, endPrice: startPrice, isDragging: true });
    };

    const onMouseMove = (e) => {
      if (!dragRef.current) return;
      const { startY, startPrice } = dragRef.current;
      const rect = el.getBoundingClientRect();
      const endY = e.clientY - rect.top;
      const endPrice = getPrice(e.clientY);
      setRuler({ startY, endY, startPrice, endPrice, isDragging: true });
    };

    const onMouseUp = (e) => {
      if (!dragRef.current) return;
      if (e.button !== 1) return;
      dragRef.current = null;
      setRuler(null);
    };

    const preventAux = (e) => { if (e.button === 1) e.preventDefault(); };

    el.addEventListener('mousedown', onMouseDown);
    el.addEventListener('auxclick', preventAux);
    window.addEventListener('mousemove', onMouseMove);
    window.addEventListener('mouseup', onMouseUp);

    return () => {
      el.removeEventListener('mousedown', onMouseDown);
      el.removeEventListener('auxclick', preventAux);
      window.removeEventListener('mousemove', onMouseMove);
      window.removeEventListener('mouseup', onMouseUp);
      clearTimeout(autoHideTimer.current);
    };
  });

  return ruler;
}

const RulerOverlay = ({ ruler, containerWidth }) => {
  if (!ruler) return null;
  const { startY, endY, startPrice, endPrice, isDragging } = ruler;
  if (startPrice == null || endPrice == null) return null;

  const minY = Math.min(startY, endY);
  const maxY = Math.max(startY, endY);
  const midY = (minY + maxY) / 2;
  const height = maxY - minY;
  const pctChange = startPrice !== 0 ? ((endPrice - startPrice) / startPrice) * 100 : 0;
  const priceDiff = endPrice - startPrice;
  const isPositive = pctChange >= 0;
  const color       = isPositive ? '#26a69a' : '#ef5350';
  const bgColor     = isPositive ? 'rgba(38,166,154,0.12)' : 'rgba(239,83,80,0.12)';
  const borderColor = isPositive ? 'rgba(38,166,154,0.7)'  : 'rgba(239,83,80,0.7)';

  if (height < 2 && isDragging) return (
    <div className="absolute pointer-events-none z-40" style={{ top: startY - 6, left: 0, right: 0, height: 12, display: 'flex', alignItems: 'center' }}>
      <div style={{ position: 'absolute', left: 0, right: 0, top: 6, borderTop: `1px dashed ${color}`, opacity: 0.7 }} />
      <div style={{ position: 'absolute', right: 4, top: -7, fontSize: 7, fontFamily: 'Monaco, monospace', color, background: 'rgba(0,0,0,0.85)', padding: '1px 4px', borderRadius: 2, border: `1px solid ${borderColor}`, whiteSpace: 'nowrap' }}>
        {startPrice?.toFixed(startPrice < 1 ? 4 : 2)}
      </div>
    </div>
  );

  const precision = startPrice < 1 ? 4 : 2;

  return (
    <div className="absolute pointer-events-none z-40" style={{ top: 0, left: 0, right: 0, bottom: 0 }}>
      <div style={{ position: 'absolute', top: minY, left: 0, right: 0, height: Math.max(height, 1), background: bgColor, borderTop: `1px solid ${borderColor}`, borderBottom: `1px solid ${borderColor}`, transition: isDragging ? 'none' : 'opacity 0.5s ease' }} />
      <div style={{ position: 'absolute', top: startY, left: 0, right: 0, borderTop: `1.5px dashed ${color}`, opacity: 0.9 }} />
      {height > 3 && <div style={{ position: 'absolute', top: endY, left: 0, right: 0, borderTop: `1.5px dashed ${color}`, opacity: 0.9 }} />}
      <div style={{ position: 'absolute', top: minY, left: containerWidth / 2, width: 1, height: Math.max(height, 2), background: `linear-gradient(to bottom, transparent, ${color}60, transparent)` }} />
      {height > 20 && Array.from({ length: Math.min(Math.floor(height / 20), 20) }, (_, i) => (
        <div key={i} style={{ position: 'absolute', top: minY + ((i + 1) * height / (Math.floor(height / 20) + 1)), left: containerWidth / 2 - 4, width: 8, height: 1, background: `${color}50` }} />
      ))}
      <div style={{ position: 'absolute', top: startY - 9, right: 4, fontSize: 7, fontFamily: 'Monaco, monospace', color: '#d1d4dc', background: 'rgba(0,0,0,0.88)', padding: '1px 4px', borderRadius: 2, border: `1px solid ${borderColor}`, whiteSpace: 'nowrap' }}>
        {startPrice?.toFixed(precision)}
      </div>
      {height > 12 && (
        <div style={{ position: 'absolute', top: endY + 2, right: 4, fontSize: 7, fontFamily: 'Monaco, monospace', color: '#d1d4dc', background: 'rgba(0,0,0,0.88)', padding: '1px 4px', borderRadius: 2, border: `1px solid ${borderColor}`, whiteSpace: 'nowrap' }}>
          {endPrice?.toFixed(precision)}
        </div>
      )}
      {height > 8 && (
        <div style={{ position: 'absolute', top: midY - 11, left: '50%', transform: 'translateX(-50%)', background: isPositive ? 'rgba(38,166,154,0.95)' : 'rgba(239,83,80,0.95)', color: '#000', fontFamily: 'Monaco, monospace', fontSize: 8, fontWeight: 'bold', padding: '2px 6px', borderRadius: 3, whiteSpace: 'nowrap', boxShadow: `0 0 8px ${color}60`, border: `1px solid ${color}` }}>
          {isPositive ? '▲' : '▼'} {Math.abs(pctChange).toFixed(2)}%
          <span style={{ fontSize: 6, opacity: 0.8, marginLeft: 4 }}>({isPositive ? '+' : ''}{priceDiff.toFixed(precision)})</span>
        </div>
      )}
      {isDragging && height < 5 && (
        <div style={{ position: 'absolute', top: startY + 6, left: '50%', transform: 'translateX(-50%)', fontSize: 6, color: 'rgba(255,255,255,0.3)', fontFamily: 'Monaco, monospace', whiteSpace: 'nowrap', background: 'rgba(0,0,0,0.6)', padding: '1px 4px', borderRadius: 2 }}>
          тяните вверх или вниз
        </div>
      )}
    </div>
  );
};

// ── Глобальный tick ────────────────────────
const TickContext = React.createContext(0);

const TradingViewChart = React.memo(({
  klines, coin, copiedSymbol, onCopy, trades15min, candleCount
}) => {
  const containerRef   = useRef(null);
  const wrapperRef     = useRef(null);
  const chartRef       = useRef(null);
  const candleRef      = useRef(null);
  const volumeRef      = useRef(null);
  const linesRef       = useRef([]);
  const roRef          = useRef(null);
  const rafRef         = useRef(null);
  const [ready, setReady] = useState(false);
  const [error, setError] = useState(false);
  const [containerWidth, setContainerWidth] = useState(0);

  const ruler = useRuler(wrapperRef, candleRef);

  const chartData = useMemo(() => {
    if (!klines?.length) return [];
    return klines.slice(-Math.min(candleCount, MAX_CANDLE_COUNT)).map(k => ({
      time:   Math.floor(parseInt(k[0]) / 1000),
      open:   parseFloat(k[1]), high:  parseFloat(k[2]),
      low:    parseFloat(k[3]), close: parseFloat(k[4]),
      volume: parseFloat(k[7] || 0),
    })).filter(d => d.time && d.open && d.high && d.low && d.close);
  }, [klines, candleCount]);

  const volumeData = useMemo(() =>
    chartData.map(d => ({ time: d.time, value: d.volume, color: d.close >= d.open ? '#26a69a' : '#ef5350' })),
    [chartData]
  );

  useEffect(() => {
    if (!containerRef.current || error) return;
    const init = () => {
      try {
        while (containerRef.current.firstChild) containerRef.current.removeChild(containerRef.current.firstChild);

        const chart = createChart(containerRef.current, {
          width:  containerRef.current.clientWidth,
          height: CHART_HEIGHT,
          layout: { background: { type: ColorType.Solid, color: '#0c0d0e' }, textColor: '#6b7280', fontSize: 8, fontFamily: 'Monaco, monospace' },
          grid: { vertLines: { color: '#1f2937', style: LineStyle.Dashed }, horzLines: { color: '#1f2937', style: LineStyle.Dashed } },
          crosshair: {
            mode: CrosshairMode.Normal,
            vertLine: { color: '#fbbf24', width: 1, style: LineStyle.Solid, labelBackgroundColor: '#fbbf24' },
            horzLine: { color: '#fbbf24', width: 1, style: LineStyle.Solid, labelBackgroundColor: '#fbbf24' },
          },
          rightPriceScale: { borderColor: '#2a2e39', textColor: '#9ca3af', fontSize: 8, scaleMargins: { top: 0.1, bottom: 0.25 } },
          handleScroll: { mouseWheel: true, pressedMouseMove: true },
          handleScale:  { mouseWheel: true, pinch: true },
          timeScale: {
            borderColor: '#2a2e39', timeVisible: true, secondsVisible: false,
            tickMarkFormatter: (t) => { const d = new Date(t*1000); return `${String(d.getHours()).padStart(2,'0')}:${String(d.getMinutes()).padStart(2,'0')}`; },
            fixLeftEdge: true, fixRightEdge: false, rightOffset: 3,
          },
        });

        const precision = coin?.lastPrice < 1 ? 4 : 2;
        const minMove   = coin?.lastPrice < 1 ? 0.0001 : 0.01;

        candleRef.current = chart.addCandlestickSeries({
          upColor: '#26a69a', downColor: '#ef5350', borderVisible: false,
          wickUpColor: '#26a69a', wickDownColor: '#ef5350',
          priceFormat: { type: 'price', precision, minMove },
        });

        volumeRef.current = chart.addHistogramSeries({
          color: '#26a69a', priceFormat: { type: 'volume' }, priceScaleId: 'volume',
          scaleMargins: { top: 0.8, bottom: 0 },
        });
        chart.priceScale('volume').applyOptions({ scaleMargins: { top: 0.8, bottom: 0 }, visible: true, borderVisible: false, textColor: '#6b7280', fontSize: 8 });

        chartRef.current = chart;
        setContainerWidth(containerRef.current.clientWidth);

        roRef.current = new ResizeObserver(entries => {
          cancelAnimationFrame(rafRef.current);
          rafRef.current = requestAnimationFrame(() => {
            for (const e of entries) {
              if (e.target === containerRef.current && chartRef.current) {
                chartRef.current.applyOptions({ width: e.contentRect.width });
                setContainerWidth(e.contentRect.width);
              }
            }
          });
        });
        roRef.current.observe(containerRef.current);
        setReady(true);
      } catch (e) {
        console.error('Chart init error:', e);
        setError(true);
      }
    };

    init();
    return () => {
      cancelAnimationFrame(rafRef.current);
      roRef.current?.disconnect();
      try { chartRef.current?.remove(); } catch {}
      chartRef.current = null; candleRef.current = null; volumeRef.current = null;
      setReady(false);
      prevLastTimeRef.current  = null;
      prevChartLenRef.current  = null;
      isInitializedRef.current = false;
    };
  }, [coin?.symbol, error]);

  const prevLastTimeRef    = useRef(null);
  const prevChartLenRef    = useRef(null);
  const isInitializedRef   = useRef(false);

  useEffect(() => {
    if (!candleRef.current || !volumeRef.current || !chartData.length || error) return;
    try {
      const lastCandle = chartData[chartData.length - 1];
      const lastVolume = volumeData[volumeData.length - 1];
      const prevTime   = prevLastTimeRef.current;

      const isSameDataset =
        isInitializedRef.current &&
        prevTime !== null &&
        Math.abs(chartData.length - (prevChartLenRef.current || 0)) <= 1 &&
        lastCandle.time >= prevTime;

      if (!isSameDataset) {
        candleRef.current.setData(chartData);
        volumeRef.current.setData(volumeData);
        chartRef.current?.timeScale().fitContent();
        chartRef.current?.timeScale().scrollToPosition(3, false);
      } else if (lastCandle.time === prevTime) {
        candleRef.current.update(lastCandle);
        volumeRef.current.update(lastVolume);
      } else {
        candleRef.current.update(lastCandle);
        volumeRef.current.update(lastVolume);
        chartRef.current?.timeScale().scrollToPosition(3, true);
      }

      prevLastTimeRef.current  = lastCandle.time;
      prevChartLenRef.current  = chartData.length;
      isInitializedRef.current = true;
    } catch (e) { console.error('Chart data error:', e); setError(true); }
  }, [chartData, volumeData, error]);

  useEffect(() => {
    if (!chartRef.current || !candleRef.current || !ready || error) return;
    try {
      linesRef.current.forEach(l => { try { candleRef.current?.removePriceLine(l); } catch {} });
      linesRef.current = [];
      const add = (price, color, style, width = 1, label = '') => {
        if (!price || price <= 0) return;
        linesRef.current.push(candleRef.current.createPriceLine({ price, color, lineWidth: width, lineStyle: style, axisLabelVisible: !!label, title: label }));
      };
      add(coin?.levels?.h1?.h, 'rgba(255,255,255,0.35)', LineStyle.Solid, 1);
      add(coin?.levels?.h1?.l, 'rgba(255,255,255,0.35)', LineStyle.Solid, 1);
      add(coin?.levels?.h4?.h, '#f59e0b', LineStyle.Dotted, 1);
      add(coin?.levels?.h4?.l, '#3b82f6', LineStyle.Dotted, 1);
      add(coin?.levels?.d24?.h, '#ef4444', LineStyle.Dashed, 1);
      add(coin?.levels?.d24?.l, '#22c55e', LineStyle.Dashed, 1);
    } catch {}
  }, [ready, coin?.levels, error]);

  const fmt = (v) => (v !== undefined && !isNaN(v)) ? (v * 100).toFixed(2) + '%' : '0.00%';
  const chColor = coin?.change24h >= 0 ? 'text-emerald-500' : 'text-rose-500';
  const ch30Color = (coin?.change30m || 0) >= 0 ? 'text-emerald-400' : 'text-rose-400';

  if (error) return <div className="w-full h-full bg-[#0c0d0e] flex items-center justify-center text-red-500 text-[10px]">⚠️ Ошибка графика</div>;
  if (!klines?.length) return <div className="w-full h-full bg-[#0c0d0e] flex items-center justify-center text-slate-700 text-[10px]">Нет данных</div>;

  return (
    <div className="flex-grow flex flex-col w-full relative overflow-hidden rounded bg-[#0c0d0e]">
      <div className="absolute top-0 left-0 right-0 z-20 bg-gradient-to-b from-black/95 via-black/40 to-transparent pointer-events-none p-1.5">
        <div className="flex justify-between items-start">
          <div className="flex flex-col gap-0.5">
            <div className="pointer-events-auto flex items-center gap-1 flex-wrap">
              <span className="text-[10px] text-white font-bold tracking-tight uppercase leading-none">
                {coin?.symbol?.replace('USDT', '') || '???'}
              </span>
              <button onClick={(e) => { e.stopPropagation(); onCopy(coin?.symbol); }} className="text-slate-500 hover:text-white transition-colors">
                {copiedSymbol === coin?.symbol ? <Check size={7} className="text-emerald-500" /> : <Copy size={7} />}
              </button>
            </div>
            <span className={`${chColor} font-semibold text-[6px] leading-none`}>
              {coin?.change24h > 0 ? '+' : ''}{coin?.change24h?.toFixed(2)}%
            </span>
            <span className={`${ch30Color} font-bold text-[6px] leading-none`}>
              1H: {(coin?.change30m || 0) > 0 ? '+' : ''}{(coin?.change30m || 0).toFixed(2)}%
            </span>
            <div className="flex flex-col mt-0.5 leading-tight gap-0">
              <span className="font-bold uppercase text-[6px] lg:text-[8px]" style={{color: volColor(coin?.quoteVolume||0)}}>V: <span className="text-white">${coin?.quoteVolume?.toFixed(1)}M</span></span>
              <span className="font-bold uppercase text-[6px] lg:text-[8px]" style={{color: ntrColor(coin?.ntr||0)}}>N: <span className="text-white">{coin?.ntr?.toFixed(2)}%</span></span>
              <span className="font-bold uppercase text-[5px] lg:text-[7px] leading-tight" style={{color: t15Color(trades15min||0)}}>T15: <span className="text-white">{trades15min?.toLocaleString()}</span></span>
            </div>
          </div>
          <div className="flex flex-col items-end gap-0.5 opacity-90">
            <div className="flex gap-1 items-center leading-none mt-1">
              <span className="text-[6px] px-0.5 font-mono font-semibold text-emerald-500 bg-emerald-500/10 rounded-[1px]">1H:{fmt(coin?.dist_h1h)}</span>
              <span className="text-[6px] px-0.5 font-mono font-semibold text-emerald-500 bg-emerald-500/10 rounded-[1px]">4H:{fmt(coin?.dist_h4h)}</span>
              <span className="text-[6px] px-0.5 font-mono font-semibold text-emerald-500 bg-emerald-500/10 rounded-[1px]">D:{fmt(coin?.dist_d24h)}</span>
            </div>
            <div className="flex items-center gap-0.5 mt-1">
              <div className="w-1 h-1 rounded-full bg-purple-500 animate-pulse" />
              <span className="text-[5px] text-purple-300 font-bold">{chartData.length} св</span>
            </div>
            <div className="flex items-center gap-0.5 mt-0.5 opacity-40">
              <span className="text-[4px] text-slate-500">📏 ПКМ колёсико</span>
            </div>
          </div>
        </div>
      </div>

      <div ref={wrapperRef} className="relative w-full" style={{ height: CHART_HEIGHT }}>
        <div ref={containerRef} className="w-full h-full" />
        <RulerOverlay ruler={ruler} containerWidth={containerWidth} />
        {ruler?.isDragging && (
          <div className="absolute inset-0 pointer-events-none z-50" style={{ cursor: 'ns-resize' }} />
        )}
      </div>

      <div className="absolute left-1.5 z-10 pointer-events-none opacity-80 bottom-1">
        <div className="flex gap-1 items-center">
          <span className="text-[6px] px-0.5 font-mono font-semibold text-red-400 bg-red-400/10 rounded-[1px]">1H:{fmt(coin?.dist_h1l)}</span>
          <span className="text-[6px] px-0.5 font-mono font-semibold text-red-400 bg-red-400/10 rounded-[1px]">4H:{fmt(coin?.dist_h4l)}</span>
          <span className="text-[6px] px-0.5 font-mono font-semibold text-red-400 bg-red-400/10 rounded-[1px]">D:{fmt(coin?.dist_d24l)}</span>
        </div>
      </div>

      {!ready && chartData.length > 0 && (
        <div className="absolute inset-0 flex items-center justify-center bg-black/50 z-30">
          <div className="w-4 h-4 border border-purple-500/30 border-t-purple-500 rounded-full animate-spin" />
        </div>
      )}
    </div>
  );
});
TradingViewChart.displayName = 'TradingViewChart';

function useGridFade(trigger) {
  const [visible, setVisible] = useState(true);
  const timerRef = useRef(null);
  const prevTrigger = useRef(trigger);

  useEffect(() => {
    if (prevTrigger.current === trigger) return;
    prevTrigger.current = trigger;
    clearTimeout(timerRef.current);
    setVisible(false);
    timerRef.current = setTimeout(() => setVisible(true), 220);
    return () => clearTimeout(timerRef.current);
  }, [trigger]);

  return visible;
}

const App = () => {
  const [sortBy,     setSortBy]     = useState(() => storage.get('sortBy',     'ntr'));
  const [sortOrder,  setSortOrder]  = useState(() => storage.get('sortOrder',  'desc'));
  const [minNtr,     setMinNtr]     = useState(() => storage.get('minNtr',     0.8));
  const [minVol,     setMinVol]     = useState(() => storage.get('minVol',     20));
  const [minTrades,  setMinTrades]  = useState(() => storage.get('minTrades',  100));
  const [minChange1h,setMinChange1h]= useState(() => storage.get('minChange1h', 0));
  const [change1hDir,setChange1hDir]= useState(() => storage.get('change1hDir', 'any'));
  const [candleCount,setCandleCount]= useState(() => Math.min(storage.get('candleCount', DEFAULT_CANDLE_COUNT), MAX_CANDLE_COUNT));
  const [gridMode,   setGridMode]   = useState(() => storage.get('gridMode',   '16grid'));
  const [pageIndex,  setPageIndex]  = useState(0);
  const gridVisible = useGridFade(pageIndex);
  const [panelVisible,setPanelVisible]= useState(() => storage.get('panelVisible', true));

  const [searchInput, setSearchInput] = useState('');
  const [searchTerm,  setSearchTerm]  = useState('');
  const [copiedSymbol,setCopiedSymbol]= useState(null);

  const [tick, setTick] = useState(0);
  useEffect(() => {
    const iv = setInterval(() => setTick(t => t + 1), 1000);
    return () => clearInterval(iv);
  }, []);

  const [isHovered, setIsHovered]   = useState(false);
  const frozenRef  = useRef(null);
  const searchInputRef = useRef(null);

  useEffect(() => {
    const t = setTimeout(() => setSearchTerm(searchInput), 300);
    return () => clearTimeout(t);
  }, [searchInput]);

  useEffect(() => {
    storage.set('sortBy',           sortBy);
    storage.set('sortOrder',        sortOrder);
    storage.set('minNtr',           minNtr);
    storage.set('minVol',           minVol);
    storage.set('minTrades',        minTrades);
    storage.set('minChange1h',      minChange1h);
    storage.set('change1hDir',      change1hDir);
    storage.set('candleCount',      candleCount);
    storage.set('gridMode',         gridMode);
    storage.set('panelVisible',     panelVisible);
  }, [sortBy, sortOrder, minNtr, minVol, minTrades, minChange1h, change1hDir, candleCount, gridMode, panelVisible]);

  const { data, setData, loading, refreshing, fetchData } = useMarketData(candleCount);
  const { wsConnected }  = useTickerWebSocket(setData);

  const { sortedData, totalPages, filteredCount } = useMemo(() => {
    const itemsPerPage = (gridMode === '16grid' ? 16 : 12) - 1;
    const filtered = data.filter(item => {
      if (!item || item.symbol === 'BTCUSDT') return false;
      const c1h = item.change1h || 0;
      let c1hOk = true;
      if (change1hDir === 'up')   c1hOk = c1h >= minChange1h;
      if (change1hDir === 'down') c1hOk = c1h <= -minChange1h;
      return (
        item.symbol.toLowerCase().includes(searchTerm.toLowerCase()) &&
        (item.ntr        || 0) >= minNtr    &&
        (item.quoteVolume|| 0) >= minVol    &&
        (item.trades15min|| 0) >= minTrades &&
        c1hOk
      );
    });
    const sorted = [...filtered].sort((a, b) => {
      const va = a?.[sortBy] || 0, vb = b?.[sortBy] || 0;
      return sortOrder === 'asc' ? va - vb : vb - va;
    });
    return {
      sortedData:    sorted,
      filteredCount: filtered.length,
      totalPages:    Math.max(1, Math.ceil(sorted.length / itemsPerPage)),
    };
  }, [data, sortBy, sortOrder, searchTerm, minNtr, minVol, minTrades, minChange1h, change1hDir, gridMode]);

  useEffect(() => { setPageIndex(0); }, [searchTerm, minNtr, minVol, minTrades, minChange1h, change1hDir, sortBy, sortOrder, gridMode, candleCount]);

  const pageData = useMemo(() => {
    const itemsPerPage = (gridMode === '16grid' ? 16 : 12) - 1;
    return sortedData.slice(pageIndex * itemsPerPage, (pageIndex + 1) * itemsPerPage);
  }, [sortedData, pageIndex, gridMode]);

  const displayData = useMemo(() => {
    const totalSlots  = gridMode === '16grid' ? 16 : 12;
    const btc = data.find(d => d?.symbol === 'BTCUSDT') || null;
    const grid = new Array(totalSlots).fill(null);
    pageData.forEach((c, i) => { grid[i] = c; });
    grid[totalSlots - 1] = btc;
    if (isHovered && frozenRef.current) {
      return frozenRef.current.map(fc => fc ? (data.find(d => d.symbol === fc.symbol) || fc) : null);
    }
    return grid;
  }, [pageData, data, gridMode, isHovered]);

  const { updateVisible } = useKlineWebSockets(candleCount, setData);

  useEffect(() => {
    const visible = displayData.filter(Boolean).map(c => c.symbol);
    updateVisible(visible);
  }, [displayData, updateVisible]);

  const totalPagesVal = totalPages;
  const goPage = useCallback((p) => setPageIndex(Math.max(0, Math.min(p, totalPagesVal - 1))), [totalPagesVal]);
  const prevPage = useCallback(() => goPage(pageIndex - 1), [pageIndex, goPage]);
  const nextPage = useCallback(() => goPage(pageIndex + 1), [pageIndex, goPage]);

  useEffect(() => {
    const onKey = (e) => {
      if (e.key === 'ArrowLeft')  { e.preventDefault(); prevPage(); }
      if (e.key === 'ArrowRight') { e.preventDefault(); nextPage(); }
      if (e.ctrlKey && e.key === 'f') { e.preventDefault(); searchInputRef.current?.focus(); }
      if (e.ctrlKey && e.key === 'r') { e.preventDefault(); fetchData(false); }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [prevPage, nextPage, fetchData]);

  const onCopy = useCallback((text) => {
    if (!text) return;
    navigator.clipboard.writeText(text).catch(() => {
      const el = document.createElement('textarea');
      el.value = text; document.body.appendChild(el); el.select();
      document.execCommand('copy'); document.body.removeChild(el);
    });
    setCopiedSymbol(text);
    setTimeout(() => setCopiedSymbol(null), 2000);
  }, []);

  const toggleSort = (field) => {
    if (sortBy === field) setSortOrder(o => o === 'asc' ? 'desc' : 'asc');
    else { setSortBy(field); setSortOrder(field.startsWith('dist_') ? 'asc' : 'desc'); }
  };

  useEffect(() => {
    const onMouseDown = (e) => { if (e.button === 1) e.preventDefault(); };
    const onAuxClick  = (e) => { if (e.button === 1) e.preventDefault(); };
    document.addEventListener('mousedown', onMouseDown, { passive: false });
    document.addEventListener('auxclick',  onAuxClick,  { passive: false });
    return () => {
      document.removeEventListener('mousedown', onMouseDown);
      document.removeEventListener('auxclick',  onAuxClick);
    };
  }, []);

  if (loading) return (
    <div className="h-screen bg-[#060708] flex flex-col items-center justify-center gap-2">
      <div className="w-6 h-6 border border-yellow-500/10 border-t-yellow-500 rounded-full animate-spin" />
      <span className="text-slate-600 text-[8px] uppercase font-bold tracking-widest animate-pulse">Загрузка...</span>
    </div>
  );

  const totalSlots = gridMode === '16grid' ? 16 : 12;
  const gridClass  = gridMode === '16grid' ? 'grid grid-cols-4 grid-rows-4 gap-0.5' : 'grid grid-cols-4 grid-rows-3 gap-0.5';

  return (
    <div className="h-screen bg-[#060708] text-[#d1d4dc] text-[10px] select-none p-0.5 flex flex-col overflow-hidden">

      <div className="absolute top-2 right-2 z-50 flex items-center gap-2">
        <div className="flex items-center gap-1 bg-[#1e222d] border border-[#2a2e39] rounded px-2 py-1">
          <div className={`w-2 h-2 rounded-full ${wsConnected ? 'bg-green-500 animate-pulse' : 'bg-red-500'}`} />
          <span className="text-[6px] text-slate-400">{wsConnected ? 'Online' : 'Offline'}</span>
        </div>
        <button onClick={() => setPanelVisible(v => !v)} className="bg-[#1e222d] border border-[#2a2e39] rounded p-1 hover:bg-[#2a2e39] transition-colors">
          {panelVisible ? <ChevronUp size={12} /> : <ChevronDown size={12} />}
        </button>
      </div>

      {panelVisible && (
        <div className="bg-[#131722] border border-[#2a2e39] rounded mb-0.5 px-2 py-1 flex items-center justify-between gap-2 flex-shrink-0">
          <div className="flex items-center gap-3">
            <TrendingUp className="text-yellow-400 flex-shrink-0" size={14} />
            <div className="flex items-center gap-1">
              {[['ntr','N'],['quoteVolume','V'],['trades15min','T15'],['change1h','1H'],].map(([field, label]) => (
                <button key={field} onClick={() => toggleSort(field)}
                  className={`px-2 py-0.5 rounded font-semibold uppercase text-[7px] transition-colors flex items-center gap-0.5 ${sortBy === field ? 'bg-yellow-500 text-black' : 'text-slate-500 hover:text-white'}`}>
                  {label} {sortBy === field && (sortOrder === 'desc' ? '↓' : '↑')}
                </button>
              ))}
              <select
                className={`bg-transparent border-none px-1 text-[7px] outline-none font-semibold uppercase cursor-pointer ${sortBy.startsWith('dist_') ? 'text-yellow-500' : 'text-slate-500'}`}
                value={sortBy.startsWith('dist_') ? sortBy : ''}
                onChange={e => { if (e.target.value) { setSortBy(e.target.value); setSortOrder('asc'); } }}>
                <option value="" disabled className="bg-[#131722]">УРОВНИ...</option>
                {[['dist_h1h','1H High'],['dist_h1l','1H Low'],['dist_h4h','4H High'],['dist_h4l','4H Low'],['dist_d24h','24H High'],['dist_d24l','24H Low']].map(([v,l]) => (
                  <option key={v} value={v} className="bg-[#131722]">{l}</option>
                ))}
              </select>
            </div>

            <div className="flex items-center gap-3 border-l border-white/10 pl-3">
              {[
                { label: 'Min NTR', val: minNtr, set: setMinNtr, step: 0.1, cls: 'text-yellow-500 border-yellow-500/20', w: 'w-10' },
                { label: 'Min V(M)', val: minVol, set: setMinVol, step: 10, cls: 'text-blue-400', w: 'w-12' },
                { label: 'Min T15', val: minTrades, set: setMinTrades, step: 100, cls: 'text-purple-400 border-purple-500/20', w: 'w-14' },
              ].map(({ label, val, set, step, cls, w }) => (
                <div key={label} className="flex items-center gap-1.5">
                  <span className="text-[6px] font-semibold text-slate-500 uppercase">{label}:</span>
                  <input type="number" step={step}
                    className={`bg-[#1e222d] border border-[#363c4e] rounded px-1 py-0 text-[8px] outline-none font-semibold ${w} text-center ${cls}`}
                    value={val} onChange={e => set(parseFloat(e.target.value) || 0)} />
                </div>
              ))}

              <div className="flex items-center gap-1 border-l border-white/10 pl-2">
                <span className="text-[6px] font-semibold text-slate-500 uppercase">1H:</span>
                <div className="flex rounded overflow-hidden border border-[#363c4e]">
                  {[['any','—'],['up','▲'],['down','▼']].map(([dir, icon]) => (
                    <button key={dir} onClick={() => setChange1hDir(dir)}
                      className={`px-1.5 py-0.5 text-[7px] font-bold transition-colors ${
                        change1hDir === dir
                          ? dir === 'up'   ? 'bg-emerald-500 text-black'
                          : dir === 'down' ? 'bg-rose-500 text-white'
                          : 'bg-slate-600 text-white'
                          : 'bg-[#1e222d] text-slate-500 hover:text-white'
                      }`}>
                      {icon}
                    </button>
                  ))}
                </div>
                {change1hDir !== 'any' && (
                  <input type="number" min="0" max="10" step="0.5"
                    className={`bg-[#1e222d] border border-[#363c4e] rounded px-1 py-0 text-[8px] outline-none font-bold w-10 text-center ${change1hDir === 'up' ? 'text-emerald-400' : 'text-rose-400'}`}
                    value={minChange1h}
                    onChange={e => setMinChange1h(parseFloat(e.target.value) || 0)} />
                )}
                {change1hDir !== 'any' && (
                  <span className={`text-[7px] font-bold ${change1hDir === 'up' ? 'text-emerald-400' : 'text-rose-400'}`}>%</span>
                )}
              </div>

              <div className="flex items-center gap-1.5">
                <span className="text-[6px] font-semibold text-slate-500 uppercase flex items-center gap-0.5"><Clock size={7} className="text-blue-400" /> Свечей:</span>
                <input type="number" min="10" max={MAX_CANDLE_COUNT} step="10"
                  className="bg-[#1e222d] border border-blue-500/40 rounded px-1 py-0 text-[8px] outline-none text-blue-400 font-bold w-14 text-center"
                  value={candleCount}
                  onChange={e => setCandleCount(Math.min(MAX_CANDLE_COUNT, Math.max(10, parseInt(e.target.value) || DEFAULT_CANDLE_COUNT)))} />
              </div>
              <button onClick={() => setGridMode(m => m === '16grid' ? '12grid' : '16grid')}
                className={`px-1.5 py-0.5 rounded text-[7px] font-semibold uppercase flex items-center gap-0.5 ${gridMode === '16grid' ? 'bg-yellow-500 text-black' : 'bg-slate-700 text-white'}`}>
                {gridMode === '16grid' ? <LayoutGrid size={7} /> : <Grid3x3 size={7} />}
                {gridMode === '16grid' ? '16' : '12'}
              </button>
              <div className="flex items-center gap-1 border-l border-white/5 pl-3">
                <BarChart3 size={7} className="text-emerald-400" />
                <span className="text-[6px] font-semibold text-emerald-400 uppercase">Инстр: <span className="text-white">{filteredCount}</span></span>
              </div>
              <div className="flex items-center gap-1 border-l border-white/5 pl-3">
                <button onClick={prevPage} disabled={pageIndex === 0}
                  className={`p-0.5 rounded ${pageIndex === 0 ? 'text-slate-700 cursor-not-allowed' : 'text-slate-400 hover:text-white'}`}>
                  <ChevronLeft size={9} />
                </button>
                <span className="text-[6px] font-semibold text-slate-400">{pageIndex + 1}/{totalPagesVal}</span>
                <button onClick={nextPage} disabled={pageIndex >= totalPagesVal - 1}
                  className={`p-0.5 rounded ${pageIndex >= totalPagesVal - 1 ? 'text-slate-700 cursor-not-allowed' : 'text-slate-400 hover:text-white'}`}>
                  <ChevronRight size={9} />
                </button>
              </div>
            </div>
          </div>

          <div className="flex items-center gap-2">
            {isHovered && <span className="text-[6px] text-yellow-500/50 font-bold uppercase animate-pulse">Grid Locked</span>}
            <div className="relative">
              <Search className="absolute left-1.5 top-1/2 -translate-y-1/2 text-slate-500" size={9} />
              <input ref={searchInputRef} type="text" placeholder="ПОИСК... (Ctrl+F)"
                className="bg-[#1e222d] border border-[#363c4e] rounded py-0.5 pl-5 pr-2 text-[7px] outline-none w-28 font-semibold uppercase tracking-wide"
                value={searchInput} onChange={e => setSearchInput(e.target.value)} />
            </div>
            <button onClick={() => fetchData(false)} className="bg-white/5 hover:bg-white/10 p-1 rounded transition-all">
              <RefreshCw size={11} className={refreshing ? 'animate-spin text-yellow-500' : 'text-slate-400'} />
            </button>
          </div>
        </div>
      )}

      <TickContext.Provider value={tick}>
      <div
        className={`flex-grow overflow-hidden ${gridClass}`}
        onMouseEnter={() => { frozenRef.current = displayData; setIsHovered(true); }}
        onMouseLeave={() => { frozenRef.current = null; setIsHovered(false); }}
        style={{ opacity: gridVisible ? 1 : 0, transition: 'opacity 0.25s ease' }}
      >
        {displayData.map((coin, idx) => {
          const isBtc = idx === totalSlots - 1;
          return (
            <div
              key={coin ? `${coin.symbol}-${idx}` : `empty-${idx}`}
              onClick={() => coin && onCopy(coin.symbol)}
              className={`bg-[#131722] rounded-sm flex flex-col relative overflow-hidden cursor-pointer border border-[#2a2e39] ${isBtc ? 'shadow-[inset_0_0_10px_rgba(234,179,8,0.1)]' : ''}`}
              style={{
                transition: `opacity 0.3s ease ${idx * 18}ms, transform 0.3s ease ${idx * 18}ms`,
                opacity:   gridVisible ? 1 : 0,
                transform: gridVisible ? 'scale(1)' : 'scale(0.97)',
              }}
            >
              {isBtc && (
                <div className="absolute top-0 right-0 bg-yellow-500 text-black text-[5px] px-1 font-bold z-20 rounded-bl tracking-tighter">BTC MASTER</div>
              )}
              {coin ? (
                <ChartErrorBoundary key={coin.symbol}>
                  <TradingViewChart
                    klines={coin.klines}
                    coin={coin}
                    copiedSymbol={copiedSymbol}
                    onCopy={onCopy}
                    trades15min={coin.trades15min}
                    candleCount={candleCount}
                  />
                </ChartErrorBoundary>
              ) : (
                <div className="w-full h-full flex items-center justify-center text-slate-800 text-[7px] uppercase">Пусто</div>
              )}
            </div>
          );
        })}
      </div>
      </TickContext.Provider>

    </div>
  );
};

export default App;
