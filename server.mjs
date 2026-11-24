import 'dotenv/config';
import express from 'express';
import { WebSocketServer, WebSocket } from 'ws';
import Parser from 'rss-parser';
import http from 'node:http';

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/live' });
const PORT = Number(process.env.PORT || 3000);
const BINANCE_REST = 'https://fapi.binance.com';
const BINANCE_WS = 'wss://fstream.binance.com/market/stream?streams=!ticker@arr';
const parser = new Parser({ timeout: 12000, headers: { 'User-Agent': 'OilLiquiditySniperPro/2.0' } });

app.set('trust proxy', 1);
app.use(express.json({ limit: '1mb' }));
app.use(express.static('public'));

const state = {
  connected: false, lastMarketUpdate: null, lastNewsUpdate: null,
  tickers: new Map(), klines: new Map(), signals: [], oilSymbols: [], gasSymbols: [],
  news: [], ai: null, exchangeSymbols: [], exchangeSymbolSet: new Set(), upstream: null, klineSocket: null,
  klineSubscribed: new Set(), klineLoadBusy: false, klineLoadStarted: false
};
const feeds = [
  ['Oil & energy', 'https://news.google.com/rss/search?q=oil+market+OR+crude+oil&hl=en-US&gl=US&ceid=US:en'],
  ['EIA / inventories', 'https://news.google.com/rss/search?q=EIA+crude+oil+inventories&hl=en-US&gl=US&ceid=US:en'],
  ['OPEC+', 'https://news.google.com/rss/search?q=OPEC+oil+production&hl=en-US&gl=US&ceid=US:en'],
  ['Fed / USD', 'https://news.google.com/rss/search?q=Federal+Reserve+US+Dollar+markets&hl=en-US&gl=US&ceid=US:en'],
  ['Geopolitics', 'https://news.google.com/rss/search?q=geopolitics+oil+supply&hl=en-US&gl=US&ceid=US:en']
];
const send = (ws, payload) => { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(payload)); };
function broadcast(payload) { for (const ws of wss.clients) send(ws, payload); }
const n = v => Number(v || 0);
const round = (v, d=2) => Number.isFinite(v) ? Number(v.toFixed(d)) : null;
function ema(values, period) {
  if (!values.length) return null;
  const k = 2 / (period + 1);
  let e = values[0];
  for (let i=1;i<values.length;i++) e = values[i] * k + e * (1-k);
  return e;
}
function rsi(values, period=12) {
  if (values.length <= period) return null;
  let gain=0, loss=0;
  for (let i=values.length-period;i<values.length;i++) {
    const d=values[i]-values[i-1]; if(d>=0) gain+=d; else loss-=d;
  }
  if (!loss && !gain) return 50;
  if (!loss) return 100;
  if (!gain) return 0;
  const rs=(gain/period)/(loss/period);
  return 100-(100/(1+rs));
}
function computeSignal(symbol) {
  const t=state.tickers.get(symbol), bars=state.klines.get(symbol);
  if (!t) return null;
  const closes=(bars||[]).map(x=>n(x[4])).filter(Number.isFinite);
  const e36=closes.length>=36?ema(closes.slice(-180),36):null;
  const e89=closes.length>=89?ema(closes.slice(-220),89):null;
  const e200=closes.length>=200?ema(closes.slice(-250),200):null;
  const rv=closes.length>=14?rsi(closes,12):null;
  const price=n(t.c), pct=n(t.P), qv=n(t.q);
  let score=50, reasons=[];
  if(e36 && e89){if(price>e36 && e36>e89){score+=18;reasons.push('Giá trên EMA36 và EMA36 trên EMA89');}else if(price<e36 && e36<e89){score-=18;reasons.push('Giá dưới EMA36 và EMA36 dưới EMA89');}}
  if(e200){if(price>e200){score+=8;reasons.push('Trên EMA200');}else{score-=8;reasons.push('Dưới EMA200');}}
  if(rv!==null){if(rv>=55&&rv<72){score+=9;reasons.push('RSI nghiêng tăng');}else if(rv<=45&&rv>28){score-=9;reasons.push('RSI nghiêng giảm');}else if(rv>=72){score-=5;reasons.push('RSI cao, có rủi ro quá mua');}else if(rv<=28){score+=5;reasons.push('RSI thấp, có rủi ro quá bán');}}
  if(pct>1){score+=Math.min(10,pct*2);reasons.push('Biến động 24h tích cực');}
  if(pct< -1){score-=Math.min(10,Math.abs(pct)*2);reasons.push('Biến động 24h tiêu cực');}
  score=Math.max(1,Math.min(99,Math.round(score)));
  const dataReady=closes.length>=200;
  // Never label a market LONG/SHORT from 24h change alone when the 1m history is missing.
  const bias=!dataReady?'WAIT':score>=62?'LONG':score<=38?'SHORT':'NEUTRAL';
  return {symbol,price:round(price,8),change24h:round(pct,2),quoteVolume:round(qv,0),bias,score,rsi:round(rv,1),ema36:round(e36,8),ema89:round(e89,8),ema200:round(e200,8),reasons,updatedAt:Date.now(),dataReady};
}
function buildSignals() {
  const all=[...state.tickers.keys()].map(computeSignal).filter(Boolean);
  all.sort((a,b)=>b.score-a.score);
  state.signals=all;
  return all;
}
function broadcastSnapshot() {
  const compact=[...state.tickers.values()].sort((a,b)=>n(b.q)-n(a.q));
  broadcast({type:'tickers',tickers:compact,oil:state.oilSymbols.map(x=>state.tickers.get(x.symbol)).filter(Boolean),gas:state.gasSymbols.map(x=>state.tickers.get(x.symbol)).filter(Boolean),updatedAt:state.lastMarketUpdate});
  broadcast({type:'signals',signals:state.signals});
}
async function fetchJson(url) {
  const r=await fetch(url,{headers:{'User-Agent':'OilLiquiditySniperPro/2.0'}});
  if(!r.ok) throw new Error(`Binance HTTP ${r.status}`);
  return r.json();
}
async function loadExchangeInfo() {
  try {
    const info=await fetchJson(`${BINANCE_REST}/fapi/v1/exchangeInfo`);
    state.exchangeSymbols=info.symbols.filter(s=>s.quoteAsset==='USDT'&&s.contractType==='PERPETUAL'&&s.status==='TRADING').map(s=>({symbol:s.symbol,baseAsset:s.baseAsset}));
    state.exchangeSymbolSet=new Set(state.exchangeSymbols.map(x=>x.symbol));
    for(const symbol of state.tickers.keys()) if(!state.exchangeSymbolSet.has(symbol)) state.tickers.delete(symbol);
    const oilRx=/(^CL$|WTI|BRENT|^BZ$|OIL|CRUDE)/i;
    const gasRx=/(NATGAS|NATURALGAS|NGAS|GAS)/i;
    state.oilSymbols=state.exchangeSymbols.filter(x=>oilRx.test(x.symbol)||oilRx.test(x.baseAsset));
    state.gasSymbols=state.exchangeSymbols.filter(x=>gasRx.test(x.symbol)||gasRx.test(x.baseAsset));
    broadcast({type:'universe',count:state.exchangeSymbols.length,oilSymbols:state.oilSymbols,gasSymbols:state.gasSymbols});
  } catch(e) { console.error('exchangeInfo:',e.message); }
}
async function loadTopKlines() {
  if (state.klineLoadBusy) return;
  const limit=Math.max(5,Math.min(60,Number(process.env.KLINE_SYMBOL_LIMIT||30)));
  const top=[...state.tickers.values()].filter(t=>t.s.endsWith('USDT')).sort((a,b)=>n(b.q)-n(a.q)).slice(0,limit);
  // Binance exchangeInfo is available before the ticker stream. Do not mark loading complete
  // until the market stream has populated the ticker map.
  if (!top.length) return;
  state.klineLoadBusy = true;
  try {
    for(let i=0;i<top.length;i+=5){
      await Promise.all(top.slice(i,i+5).map(async t=>{
        try {
          const bars=await fetchJson(`${BINANCE_REST}/fapi/v1/klines?symbol=${t.s}&interval=1m&limit=250`);
          state.klines.set(t.s,bars);
        } catch(e) { console.warn('Kline preload',t.s,e.message); }
      }));
    }
    for(const t of top) subscribeKlineSymbol(t.s,'1m');
    if(!state.klineSocket||state.klineSocket.readyState===WebSocket.CLOSED) connectKlineStream([...state.klineSubscribed]);
    state.klineLoadStarted = true;
    buildSignals();
    broadcastSnapshot();
  } finally {
    state.klineLoadBusy = false;
  }
}
function subscribeKlineSymbol(symbol, interval='1m') {
  const stream=`${symbol.toLowerCase()}@kline_${interval}`;
  if(state.klineSubscribed.has(stream)) return;
  state.klineSubscribed.add(stream);
  const ws=state.klineSocket;
  if(ws && ws.readyState===WebSocket.OPEN) {
    ws.send(JSON.stringify({method:'SUBSCRIBE',params:[stream],id:Date.now()%1000000000}));
  }
}
function connectKlineStream(streams) {
  for(const stream of streams) state.klineSubscribed.add(stream);
  if(state.klineSocket) { try{state.klineSocket.close();}catch{} }
  if(!state.klineSubscribed.size)return;
  const ws=new WebSocket('wss://fstream.binance.com/market/stream');
  state.klineSocket=ws;
  ws.on('open',()=>ws.send(JSON.stringify({method:'SUBSCRIBE',params:[...state.klineSubscribed],id:22})));
  ws.on('message',raw=>{
    try{
      const msg=JSON.parse(raw.toString()), d=msg.data||msg, k=d.k;
      if(!k||!d.s)return;
      const interval=k.i||'1m';
      const bar=[k.t,k.o,k.h,k.l,k.c,k.v,k.T,k.q,k.n,k.V,k.Q,k.B];
      // Keep the scanner's history strictly on 1m candles; other chart intervals are UI-only.
      if(interval==='1m'){
        const arr=state.klines.get(d.s)||[];
        if(arr.length&&arr[arr.length-1][0]===k.t)arr[arr.length-1]=bar;else arr.push(bar);
        if(arr.length>260)arr.shift();
        state.klines.set(d.s,arr);
        const sig=computeSignal(d.s);
        if(sig){const ix=state.signals.findIndex(x=>x.symbol===d.s);if(ix>=0)state.signals[ix]=sig;else state.signals.push(sig);}
      }
      broadcast({type:'kline',symbol:d.s,interval,bar});
      if(k.x&&interval==='1m'){state.signals.sort((a,b)=>b.score-a.score);broadcast({type:'signals',signals:state.signals});}
    }catch{}
  });
  ws.on('close',()=>setTimeout(()=>{if(state.klineSocket===ws)connectKlineStream([...state.klineSubscribed]);},5000));
  ws.on('error',()=>{});
}
function connectMarket() {
  const ws=new WebSocket(BINANCE_WS);
  state.upstream=ws;
  ws.on('open',()=>{state.connected=true;broadcast({type:'status',connected:true});});
  ws.on('message',raw=>{
    try{
      const parsed=JSON.parse(raw.toString()), arr=Array.isArray(parsed.data)?parsed.data:[parsed.data||parsed];
      for(const t of arr) if(t?.s&&state.exchangeSymbolSet.has(t.s)) state.tickers.set(t.s,t);
      state.lastMarketUpdate=Date.now();
      if (!state.klineLoadStarted && state.tickers.size > 20) loadTopKlines().catch(e=>console.error('Initial klines:',e.message));
      const compact=[...state.tickers.values()].sort((a,b)=>n(b.q)-n(a.q));
      buildSignals();
      broadcast({type:'tickers',tickers:compact,oil:state.oilSymbols.map(x=>state.tickers.get(x.symbol)).filter(Boolean),gas:state.gasSymbols.map(x=>state.tickers.get(x.symbol)).filter(Boolean),updatedAt:state.lastMarketUpdate});
      if(Math.random()<0.01) broadcast({type:'signals',signals:state.signals});
    }catch{}
  });
  ws.on('close',()=>{state.connected=false;broadcast({type:'status',connected:false});setTimeout(()=>{if(state.upstream===ws)connectMarket();},4000);});
  ws.on('error',()=>{});
}
async function refreshNews() {
  const all=[];
  await Promise.all(feeds.map(async ([category,url])=>{
    try{
      const feed=await parser.parseURL(url);
      for(const item of (feed.items||[]).slice(0,12)) all.push({
        title:item.title||'Untitled',link:item.link||'',source:item.creator||feed.title||category,
        category,publishedAt:item.isoDate||item.pubDate||null,
        snippet:(item.contentSnippet||item.content||'').replace(/<[^>]*>/g,' ').slice(0,400)
      });
    }catch(e){console.warn('RSS',category,e.message);}
  }));
  const seen=new Set();
  state.news=all.filter(x=>{const k=x.title.toLowerCase();if(seen.has(k))return false;seen.add(k);return true;})
    .sort((a,b)=>new Date(b.publishedAt||0)-new Date(a.publishedAt||0)).slice(0,45);
  state.lastNewsUpdate=Date.now();
  broadcast({type:'news',news:state.news,updatedAt:state.lastNewsUpdate});
}
function marketContext() {
  const oil=state.oilSymbols.map(x=>computeSignal(x.symbol)).filter(x=>x?.dataReady);
  const top=state.signals.filter(x=>x.dataReady&&x.bias==='LONG').slice(0,25);
  const short=state.signals.filter(x=>x.dataReady&&x.bias==='SHORT').slice(0,25);
  return {asOf:new Date().toISOString(),universeCount:state.exchangeSymbols.length,oilListed:state.oilSymbols,gasListed:state.gasSymbols,oilSignals:oil,topLong:top,shortCandidates:short,news:state.news.slice(0,30)};
}
async function runAI() {
  if(!process.env.OPENAI_API_KEY) return {available:false,message:'Chưa cấu hình OPENAI_API_KEY trên máy chủ. Dữ liệu Binance và bộ chấm điểm kỹ thuật vẫn hoạt động.'};
  const ctx=marketContext();
  const prompt=`Phân tích dữ liệu thị trường Futures Binance và tin tức được cung cấp. Không được bịa tin, giá, lịch sự kiện hoặc nguyên nhân. Nếu tin không đủ/không mới, ghi rõ. Phân biệt dữ liệu đo được và suy luận. Không đưa ra cam kết lợi nhuận. Trả lời tiếng Việt theo JSON với các khóa: market_regime, oil_view, long_candidates (mảng), short_candidates (mảng), conflicts (mảng), risk_notes (mảng), summary. Mỗi candidate gồm symbol, bias, score_0_100, timeframe, reasons, invalidation. Chỉ đánh giá mã có trong dữ liệu. Đánh dấu "insufficient_data" khi dữ liệu kỹ thuật chưa đủ 200 nến. Đây là phân tích tham khảo, không phải khuyến nghị đầu tư.\n\nDỮ LIỆU:\n${JSON.stringify(ctx)}`;
  const r=await fetch('https://api.openai.com/v1/responses',{
    method:'POST',headers:{'Authorization':`Bearer ${process.env.OPENAI_API_KEY}`,'Content-Type':'application/json'},
    body:JSON.stringify({model:process.env.OPENAI_MODEL||'gpt-6-luna',input:prompt,store:false})
  });
  const data=await r.json();
  if(!r.ok) throw new Error(data.error?.message||`OpenAI HTTP ${r.status}`);
  const out=(data.output||[]).flatMap(x=>x.content||[]).filter(x=>x.type==='output_text').map(x=>x.text).join('\n')||data.output_text||'';
  let parsed;
  try{const clean=out.replace(/^```(?:json)?/,'').replace(/```$/,'').trim();parsed=JSON.parse(clean);}
  catch{parsed={summary:out,market_regime:'Không đọc được JSON có cấu trúc',long_candidates:[],short_candidates:[],conflicts:[],risk_notes:['Kết quả AI cần được kiểm tra lại.']};}
  state.ai={available:true,generatedAt:Date.now(),result:parsed};
  broadcast({type:'ai',ai:state.ai});
  return state.ai;
}
app.get('/api/health',(req,res)=>res.json({ok:true,connected:state.connected,marketUpdatedAt:state.lastMarketUpdate,newsUpdatedAt:state.lastNewsUpdate,openAIConfigured:!!process.env.OPENAI_API_KEY,symbolCount:state.exchangeSymbols.length,tickerCount:state.tickers.size,signalCount:state.signals.length,candleCount:state.klines.size,oilSymbols:state.oilSymbols,gasSymbols:state.gasSymbols}));
app.get('/api/symbols',(req,res)=>res.json({count:state.exchangeSymbols.length,oilSymbols:state.oilSymbols,gasSymbols:state.gasSymbols,signals:state.signals}));
app.get('/api/klines',async(req,res)=>{
  const symbol=String(req.query.symbol||'').toUpperCase();
  const interval=String(req.query.interval||'1m');
  const allowed=['1m','3m','5m','15m','30m','1h','2h','4h','6h','8h','12h','1d'];
  if(!state.exchangeSymbols.length)return res.status(503).json({error:'Máy chủ đang tải danh sách Binance Futures, vui lòng thử lại sau ít giây.'});
  if(!state.exchangeSymbolSet.has(symbol))return res.status(400).json({error:'Symbol không thuộc danh sách USDⓈ-M Futures USDT perpetual đang giao dịch.'});
  if(!allowed.includes(interval))return res.status(400).json({error:'Interval không được hỗ trợ.'});
  try{
    const limit=Math.max(50,Math.min(500,Number(req.query.limit||250)));
    const bars=await fetchJson(`${BINANCE_REST}/fapi/v1/klines?symbol=${symbol}&interval=${interval}&limit=${limit}`);
    subscribeKlineSymbol(symbol,interval);
    if(interval==='1m'){
      state.klines.set(symbol,bars);
    } else if(!state.klines.has(symbol)) {
      // Keep the signal model anchored to 1m candles even when the chart is on 5m/15m/1h.
      subscribeKlineSymbol(symbol,'1m');
      try{const oneMinute=await fetchJson(`${BINANCE_REST}/fapi/v1/klines?symbol=${symbol}&interval=1m&limit=250`);state.klines.set(symbol,oneMinute);}catch(e){console.warn('1m signal preload',symbol,e.message);}
    }
    const sig=computeSignal(symbol);
    if(sig){const ix=state.signals.findIndex(x=>x.symbol===symbol);if(ix>=0)state.signals[ix]=sig;else state.signals.push(sig);state.signals.sort((a,b)=>b.score-a.score);broadcast({type:'signals',signals:state.signals});}
    res.json({symbol,interval,bars});
  }
  catch(e){res.status(502).json({error:e.message});}
});
app.get('/api/news',(req,res)=>res.json({items:state.news,updatedAt:state.lastNewsUpdate}));
app.get('/api/market',(req,res)=>res.json({connected:state.connected,oilSymbols:state.oilSymbols,gasSymbols:state.gasSymbols,tickers:[...state.tickers.values()].sort((a,b)=>n(b.q)-n(a.q)),signals:state.signals,updatedAt:state.lastMarketUpdate}));
app.get('/api/ai',(req,res)=>res.json(state.ai||{available:!!process.env.OPENAI_API_KEY,message:process.env.OPENAI_API_KEY?'Chưa chạy phân tích AI.':'Chưa cấu hình OPENAI_API_KEY.'}));
const aiUsage={day:'',count:0,lastByIp:new Map()};
app.post('/api/ai/analyze',async(req,res)=>{
  if(!process.env.OPENAI_API_KEY)return res.status(503).json({available:false,error:'Chưa cấu hình OPENAI_API_KEY trên Render.'});
  const day=new Date().toISOString().slice(0,10);
  if(aiUsage.day!==day){aiUsage.day=day;aiUsage.count=0;aiUsage.lastByIp.clear();}
  const ip=String(req.ip||req.socket.remoteAddress||'unknown');
  const now=Date.now(), last=aiUsage.lastByIp.get(ip)||0;
  const dailyLimit=Math.max(1,Math.min(100,Number(process.env.AI_REQUESTS_PER_DAY||12)));
  if(now-last<60000)return res.status(429).json({error:'Vui lòng chờ 60 giây giữa các lần gọi AI.'});
  if(aiUsage.count>=dailyLimit)return res.status(429).json({error:`Đã đạt giới hạn ${dailyLimit} lượt AI trong ngày để kiểm soát chi phí API.`});
  aiUsage.lastByIp.set(ip,now);aiUsage.count++;
  try{const result=await runAI();res.json(result);}
  catch(e){res.status(502).json({error:e.message});}
});
wss.on('connection',ws=>{
  ws.on('message',raw=>{if(raw.toString()==='ping')send(ws,{type:'pong',at:Date.now()});});
  send(ws,{type:'status',connected:state.connected});
  send(ws,{type:'universe',count:state.exchangeSymbols.length,oilSymbols:state.oilSymbols,gasSymbols:state.gasSymbols});
  send(ws,{type:'tickers',tickers:[...state.tickers.values()].sort((a,b)=>n(b.q)-n(a.q)),oil:state.oilSymbols.map(x=>state.tickers.get(x.symbol)).filter(Boolean),gas:state.gasSymbols.map(x=>state.tickers.get(x.symbol)).filter(Boolean),updatedAt:state.lastMarketUpdate});
  send(ws,{type:'signals',signals:state.signals});
  send(ws,{type:'news',news:state.news,updatedAt:state.lastNewsUpdate});
  if(state.ai)send(ws,{type:'ai',ai:state.ai});
});
server.listen(PORT,async()=>{
  console.log(`Oil Liquidity Sniper Pro running at http://localhost:${PORT}`);
  connectMarket();
  await loadExchangeInfo();
  await refreshNews();
  setInterval(refreshNews,Math.max(60000,Number(process.env.NEWS_REFRESH_MS||300000)));
  setInterval(()=>loadTopKlines().catch(e=>console.error('Kline refresh:',e.message)),5*60*1000);
  // AI analysis is user-triggered only. This prevents unexpected API charges in the background.
});
