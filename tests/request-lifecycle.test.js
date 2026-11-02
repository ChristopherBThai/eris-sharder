'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');
const vm = require('vm');
const { createRequire } = require('module');
const { EventEmitter } = require('events');
const { AsyncLocalStorage } = require('async_hooks');
const root = path.dirname(require.resolve('eris'));
const EXPECTED = {
  "lib/rest/RequestHandler.js": "cf6dc579597dfffe83f535ab8357e981efecafdbe4e7426a3b1f344bb3dc9b6a",
  "lib/util/SequentialBucket.js": "6eae9f046c08621830726bfcdf3ea07bec6d1333dd5fb5807560bf58967634ed",
  "lib/Client.js": "915913936700ca20bc92e74ced505424b1ff44d93ec8d8a7a26365f7ffdbd8f7"
};
function harness(options = {}, faults = {}) {
  let now = faults.now || 100000;
  const timers = [], sent = [];
  const clock = class extends Date { static now() { return now; } };
  const setTimeout = (fn, delay) => { const t = { fn, at: now + Number(delay), unref() { this.unreffed = true; } }; timers.push(t); return t; };
  const clearTimeout = t => { const i = timers.indexOf(t); if (i >= 0) timers.splice(i, 1); };
  const https = { request(options) {
    if (faults.construct) { faults.construct = false; throw new Error('HTTPS construction'); }
    const req = new EventEmitter(); req.options = options; req.at = now; req.chunks = [];
    req.setTimeout = (_, fn) => { req.timeout = fn; }; req.write = chunk => req.chunks.push(chunk);
    req.abort = () => req.emit('abort'); req.end = data => { req.data = data; sent.push(req); }; return req;
  } };
  function load(relative, extra = {}) {
    const filename = path.join(root, relative), native = createRequire(filename), module = { exports: {} };
    vm.runInNewContext(fs.readFileSync(filename, 'utf8'), { module, exports: module.exports,
      require: name => name in extra ? extra[name] : native(name), Date: clock, setTimeout, clearTimeout, Buffer, console }, { filename });
    return module.exports;
  }
  const Bucket = load('lib/util/SequentialBucket.js');
  const Handler = load('lib/rest/RequestHandler.js', { https, '../util/SequentialBucket': Bucket });
  const client = new EventEmitter(); client.options = {}; client._token = 'synthetic'; client.on('error', () => {});
  const handler = new Handler(client, { disableLatencyCompensation: true, ...options });
  const flush = async () => { for (let i = 0; i < 16; i++) await Promise.resolve(); };
  function respond(index, status, headers = {}, body = '') {
    const resp = new EventEmitter(); resp.statusCode = status; resp.headers = { date: new Date(now).toUTCString(), ...headers };
    sent[index].emit('response', resp); if (body) resp.emit('data', body); resp.emit('end'); return resp;
  }
  async function advance(ms) {
    const target = now + ms;
    for (;;) {
      timers.sort((a,b) => a.at - b.at);
      if (!timers.length || timers[0].at > target) break;
      const t = timers.shift(); now = t.at; t.fn(); await flush();
    }
    now = target; await flush();
  }
  return { handler, client, sent, timers, respond, advance, flush, setTimeout, clearTimeout };
}

function bridge(options = {}, timeout = 100) {
  const h = harness(options), events = new Map(), replies = [], transactions = [];
  const cluster = new EventEmitter(); cluster.isMaster = false; cluster.isWorker = false;
  const proc = new EventEmitter();
  const apm = { currentTraceparent: undefined, startSpan() { return null; }, captureError() {}, startTransaction() {
    const tx = { ended: 0, setLabel() {}, setOutcome() {}, end() { this.ended++; } }; transactions.push(tx); return tx;
  } };
  function load(filename, extra) {
    const native = createRequire(filename), module = { exports: {} };
    vm.runInNewContext(fs.readFileSync(filename, 'utf8'), { module, exports: module.exports, Buffer, console,
      process: proc, setTimeout: h.setTimeout, clearTimeout: h.clearTimeout, require: name => name in extra ? extra[name] : native(name) }, { filename });
    return module.exports;
  }
  const sharder = path.resolve(__dirname, '..');
  const Manager = load(path.join(sharder, 'src/sharding/clustermanager.js'), {
    cluster, './cluster.js': class {}, '../utils/logger.js': {}, eris: class { constructor() { this.requestHandler = h.handler; } },
    '../utils/queue.js': EventEmitter, 'elastic-apm-node': apm
  });
  const manager = new Manager('synthetic', 'unused', {});
  manager.workers.set(1, 0);
  manager.sendTo = (_, message) => { replies.push(message); events.get(message._eventName)?.(message); };
  const Worker = load(path.join(sharder, 'src/structures/SyncedRequestHandler.js'), { 'elastic-apm-node': apm });
  const worker = new Worker({ register: (name, cb) => events.set(name, cb), unregister: name => events.delete(name) }, { timeout: timeout - 1000 });
  proc.send = (message, cb) => { cluster.emit('message', { id: 1 }, JSON.parse(JSON.stringify(message))); cb(); };
  const Client = load(path.join(root, 'lib/Client.js'), new Proxy({}, { has: (_, name) => !['events', 'eventemitter3', './rest/Endpoints', './Constants'].includes(name), get: () => class {} }));
  const client = Object.create(Client.prototype); client.requestHandler = worker;
  return { ...h, client, worker, events, replies, transactions };
}

test('installed Eris matches reviewed lifecycle source', () => {
  const crypto = require('crypto');
  for (const [file, expected] of Object.entries(EXPECTED)) {
    assert.equal(crypto.createHash('sha256').update(fs.readFileSync(path.join(root, file))).digest('hex'), expected, file);
  }
});

test('actual callback adapter traverses IPC and master once under global/startup gates', async () => {
  for (const startup of [false, true]) {
    const h = bridge({ forceQueueing: startup }); h.handler.globalBlock = true; h.handler.globalRatelimit = !startup;
    const promise = h.client.createInteractionResponse('1', 'token', { type: 4, with_response: true });
    await h.flush(); assert.equal(h.sent.length, startup ? 0 : 1);
    h.handler.globalUnblock(); await h.flush(); assert.equal(h.sent.length, 1);
    assert.match(h.sent[0].options.path, /callback\?with_response=true$/);
    h.respond(0, 204); await promise; await h.flush();
    h.handler.globalUnblock(); await h.flush(); assert.equal(h.sent.length, 1);
    assert.equal(Object.keys(h.handler.ratelimits).length, 0); assert.equal(h.events.size, 0);
    assert.equal(h.replies.length, 1); assert.equal(h.transactions[0].ended, 1);
  }
});

test('actual webhook adapter preserves A retry B while token C progresses', async () => {
  const h = bridge(), token = 'x'.repeat(64), id = '123456789012345678';
  const a = h.client.editWebhookMessage(id, token, '@original', { content: 'A' });
  const b = h.client.editWebhookMessage(id, token, '@original', { content: 'B' });
  const c = h.client.editWebhookMessage(id, 'y'.repeat(64), '@original', { content: 'C' });
  await h.flush(); assert.equal(h.sent.length, 2);
  h.respond(0, 429, { 'retry-after': '0.02', 'x-ratelimit-scope': 'shared', 'x-ratelimit-remaining': '1' }, '{"retry_after":0.02}');
  h.respond(1, 204); await c; await h.advance(19); assert.equal(h.sent.length, 2);
  await h.advance(1); assert.equal(h.sent.length, 3); assert.match(h.sent[2].data, /"content":"A"/);
  h.respond(2, 204); await a; await h.advance(2); assert.equal(h.sent.length, 4); assert.match(h.sent[3].data, /"content":"B"/);
  h.respond(3, 204); await b; await h.flush(); assert.equal(h.events.size, 0);
  assert(h.transactions.every(tx => tx.ended === 1));
});

test('malformed shared response settles once and releases next worker request', async () => {
  const h = bridge(), url = '/webhooks/123456789012345678/' + 'x'.repeat(64) + '/messages/@original';
  const a = h.worker.request('PATCH', url, false).catch(error => error);
  const b = h.worker.request('PATCH', url, false); await h.flush();
  h.respond(0, 429, { 'x-ratelimit-scope': 'shared', 'content-type': 'application/json' }, '{');
  assert((await a).message); await h.advance(2); assert.equal(h.sent.length, 2);
  h.respond(1, 204); await b; await h.flush(); assert.equal(h.events.size, 0);
  assert.equal(h.replies.length, 2); assert(h.transactions.every(tx => tx.ended === 1));
});

test('worker timeout unregisters but master completes only on late transport settlement', async () => {
  const h = bridge({}, 5);
  const result = h.worker.request('PATCH', '/normal', false).catch(error => error);
  await h.flush(); assert.equal(h.sent.length, 1);
  await h.advance(5); assert.match((await result).message, /timed out/); assert.equal(h.events.size, 0);
  assert.equal(h.transactions[0].ended, 0); assert.equal(h.replies.length, 0);
  assert.equal(Object.values(h.handler.ratelimits)[0].owners, 1);
  h.respond(0, 204); await h.flush(); assert.equal(h.transactions[0].ended, 1);
  assert.equal(h.replies.length, 1); assert.equal(h.events.size, 0);
  assert.equal(Object.values(h.handler.ratelimits)[0].owners, 0);
});
