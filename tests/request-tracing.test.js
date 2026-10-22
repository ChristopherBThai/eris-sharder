'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const apm = require('elastic-apm-node').start({serviceName: 'sharder-offline', disableSend: true, centralConfig: false, cloudProvider: 'none', metricsInterval: '0s', logLevel: 'off'});
const Handler = require('../src/structures/SyncedRequestHandler');

function ipc() {
    const events = new Map();
    return {events, register(name, callback) { events.set(name, callback); }, unregister(name) { events.delete(name); }};
}

test('RPC settles once and releases pending state on send throw, send callback error and disconnect', async () => {
    const originalSend = process.send;
    try {
        for (const mode of ['throw', 'callback', 'disconnect', 'response']) {
            const messages = ipc();
            const handler = new Handler(messages, {timeout: 100});
            const disconnect = process.listeners('disconnect').at(-1);
            process.send = (message, callback) => {
                if (mode === 'throw') throw new Error('synthetic send error');
                if (mode === 'callback') return callback(new Error('synthetic callback error'));
                if (mode === 'disconnect') return disconnect();
                messages.events.get(`apiResponse.${message.requestID}`)({data: 'ok'});
                callback(new Error('late send error'));
            };
            const tx = apm.startTransaction('caller');
            const result = handler.request('POST', '/webhooks/123/synthetic');
            assert.equal(apm.currentTransaction, tx);
            assert.equal(apm.currentSpan, null);
            if (mode === 'response') assert.equal(await result, 'ok');
            else await assert.rejects(result);
            assert.equal(messages.events.size, 0);
            tx.end();
            process.removeListener('disconnect', disconnect);
        }
    } finally { process.send = originalSend; }
});

test.after(async () => { await apm.destroy(); });
