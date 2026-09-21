'use strict';

const assert = require('assert');
const SyncedRequestHandler = require('../src/structures/SyncedRequestHandler');

class FakeIPC {
    constructor() {
        this.events = new Map();
        this.unregisterCalls = [];
    }

    register(name, callback) {
        this.events.set(name, callback);
    }

    unregister(name) {
        this.unregisterCalls.push(name);
        this.events.delete(name);
    }

    respond(name, data) {
        const callback = this.events.get(name);
        if (callback) callback(data);
    }
}

async function flushPromises() {
    await Promise.resolve();
    await Promise.resolve();
}

async function withRequestHarness(run) {
    const originalSend = process.send;
    const originalSetTimeout = global.setTimeout;
    const originalClearTimeout = global.clearTimeout;
    const sent = [];
    const timers = [];

    process.send = message => sent.push(message);
    global.setTimeout = (callback, delay) => {
        const timer = { callback, delay, active: true };
        timers.push(timer);
        return timer;
    };
    global.clearTimeout = timer => {
        timer.active = false;
    };

    try {
        await run({
            sent,
            timers,
            tick(milliseconds) {
                timers.filter(timer => timer.active && timer.delay <= milliseconds).forEach(timer => {
                    timer.active = false;
                    timer.callback();
                });
            }
        });
    } finally {
        process.send = originalSend;
        global.setTimeout = originalSetTimeout;
        global.clearTimeout = originalClearTimeout;
    }
}

async function testLateSuccessRemainsPending() {
    await withRequestHarness(async ({ sent, timers, tick }) => {
        const ipc = new FakeIPC();
        const handler = new SyncedRequestHandler(ipc, { timeout: 15000 });
        let settled = false;
        const request = handler.request('POST', '/channels/1/messages', true, { content: 'hello' });
        const result = request.then(
            value => ({ status: 'resolved', value }),
            error => ({ status: 'rejected', error })
        );
        result.then(() => {
            settled = true;
        });

        assert.strictEqual(sent.length, 1);
        assert.strictEqual(timers.length, 0, 'the worker must not create a shadow request deadline');
        const eventName = `apiResponse.${sent[0].requestID}`;

        tick(16001);
        await flushPromises();

        assert.strictEqual(settled, false, 'request must remain pending after the old worker timeout boundary');
        assert.strictEqual(ipc.events.has(eventName), true, 'response listener must remain registered');

        ipc.respond(eventName, { data: { id: 'message-id' } });

        assert.deepStrictEqual(await result, {
            status: 'resolved',
            value: { id: 'message-id' }
        });
        assert.deepStrictEqual(ipc.unregisterCalls, [eventName]);
        assert.strictEqual(sent.length, 1, 'the worker must forward the request exactly once');
    });
}

async function testErrorPreservesMasterAndCallerStack() {
    await withRequestHarness(async ({ sent }) => {
        const ipc = new FakeIPC();
        const handler = new SyncedRequestHandler(ipc, { timeout: 15000 });
        const request = handler.request('POST', '/channels/1/messages', true, { content: 'hello' });
        const eventName = `apiResponse.${sent[0].requestID}`;

        ipc.respond(eventName, {
            err: {
                message: 'Discord rejected the request',
                code: 50013,
                stack: 'Error: Discord rejected the request\n    at masterRequest'
            }
        });

        const error = await request.then(
            () => null,
            requestError => requestError
        );

        assert(error instanceof Error);
        assert.strictEqual(error.message, 'Discord rejected the request');
        assert.strictEqual(error.code, 50013);
        assert(error.stack.startsWith('Error: Discord rejected the request\n    at masterRequest'));
        assert(error.stack.includes('testErrorPreservesMasterAndCallerStack'));
        assert.deepStrictEqual(ipc.unregisterCalls, [eventName]);
        assert.strictEqual(sent.length, 1, 'the worker must not retry propagated errors');
    });
}

(async () => {
    await testLateSuccessRemainsPending();
    await testErrorPreservesMasterAndCallerStack();
    console.log('SyncedRequestHandler tests passed');
})().catch(error => {
    console.error(error);
    process.exitCode = 1;
});
