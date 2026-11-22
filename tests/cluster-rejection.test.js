'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const { EventEmitter } = require('node:events');
const { inspect } = require('node:util');

function spawnCluster() {
    const messages = [];
    const process = new EventEmitter();
    process.send = message => messages.push(message);
    const module = { exports: {} };
    // Isolate process listeners and console overrides; never load a gateway client.
    const dependencies = {
        'eris-additions': eris => eris,
        'eris': class {},
        '../structures/Base.js': class {},
        '../structures/SyncedRequestHandler.js': class {},
        '../structures/IPC.js': class {},
        'util': { inspect }
    };
    const filename = require.resolve('../src/sharding/cluster.js');
    vm.runInNewContext(fs.readFileSync(filename, 'utf8'), {
        module,
        process,
        console: {},
        require(name) {
            assert.ok(Object.hasOwn(dependencies, name), `Unexpected dependency: ${name}`);
            return dependencies[name];
        }
    }, { filename });
    new module.exports().spawn();
    assert.equal(process.listenerCount('unhandledRejection'), 1);
    return { process, messages };
}

const circular = { message: 'circular rejection' };
circular.self = circular;
const error = new Error('original failure');
const cases = [
    ['undefined', undefined, 'undefined'],
    ['null', null, 'null'],
    ['string', 'failure detail', "'failure detail'"],
    ['object', { code: 'FAIL' }, "{ code: 'FAIL' }"],
    ['Error', error, error.stack],
    ['circular object', circular, inspect(circular)]
];

for (const [name, reason, diagnostic] of cases) {
    test(`registered rejection listener logs ${name} through existing IPC shape`, () => {
        const { process, messages } = spawnCluster();
        const promise = Promise.resolve();
        assert.doesNotThrow(() => process.emit('unhandledRejection', reason, promise));
        assert.equal(messages.length, 1);
        assert.deepEqual(Object.keys(messages[0]).sort(), ['msg', 'name']);
        assert.equal(messages[0].name, 'error');
        assert.equal(messages[0].msg, `Unhandled rejection at: Promise  ${promise} reason:  ${diagnostic}`);
    });
}
