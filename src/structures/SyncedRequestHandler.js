const crypto = require('crypto');
const { AsyncResource } = require('async_hooks');
const apm = require('elastic-apm-node');

class SyncedRequestHandler {
    #pending = new Map();
    constructor(ipc, options) {
        this.ipc = ipc;
        this.timeout = options.timeout + 1000;
        process.once('disconnect', () => {
            for (const finish of this.#pending.values()) {
                finish(new Error('Discord RPC disconnected'), undefined, 'disconnected');
            }
        });
    }

    request(method, url, auth, body, file, _route, short) {
        // Starting a span must not make parallel calls children of one another.
        const scope = new AsyncResource('discord.rpc');
        return scope.runInAsyncScope(() => new Promise((resolve, reject) => {
            const stackCapture = new Error().stack;
            const requestID = crypto.randomBytes(16).toString('hex');
            const parent = apm.currentTraceparent;
            const span = apm.startSpan('discord.rpc', 'discord', 'rpc');
            span?.setLabel('method', /^(GET|POST|PUT|PATCH|DELETE|HEAD|OPTIONS)$/.test(method) ? method : 'other');
            span?.setLabel('route_family', /^\/(channels|guilds|interactions|webhooks)(?:\/|$)/.exec(url)?.[1] || 'other');
            // A budget-dropped span can still be returned, with its recorded bit
            // cleared. Propagate the caller instead of suppressing downstream work.
            const spanParent = span?.traceparent;
            const traceParent = spanParent?.endsWith('-01') ? spanParent : parent;
            const event = `apiResponse.${requestID}`;
            let settled = false;
            let timeout;
            const finish = (error, data, result) => {
                if (settled) return;
                settled = true;
                clearTimeout(timeout);
                this.ipc.unregister(event);
                this.#pending.delete(requestID);
                span?.setLabel('result', result);
                span?.setOutcome(error ? 'failure' : 'success');
                span?.end();
                scope.emitDestroy();
                if (error) reject(error);
                else resolve(data);
            };
            this.#pending.set(requestID, finish);
            this.ipc.register(event, data => {
                if (data.err) {
                    const error = new Error(data.err.message);
                    error.stack = data.err.stack + '\n' + stackCapture.substring(stackCapture.indexOf('\n') + 1);
                    error.code = data.err.code;
                    // Preserve transport identity for export filters without changing
                    // the caller-facing error name, message, stack or code.
                    error.discordTransport = true;
                    finish(error, undefined, 'request_error');
                } else {
                    finish(null, data.data, 'response');
                }
            });
            timeout = setTimeout(() => {
                const error = new Error(`Discord RPC timed out (>${this.timeout}ms)`);
                error.stack = `Error: ${error.message}\n` + stackCapture.substring(stackCapture.indexOf('\n') + 1);
                finish(error, undefined, 'caller_timeout');
            }, this.timeout);
            try {
                if (file && file.file) file.file = Buffer.from(file.file).toString('base64');
                process.send({ name: 'apiRequest', requestID, method, url, auth, body, file, _route, short, traceParent, callerBudget: this.timeout }, error => {
                    if (error) finish(error, undefined, 'send_error');
                });
            } catch (error) {
                finish(error, undefined, 'send_error');
            }
        }));
    }
}

module.exports = SyncedRequestHandler;
