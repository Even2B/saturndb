class SaturnDBClient {
    #host;
    #socket;
    #buffer;
    #handlers;
    #reconnectDelay;
    #closing;
    #pendingResolve;

    #token;

    constructor(host = "127.0.0.1", port = 7379, token = "") {
        this.#host           = { host, port };
        this.#token          = token;
        this.#socket         = null;
        this.#buffer         = "";
        this.#handlers       = new Map();
        this.#reconnectDelay = 500;
        this.#closing        = false;
        this.#pendingResolve = null;
    }

    async connect() {
        return new Promise((resolve, reject) => {
            const net    = require("net");
            this.#socket = net.createConnection(this.#host, () => {
                this.#reconnectDelay = 500;
                resolve();
            });
            this.#socket.setNoDelay(true);
            this.#socket.on("data",  (data) => this.#onData(data));
            this.#socket.on("error", (err)  => reject(err));
            this.#socket.on("close", ()     => this.#onClose());
        });
        if (this.#token) await this.#sendAndRead(`AUTH ${this.#token}\n`);
    }

    async emit(stream, payload) {
        await this.#sendAndRead(`EMIT ${stream} ${JSON.stringify(payload)}\n`);
    }

    async get(stream) {
        const response = await this.#sendAndRead(`GET ${stream}\n`);
        if (response.startsWith("VALUE ")) return JSON.parse(response.slice(6));
        return null;
    }

    async since(stream, ts) {
        const response = await this.#sendAndRead(`SINCE ${stream} ${ts}\n`);
        if (response === "EMPTY") return [];
        return response
            .split("\n")
            .filter(l => l.startsWith("EVENT "))
            .map(l => {
                const rest    = l.slice(6);
                const spaceAt = rest.indexOf(" ");
                return {
                    stream:  rest.slice(0, spaceAt),
                    payload: JSON.parse(rest.slice(spaceAt + 1)),
                };
            });
    }

    async watch(pattern, handler) {
        this.#handlers.set(pattern, handler);
        await this.#sendAndRead(`WATCH ${pattern}\n`);
    }

    async ping() {
        return this.#sendAndRead("PING\n");
    }

    disconnect() {
        this.#closing = true;
        this.#socket?.destroy();
    }

    #onData(data) {
        this.#buffer += data.toString();
        const lines   = this.#buffer.split("\n");
        this.#buffer  = lines.pop();

        for (const line of lines) {
            if (!line.trim()) continue;
            if (line.startsWith("EVENT ")) {
                this.#dispatchEvent(line);
            } else if (this.#pendingResolve) {
                const resolve        = this.#pendingResolve;
                this.#pendingResolve = null;
                resolve(line.trim());
            }
        }
    }

    #dispatchEvent(line) {
        const rest    = line.slice(6);
        const spaceAt = rest.indexOf(" ");
        const stream  = rest.slice(0, spaceAt);
        const payload = JSON.parse(rest.slice(spaceAt + 1));

        for (const [pattern, handler] of this.#handlers) {
            if (matchesPattern(pattern, stream)) {
                handler({ stream, payload });
            }
        }
    }

    #sendAndRead(line) {
        return new Promise((resolve) => {
            this.#pendingResolve = resolve;
            this.#socket.write(line);
        });
    }

    async #onClose() {
        if (this.#closing) return;
        console.log(`[saturn] disconnected — reconnecting in ${this.#reconnectDelay}ms`);
        await sleep(this.#reconnectDelay);
        this.#reconnectDelay = Math.min(this.#reconnectDelay * 2, 10000);
        try {
            await this.connect();
            for (const pattern of this.#handlers.keys()) {
                await this.#sendAndRead(`WATCH ${pattern}\n`);
            }
            console.log("[saturn] reconnected");
        } catch {
            this.#onClose();
        }
    }
}

function matchesPattern(pattern, stream) {
    if (pattern === stream) return true;
    if (pattern.endsWith("*")) return stream.startsWith(pattern.slice(0, -1));
    return false;
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

module.exports = { SaturnDBClient };
