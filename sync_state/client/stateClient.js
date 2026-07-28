// client/stateClient.js
export class StateClient {
    constructor(url, idKey = 'id', clientId = null) {
        this.url = url;
        this.idKey = idKey;
        this.localVersions = {};
        this.manifestVersions = {};
        this.store = {};
        this.schemaVersion = null;
        this.clientId = clientId || this._generateClientId();

        this.eventSource = null;
        this.isConnecting = false;
        this.isReconnecting = false;
        this.reconnectTimer = null;

        this.callbacks = {
            onDelta: null,
            onManifest: null,
            onError: null,
            onSchemaMismatch: null,
            onCommandAck: null,
        };
    }

    _generateClientId() {
        return 'client_' + Math.random().toString(36).substring(2, 10);
    }

    setCallbacks(callbacks) {
        Object.assign(this.callbacks, callbacks);
    }

    connect() {
        if (this.eventSource || this.isConnecting) return;
        this.isConnecting = true;

        const versionParam = encodeURIComponent(JSON.stringify(this.localVersions));
        const streamUrl = `${this.url}?versions=${versionParam}&client_id=${this.clientId}`;

        const es = new EventSource(streamUrl);
        this.eventSource = es;

        es.addEventListener('open', () => {
            this.isConnecting = false;
            this.isReconnecting = false;
        });

        es.addEventListener('manifest', (e) => {
            try {
                const manifest = JSON.parse(e.data);
                this._processManifest(manifest);
            } catch (err) {
                console.error('[StateClient] Manifest parse error:', err);
            }
        });

        es.addEventListener('delta', (e) => {
            try {
                const delta = JSON.parse(e.data);
                this._applyDelta(delta);
                if (this.callbacks.onDelta) {
                    this.callbacks.onDelta(delta);
                }
            } catch (err) {
                console.error('[StateClient] Delta parse error:', err);
            }
        });

        // New: command_ack
        es.addEventListener('command_ack', (e) => {
            try {
                const ack = JSON.parse(e.data);
                if (this.callbacks.onCommandAck) {
                    this.callbacks.onCommandAck(ack);
                }
            } catch (err) {
                console.error('[StateClient] Command ack parse error:', err);
            }
        });

        es.addEventListener('error', (e) => {
            // SSE error may contain fault data
            if (e.data) {
                try {
                    const errData = JSON.parse(e.data);
                    if (errData.fault) {
                        console.error('[StateClient] Fault detected:', errData);
                        if (this.callbacks.onError) {
                            this.callbacks.onError({ fault: true, ...errData });
                        }
                        this.disconnect();
                        return;
                    }
                } catch (_) {}
            }
            // Otherwise treat as normal disconnect
            this._handleDisconnect(e);
        });

        es.addEventListener('keepalive', () => {});
    }

    // New: send command
    async sendCommand(action, params, commandId = null) {
        if (!commandId) {
            commandId = crypto.randomUUID ? crypto.randomUUID() : Math.random().toString(36);
        }
        const url = `${this.url}/command?client_id=${this.clientId}`;
        const response = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ command_id: commandId, action, params })
        });
        if (!response.ok) {
            const err = await response.text();
            throw new Error(`Command failed: ${response.status} - ${err}`);
        }
        return await response.json();  // immediate receipt
    }

    disconnect() {
        this.isConnecting = false;
        this.isReconnecting = false;
        if (this.reconnectTimer) {
            clearTimeout(this.reconnectTimer);
            this.reconnectTimer = null;
        }
        if (this.eventSource) {
            this.eventSource.close();
            this.eventSource = null;
        }
    }

    _handleDisconnect(err) {
        this.disconnect();
        if (this.callbacks.onError) {
            this.callbacks.onError(err);
        }

        if (!this.isReconnecting) {
            this.isReconnecting = true;
            this.reconnectTimer = setTimeout(() => {
                this.reconnectTimer = null;
                this.connect();
            }, 2000);
        }
    }

    _processManifest(manifest) {
        if (manifest.schema_version !== undefined) {
            if (this.schemaVersion === null) {
                this.schemaVersion = manifest.schema_version;
            } else if (this.schemaVersion !== manifest.schema_version) {
                console.warn('[StateClient] Schema version mismatch detected.');
                if (this.callbacks.onSchemaMismatch) {
                    this.callbacks.onSchemaMismatch(manifest.schema_version, this.schemaVersion);
                }
            }
        }

        if (Array.isArray(manifest.types)) {
            const activeTypes = new Set(manifest.types);
            for (const type in this.store) {
                if (!activeTypes.has(type)) {
                    delete this.store[type];
                    delete this.localVersions[type];
                    delete this.manifestVersions[type];
                }
            }
        }

        if (manifest.versions) {
            for (const [type, serverVer] of Object.entries(manifest.versions)) {
                this.manifestVersions[type] = serverVer;
                if ((this.localVersions[type] || 0) > serverVer) {
                    this.localVersions[type] = 0;
                }
            }
        }

        if (this.callbacks.onManifest) {
            this.callbacks.onManifest(manifest);
        }
    }

    _applyDelta(delta) {
        const type = delta.type;
        if (!type) return;

        const currentLocalVersion = this.localVersions[type] || 0;

        if (!delta.full && delta.version <= currentLocalVersion) {
            console.warn(`[StateClient] Dropped stale delta for ${type}. Current: ${currentLocalVersion}, Incoming: ${delta.version}`);
            return;
        }

        if (!this.store[type]) {
            this.store[type] = {};
        }

        if (delta.full) {
            this.store[type] = {};
            if (Array.isArray(delta.added)) {
                for (const entity of delta.added) {
                    if (entity && entity[this.idKey] !== undefined) {
                        this.store[type][entity[this.idKey]] = entity;
                    }
                }
            }
            this.localVersions[type] = delta.version;
            return;
        }

        if (Array.isArray(delta.deleted)) {
            for (const id of delta.deleted) {
                delete this.store[type][id];
            }
        }

        if (Array.isArray(delta.added)) {
            for (const entity of delta.added) {
                if (entity && entity[this.idKey] !== undefined) {
                    this.store[type][entity[this.idKey]] = entity;
                }
            }
        }

        if (Array.isArray(delta.updated)) {
            for (const entity of delta.updated) {
                if (entity && entity[this.idKey] !== undefined) {
                    this.store[type][entity[this.idKey]] = entity;
                }
            }
        }

        this.localVersions[type] = delta.version;
    }

    getState() {
        return { ...this.store };
    }

    getEntities(type) {
        return Object.values(this.store[type] || {});
    }

    getEntity(type, id) {
        return (this.store[type] || {})[id] || null;
    }
}