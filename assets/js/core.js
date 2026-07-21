/**
 * ================================================================
 *  core.js - Core System untuk GTN MealSense
 *  Single Source of Truth, MQTT Manager, State Manager, Sync Engine
 * ================================================================
 *  Version: 3.0.1 (dengan perbaikan)
 *  Author: Muhammad Gatan Rifani
 *  License: Proprietary
 * ================================================================
 */

(function(global) {
    'use strict';

    // ================================================================
    // 1. KONFIGURASI DASAR (tidak berubah)
    // ================================================================

    var CONFIG = {
        MQTT_BROKER: 'wss://broker.emqx.io:8084/mqtt',
        MQTT_OPTIONS: {
            clientId: 'core_' + Date.now().toString(36) + '_' + Math.random().toString(36).slice(2, 6),
            clean: true,
            reconnectPeriod: 5000,
            connectTimeout: 10000,
            keepalive: 30,
            will: {
                topic: 'system/status',
                payload: JSON.stringify({ status: 'offline', timestamp: new Date().toISOString() }),
                qos: 1,
                retain: true
            }
        },
        MQTT_TOPICS: {
            SENSOR_SHT31: 'sensor/sht31',
            SENSOR_MQ135: 'sensor/mq135',
            SENSOR_STATUS: 'sensor/status',
            ACTUATOR_STATUS: 'actuator/status',
            ACTUATOR_CONTROL: 'actuator/control',
            SYSTEM_STATE: 'system/state',
            SYSTEM_CONTROL: 'system/control',
            SYSTEM_STATUS: 'system/status',
            SYSTEM_ALARM: 'system/alarm',
            SYSTEM_HEARTBEAT: 'system/heartbeat',
            FUZZY_STATUS: 'fuzzy/status',
            FUZZY_CONTROL: 'fuzzy/control',
            SETTING_UPDATE: 'setting/update',
            SELF_MONITOR: 'system/selfmonitor',
            TELEGRAM_LOG: 'telegram/log'
        },
        SPREADSHEET_API: 'https://script.google.com/macros/s/AKfycbxKOjtJQ4kF4biMhOo9kjWTwtlW82d48nsvFi1qzS9qz2GEsuZhudHSDgdyxQPlaemXXw/exec',
        PUBLISH_INTERVAL: 2000,
        HEARTBEAT_INTERVAL: 10000,
        SPREADSHEET_HEALTH_INTERVAL: 30000,
        MQTT_HEARTBEAT_INTERVAL: 10000,
        STATE_SYNC_INTERVAL: 2000,
        MAX_RECONNECT_ATTEMPTS: 10,
        RECONNECT_BASE_DELAY: 1000,
        MAX_OFFLINE_QUEUE: 100,
        DEBUG: true,
    };

    // ================================================================
    // 2. STATE (tidak berubah)
    // ================================================================

    var State = { /* ... sama seperti sebelumnya ... */ };

    // ================================================================
    // 3. MQTT MANAGER – PERBAIKAN di onConnect
    // ================================================================

    var MqttManager = {
        // ... properti lainnya sama ...

        // --- Event Handlers ---
        onConnect: function() {
            this.isConnected = true;
            this.isConnecting = false;
            this.reconnectAttempts = 0;
            State.mqttStatus = 'online';
            State.isMqttConnected = true;

            this.log('Connected successfully');

            // Resubscribe ke semua topik yang terdaftar di messageHandlers
            var topics = Object.keys(this.messageHandlers);
            if (topics.length > 0) {
                this.client.subscribe(topics, { qos: 1 }, function(err) {
                    if (err) {
                        this.log('Resubscribe error:', err);
                    } else {
                        this.log('Resubscribed to', topics.length, 'topics');
                    }
                }.bind(this));
            }

            // ===== PERBAIKAN: Subscribe ke semua topik yang diperlukan =====
            // Daftar topik yang harus selalu disubscribe agar dashboard bisa menerima data
            var requiredTopics = [
                CONFIG.MQTT_TOPICS.SENSOR_SHT31,
                CONFIG.MQTT_TOPICS.SENSOR_MQ135,
                CONFIG.MQTT_TOPICS.ACTUATOR_STATUS,
                CONFIG.MQTT_TOPICS.FUZZY_STATUS,
                CONFIG.MQTT_TOPICS.SYSTEM_ALARM,
                CONFIG.MQTT_TOPICS.SELF_MONITOR,
                CONFIG.MQTT_TOPICS.SYSTEM_HEARTBEAT,   // <-- tambahan
                CONFIG.MQTT_TOPICS.SYSTEM_STATUS,      // <-- tambahan
                CONFIG.MQTT_TOPICS.SYSTEM_STATE,       // <-- untuk sinkronisasi multi-user
                CONFIG.MQTT_TOPICS.SETTING_UPDATE,     // <-- untuk menerima perubahan setting
            ];

            // Subscribe ke topik yang belum ada di messageHandlers (agar tidak dobel)
            var toSubscribe = requiredTopics.filter(function(t) {
                return !this.messageHandlers[t];
            }.bind(this));

            if (toSubscribe.length > 0) {
                this.client.subscribe(toSubscribe, { qos: 1 }, function(err) {
                    if (err) {
                        this.log('Subscribe to required topics error:', err);
                    } else {
                        this.log('Subscribed to required topics:', toSubscribe.join(', '));
                    }
                }.bind(this));
            }
            // ===== SAMPAI SINI =====

            this.processQueue();

            this.publish(CONFIG.MQTT_TOPICS.SYSTEM_STATUS, JSON.stringify({
                status: 'online',
                timestamp: new Date().toISOString(),
                clientId: CONFIG.MQTT_OPTIONS.clientId,
            }), { qos: 1, retain: true });

            this.publishState();

            this.updateStatus();
            this.notifySubscribers('connect', { connected: true });
        },

        // ... sisanya sama ...
    };

    // ================================================================
    // 4. STATE MANAGER – PERBAIKAN di handleMqttMessage (SELF_MONITOR)
    // ================================================================

    var StateManager = {
        // ... properti sama ...

        handleMqttMessage: function(topic, data) {
            var updates = {};

            switch (topic) {
                // ... case lain sama ...

                case CONFIG.MQTT_TOPICS.SELF_MONITOR:
                    // ===== PERBAIKAN: handle komponen dengan lebih aman =====
                    if (data.components && typeof data.components === 'object') {
                        var comps = this.state.components;
                        for (var key in data.components) {
                            if (data.components.hasOwnProperty(key)) {
                                // Jika komponen belum ada di state, buat entri baru
                                if (!comps[key]) {
                                    comps[key] = {
                                        status: 'online',
                                        label: key,
                                        lastSeen: null
                                    };
                                }
                                comps[key].status = data.components[key].status || 'online';
                                comps[key].lastSeen = new Date();
                            }
                        }
                    }
                    if (data.health !== undefined) {
                        updates['health'] = data.health;
                    }
                    break;

                default:
                    break;
            }

            if (Object.keys(updates).length > 0) {
                this.update(updates, { publish: false, sync: false, silent: true });
            }
        },

        // ... sisanya sama ...
    };

    // ================================================================
    // 5. SPREADSHEET MANAGER – PERBAIKAN logData
    // ================================================================

    var SpreadsheetManager = {
        // ... properti sama ...

        // --- Log Data to Spreadsheet ---
        logData: function(data) {
            if (!this.isConfigured) {
                console.warn('[Spreadsheet] Not configured, data not logged');
                return;
            }
            var apiUrl = CONFIG.SPREADSHEET_API;
            var payload = {
                timestamp: new Date().toISOString(),
                temperature: data.temperature || State.temperature || 0,
                humidity: data.humidity || State.humidity || 0,
                nh3: data.nh3 || State.nh3 || 0,
                peltierPwm: data.peltierPwm || State.peltierPwm || 0,
                fanPwm: data.fanPwm || State.fanPwm || 0,
                coolFanPwm: data.coolFanPwm || State.coolFanPwm || 0,
                kp: data.kp || State.kp || 0,
                ki: data.ki || State.ki || 0,
                kd: data.kd || State.kd || 0,
                mode: data.mode || State.mode || 'AUTO',
                alarm: State.alarms.filter(function(a) { return a.active; }).map(function(a) { return a.message; }).join(','),
                status: State.health > 80 ? 'Normal' : State.health > 60 ? 'Warning' : 'Danger',
            };
            console.log('[Spreadsheet] Logging data:', payload);

            // ===== PERBAIKAN: Hanya gunakan mode 'cors' dan tangani error dengan baik =====
            // Menghapus fallback no-cors karena tidak memberikan feedback yang berarti
            fetch(apiUrl, {
                method: 'POST',
                mode: 'cors',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload),
            })
            .then(function(response) {
                if (!response.ok) {
                    throw new Error('HTTP ' + response.status + ' - ' + response.statusText);
                }
                return response.text(); // atau response.json() jika server merespon JSON
            })
            .then(function(text) {
                console.log('[Spreadsheet] Log success:', text);
            })
            .catch(function(error) {
                console.warn('[Spreadsheet] Log error:', error.message);
                // Jangan ulangi secara otomatis, biarkan interval berikutnya mencoba lagi
            });
        },

        // ... sisanya sama ...
    };

    // ================================================================
    // 6. TELEGRAM MANAGER (tidak berubah)
    // 7. BUZZER MANAGER (tidak berubah)
    // 8. CORE INIT (tidak berubah)
    // 9. EXPOSE GLOBAL (tidak berubah)
    // ================================================================

    // ... kode sisanya tetap sama ...

})(window);