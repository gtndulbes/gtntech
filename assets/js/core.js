/**
 * ================================================================
 *  core.js - Core System untuk GTN MealSense
 *  Single Source of Truth, MQTT Manager, State Manager, Sync Engine
 * ================================================================
 *  Version: 3.0.0
 *  Author: Muhammad Gatan Rifani
 *  License: Proprietary
 * ================================================================
 */

(function(global) {
    'use strict';

    // ================================================================
    // 1. KONFIGURASI DASAR
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
            // Sensor
            SENSOR_SHT31: 'sensor/sht31',
            SENSOR_MQ135: 'sensor/mq135',
            SENSOR_STATUS: 'sensor/status',

            // Aktuator
            ACTUATOR_STATUS: 'actuator/status',
            ACTUATOR_CONTROL: 'actuator/control',

            // Sistem
            SYSTEM_STATE: 'system/state',
            SYSTEM_CONTROL: 'system/control',
            SYSTEM_STATUS: 'system/status',
            SYSTEM_ALARM: 'system/alarm',
            SYSTEM_HEARTBEAT: 'system/heartbeat',

            // Fuzzy PID
            FUZZY_STATUS: 'fuzzy/status',
            FUZZY_CONTROL: 'fuzzy/control',

            // Setting
            SETTING_UPDATE: 'setting/update',

            // Self Monitoring
            SELF_MONITOR: 'system/selfmonitor',

            // Telegram
            TELEGRAM_LOG: 'telegram/log',
        },
        SPREADSHEET_API: 'https://script.google.com/macros/s/AKfycbzbndPNy1970Id-boAcqgr9QC12BexNdGb5_xuqCZQ6wxpxTxWfNjKEYgRHtHIZ2eocqw/exec', // 
        PUBLISH_INTERVAL: 2000,
        HEARTBEAT_INTERVAL: 10000,
        SPREADSHEET_HEALTH_INTERVAL: 30000,
        MQTT_HEARTBEAT_INTERVAL: 10000,
        STATE_SYNC_INTERVAL: 2000,
        MAX_RECONNECT_ATTEMPTS: 10,
        RECONNECT_BASE_DELAY: 1000,
        MAX_OFFLINE_QUEUE: 100,
    };

    // ================================================================
    // 2. STATE - Single Source of Truth
    // ================================================================

    var State = {
        // --- Sensor ---
        temperature: null,
        humidity: null,
        nh3: null,
        temperatureValid: false,
        humidityValid: false,
        nh3Valid: false,

        // --- Aktuator ---
        peltierOn: false,
        peltierPwm: 0,
        peltierMode: 'standby', // 'cooling' | 'heating' | 'standby'
        fanOn: false,
        fanPwm: 0,
        coolFanOn: false,
        coolFanPwm: 0,
        mode: 'AUTO', // 'AUTO' | 'MANUAL'

        // --- Fuzzy PID ---
        setpointTemp: 27.5,
        setpointHum: 75,
        kp: 2.0,
        ki: 0.5,
        kd: 0.1,
        error: 0,
        deltaError: 0,
        outputPid: 0,

        // --- Status Sistem ---
        health: 100,
        uptime: 0,
        esp32Status: 'online',
        wifiStatus: 'online',
        mqttStatus: 'online',
        spreadsheetStatus: 'online',
        lastHeartbeat: null,
        lastStateUpdate: null,

        // --- Alarm & Error ---
        alarms: [],
        errors: [],
        alarmCount: 0,
        errorCount: 0,

        // --- Self Monitoring ---
        components: {
            esp32: { status: 'online', label: 'ESP32', lastSeen: null },
            wifi: { status: 'online', label: 'WiFi', lastSeen: null },
            mqtt: { status: 'online', label: 'MQTT Broker', lastSeen: null },
            spreadsheet: { status: 'online', label: 'Spreadsheet', lastSeen: null },
            sht31: { status: 'online', label: 'Sensor SHT31-D', lastSeen: null },
            mq135: { status: 'online', label: 'Sensor MQ-135', lastSeen: null },
            peltier: { status: 'online', label: 'Modul Peltier', lastSeen: null },
            fan: { status: 'online', label: 'Kipas Sirkulasi', lastSeen: null },
            coolFan: { status: 'online', label: 'Kipas Pendingin', lastSeen: null },
            currentSense: { status: 'online', label: 'Current Sense', lastSeen: null },
            i2c: { status: 'online', label: 'Bus I²C', lastSeen: null },
            adc: { status: 'online', label: 'ADC ESP32', lastSeen: null },
            watchdog: { status: 'online', label: 'Watchdog Timer', lastSeen: null },
            autoReconnect: { status: 'online', label: 'Auto Reconnect', lastSeen: null },
        },

        // --- Telemetry ---
        telegramEnabled: true,
        telegramLastSent: null,
        telegramQueue: [],

        // --- Settings ---
        settings: {
            samplingInterval: 2,
            loggingInterval: 30,
            defaultMode: 'AUTO',
            timezone: 'Asia/Jakarta',
            alarmTempMax: 32,
            alarmTempMin: 24,
            alarmHumMax: 90,
            alarmHumMin: 60,
            alarmNh3Max: 25,
            alarmDelay: 10,
        },

        // --- System ---
        isInitialized: false,
        isMqttConnected: false,
        isSpreadsheetConnected: false,
        dataReceived: false,
        reconnectAttempts: 0,
        lastReconnectTime: null,
        offlineQueue: [],
        subscribers: [],
    };

    // ================================================================
    // 3. MQTT MANAGER
    // ================================================================

    var MqttManager = {
        client: null,
        isConnected: false,
        isConnecting: false,
        reconnectAttempts: 0,
        reconnectTimer: null,
        heartbeatTimer: null,
        stateSyncTimer: null,
        offlineQueue: [],
        isProcessingQueue: false,
        subscribers: [],
        messageHandlers: {},

        // --- Init ---
        init: function() {
            if (this.client) {
                console.warn('[MQTT] Already initialized');
                return;
            }
            console.log('[MQTT] Initializing...');
            this.connect();
            this.setupHeartbeat();
            this.setupStateSync();
        },

        // --- Connect ---
        connect: function() {
            if (this.isConnecting) {
                console.log('[MQTT] Already connecting...');
                return;
            }
            if (this.client && this.isConnected) {
                console.log('[MQTT] Already connected');
                return;
            }

            this.isConnecting = true;
            console.log('[MQTT] Connecting to:', CONFIG.MQTT_BROKER);

            try {
                var options = Object.assign({}, CONFIG.MQTT_OPTIONS);
                options.clientId = 'core_' + Date.now().toString(36) + '_' + Math.random().toString(36).slice(2, 6);

                this.client = mqtt.connect(CONFIG.MQTT_BROKER, options);

                this.client.on('connect', this.onConnect.bind(this));
                this.client.on('reconnect', this.onReconnect.bind(this));
                this.client.on('close', this.onClose.bind(this));
                this.client.on('offline', this.onOffline.bind(this));
                this.client.on('error', this.onError.bind(this));
                this.client.on('message', this.onMessage.bind(this));
                this.client.on('packetsend', this.onPacketSend.bind(this));
                this.client.on('packetreceive', this.onPacketReceive.bind(this));

            } catch (error) {
                console.error('[MQTT] Connection error:', error);
                this.isConnecting = false;
                this.scheduleReconnect();
            }
        },

        // --- Disconnect ---
        disconnect: function() {
            if (this.client) {
                try {
                    this.client.end(true);
                } catch (e) {
                    console.warn('[MQTT] Disconnect error:', e);
                }
                this.client = null;
            }
            this.isConnected = false;
            this.isConnecting = false;
            this.updateStatus();
            console.log('[MQTT] Disconnected');
        },

        // --- Reconnect with Exponential Backoff ---
        scheduleReconnect: function() {
            if (this.reconnectTimer) {
                clearTimeout(this.reconnectTimer);
                this.reconnectTimer = null;
            }

            if (this.reconnectAttempts >= CONFIG.MAX_RECONNECT_ATTEMPTS) {
                console.error('[MQTT] Max reconnect attempts reached, giving up');
                State.mqttStatus = 'offline';
                this.updateStatus();
                return;
            }

            var delay = CONFIG.RECONNECT_BASE_DELAY * Math.pow(1.5, this.reconnectAttempts);
            delay = Math.min(delay, 60000); // Max 60 seconds

            console.log('[MQTT] Scheduling reconnect in', delay / 1000, 'seconds (attempt', this.reconnectAttempts + 1,
                ')');

            this.reconnectTimer = setTimeout(function() {
                this.reconnectAttempts++;
                this.isConnecting = false;
                this.connect();
            }.bind(this), delay);
        },

        // --- Heartbeat ---
        setupHeartbeat: function() {
            if (this.heartbeatTimer) {
                clearInterval(this.heartbeatTimer);
                this.heartbeatTimer = null;
            }

            this.heartbeatTimer = setInterval(function() {
                if (this.isConnected) {
                    var heartbeat = {
                        timestamp: new Date().toISOString(),
                        status: 'online',
                        uptime: Math.floor(performance.now() / 1000),
                        freeHeap: 0, // Will be filled by ESP32
                        clientId: CONFIG.MQTT_OPTIONS.clientId,
                    };
                    this.publish(CONFIG.MQTT_TOPICS.SYSTEM_HEARTBEAT, JSON.stringify(heartbeat), { qos: 1, retain: false });
                    console.log('[MQTT] Heartbeat sent');
                }
            }.bind(this), CONFIG.MQTT_HEARTBEAT_INTERVAL);
        },

        // --- State Sync ---
        setupStateSync: function() {
            if (this.stateSyncTimer) {
                clearInterval(this.stateSyncTimer);
                this.stateSyncTimer = null;
            }

            this.stateSyncTimer = setInterval(function() {
                if (this.isConnected) {
                    this.publishState();
                }
            }.bind(this), CONFIG.STATE_SYNC_INTERVAL);
        },

        // --- Publish State ---
        publishState: function() {
            var state = StateManager.getState();
            var payload = JSON.stringify(state);
            this.publish(CONFIG.MQTT_TOPICS.SYSTEM_STATE, payload, { qos: 1, retain: true });
        },

        // --- Publish with Offline Queue ---
        publish: function(topic, message, options) {
            options = options || { qos: 1, retain: false };

            if (this.isConnected && this.client) {
                this.client.publish(topic, message, options, function(err) {
                    if (err) {
                        console.warn('[MQTT] Publish error:', err);
                        this.queueMessage(topic, message, options);
                    }
                }.bind(this));
                return true;
            } else {
                this.queueMessage(topic, message, options);
                return false;
            }
        },

        // --- Queue Message for Offline ---
        queueMessage: function(topic, message, options) {
            if (this.offlineQueue.length >= CONFIG.MAX_OFFLINE_QUEUE) {
                this.offlineQueue.shift();
            }
            this.offlineQueue.push({
                topic: topic,
                message: message,
                options: options,
                timestamp: Date.now()
            });
            console.log('[MQTT] Queued message (queue size:', this.offlineQueue.length, ')');
            this.processQueue();
        },

        // --- Process Offline Queue ---
        processQueue: function() {
            if (this.isProcessingQueue || !this.isConnected || this.offlineQueue.length === 0) {
                return;
            }

            this.isProcessingQueue = true;
            console.log('[MQTT] Processing offline queue (', this.offlineQueue.length, 'messages)');

            var batch = this.offlineQueue.splice(0, 10);
            var processed = 0;

            batch.forEach(function(msg) {
                if (this.client) {
                    this.client.publish(msg.topic, msg.message, msg.options, function(err) {
                        if (err) {
                            console.warn('[MQTT] Queue publish error:', err);
                            this.offlineQueue.push(msg);
                        } else {
                            processed++;
                        }
                    }.bind(this));
                } else {
                    this.offlineQueue.push(msg);
                }
            }.bind(this));

            this.isProcessingQueue = false;

            if (this.offlineQueue.length > 0) {
                setTimeout(function() {
                    this.processQueue();
                }.bind(this), 1000);
            }

            if (processed > 0) {
                console.log('[MQTT] Processed', processed, 'queued messages');
            }
        },

        // --- Subscribe ---
        subscribe: function(topic, callback) {
            if (typeof topic === 'string') {
                topic = [topic];
            }

            topic.forEach(function(t) {
                if (!this.messageHandlers[t]) {
                    this.messageHandlers[t] = [];
                }
                this.messageHandlers[t].push(callback);

                if (this.isConnected && this.client) {
                    this.client.subscribe(t, { qos: 1 }, function(err) {
                        if (err) {
                            console.warn('[MQTT] Subscribe error for', t, ':', err);
                        } else {
                            console.log('[MQTT] Subscribed to:', t);
                        }
                    });
                }
            }.bind(this));
        },

        // --- Unsubscribe ---
        unsubscribe: function(topic, callback) {
            if (typeof topic === 'string') {
                topic = [topic];
            }

            topic.forEach(function(t) {
                if (this.messageHandlers[t]) {
                    if (callback) {
                        var index = this.messageHandlers[t].indexOf(callback);
                        if (index !== -1) {
                            this.messageHandlers[t].splice(index, 1);
                        }
                    } else {
                        delete this.messageHandlers[t];
                    }
                }

                if (this.isConnected && this.client) {
                    this.client.unsubscribe(t, function(err) {
                        if (err) {
                            console.warn('[MQTT] Unsubscribe error for', t, ':', err);
                        }
                    });
                }
            }.bind(this));
        },

        // --- Event Handlers ---
        onConnect: function() {
            this.isConnected = true;
            this.isConnecting = false;
            this.reconnectAttempts = 0;
            State.mqttStatus = 'online';
            State.isMqttConnected = true;

            console.log('[MQTT] Connected successfully');

            // Resubscribe to all topics
            var topics = Object.keys(this.messageHandlers);
            if (topics.length > 0) {
                this.client.subscribe(topics, { qos: 1 }, function(err) {
                    if (err) {
                        console.warn('[MQTT] Resubscribe error:', err);
                    } else {
                        console.log('[MQTT] Resubscribed to', topics.length, 'topics');
                    }
                });
            }

            // Process offline queue
            this.processQueue();

            // Publish online status
            this.publish(CONFIG.MQTT_TOPICS.SYSTEM_STATUS, JSON.stringify({
                status: 'online',
                timestamp: new Date().toISOString(),
                clientId: CONFIG.MQTT_OPTIONS.clientId,
            }), { qos: 1, retain: true });

            // Publish current state
            this.publishState();

            this.updateStatus();
            this.notifySubscribers('connect', { connected: true });
        },

        onReconnect: function() {
            console.log('[MQTT] Reconnecting...');
            this.isConnecting = true;
            this.notifySubscribers('reconnect', { attempt: this.reconnectAttempts });
        },

        onClose: function() {
            console.log('[MQTT] Connection closed');
            this.isConnected = false;
            this.isConnecting = false;
            State.mqttStatus = 'offline';
            State.isMqttConnected = false;
            this.updateStatus();
            this.notifySubscribers('close', { connected: false });
            this.scheduleReconnect();
        },

        onOffline: function() {
            console.warn('[MQTT] Offline');
            this.isConnected = false;
            State.mqttStatus = 'offline';
            State.isMqttConnected = false;
            this.updateStatus();
            this.notifySubscribers('offline', { connected: false });
        },

        onError: function(error) {
            console.error('[MQTT] Error:', error);
            State.mqttStatus = 'error';
            this.updateStatus();
            this.notifySubscribers('error', { error: error });
        },

        onMessage: function(topic, message) {
            try {
                var payload = message.toString();
                var data = JSON.parse(payload);

                // Update state based on topic
                StateManager.handleMqttMessage(topic, data);

                // Notify handlers
                if (this.messageHandlers[topic]) {
                    this.messageHandlers[topic].forEach(function(handler) {
                        try {
                            handler(data, topic);
                        } catch (e) {
                            console.error('[MQTT] Handler error for', topic, ':', e);
                        }
                    });
                }

                // Notify generic subscribers
                this.notifySubscribers('message', { topic: topic, data: data });

            } catch (error) {
                console.warn('[MQTT] Message parse error for', topic, ':', error);
            }
        },

        onPacketSend: function(packet) {
            // Silently track sends
        },

        onPacketReceive: function(packet) {
            // Silently track receives
        },

        // --- Update Status ---
        updateStatus: function() {
            State.mqttStatus = this.isConnected ? 'online' : (this.isConnecting ? 'connecting' : 'offline');
            State.isMqttConnected = this.isConnected;
            this.notifySubscribers('statusChange', {
                connected: this.isConnected,
                status: State.mqttStatus
            });
        },

        // --- Subscribers ---
        subscribeSystem: function(callback) {
            if (typeof callback === 'function') {
                this.subscribers.push(callback);
            }
        },

        unsubscribeSystem: function(callback) {
            var index = this.subscribers.indexOf(callback);
            if (index !== -1) {
                this.subscribers.splice(index, 1);
            }
        },

        notifySubscribers: function(event, data) {
            this.subscribers.forEach(function(callback) {
                try {
                    callback(event, data);
                } catch (e) {
                    console.error('[MQTT] Subscriber error:', e);
                }
            });
        },

        // --- Get Status ---
        getStatus: function() {
            return {
                connected: this.isConnected,
                connecting: this.isConnecting,
                status: State.mqttStatus,
                reconnectAttempts: this.reconnectAttempts,
                offlineQueueSize: this.offlineQueue.length,
                messageHandlers: Object.keys(this.messageHandlers).length,
            };
        },
    };

    // ================================================================
    // 4. STATE MANAGER
    // ================================================================

    var StateManager = {
        state: State,
        listeners: [],
        updateQueue: [],
        isProcessing: false,

        // --- Get State ---
        getState: function() {
            return this.state;
        },

        // --- Get Specific State ---
        get: function(key) {
            var keys = key.split('.');
            var value = this.state;
            for (var i = 0; i < keys.length; i++) {
                if (value && typeof value === 'object' && keys[i] in value) {
                    value = value[keys[i]];
                } else {
                    return undefined;
                }
            }
            return value;
        },

        // --- Update State ---
        set: function(key, value, options) {
            options = options || { publish: true, sync: true, silent: false };

            var keys = key.split('.');
            var target = this.state;
            var lastKey = keys.pop();

            for (var i = 0; i < keys.length; i++) {
                if (target[keys[i]] === undefined) {
                    target[keys[i]] = {};
                }
                target = target[keys[i]];
            }

            var oldValue = target[lastKey];
            if (oldValue === value) {
                return;
            }

            target[lastKey] = value;
            this.state.lastStateUpdate = new Date();

            if (!options.silent) {
                this.notifyListeners(key, value, oldValue);
            }

            if (options.publish) {
                this.publishStateUpdate(key, value, options);
            }

            if (options.sync) {
                MqttManager.publishState();
            }
        },

        // --- Batch Update ---
        update: function(updates, options) {
            options = options || { publish: true, sync: true, silent: false };

            this.updateQueue.push({
                updates: updates,
                options: options
            });

            if (!this.isProcessing) {
                this.processUpdates();
            }
        },

        processUpdates: function() {
            if (this.updateQueue.length === 0) {
                this.isProcessing = false;
                return;
            }

            this.isProcessing = true;
            var batch = this.updateQueue.shift();
            var changed = false;

            for (var key in batch.updates) {
                if (batch.updates.hasOwnProperty(key)) {
                    var value = batch.updates[key];
                    var keys = key.split('.');
                    var target = this.state;
                    var lastKey = keys.pop();

                    for (var i = 0; i < keys.length; i++) {
                        if (target[keys[i]] === undefined) {
                            target[keys[i]] = {};
                        }
                        target = target[keys[i]];
                    }

                    if (target[lastKey] !== value) {
                        target[lastKey] = value;
                        changed = true;
                        if (!batch.options.silent) {
                            this.notifyListeners(key, value, target[lastKey]);
                        }
                    }
                }
            }

            if (changed) {
                this.state.lastStateUpdate = new Date();

                if (batch.options.publish) {
                    MqttManager.publishState();
                }

                if (batch.options.sync) {
                    MqttManager.publishState();
                }
            }

            // Process next batch
            setTimeout(function() {
                this.processUpdates();
            }.bind(this), 10);
        },

        // --- Handle MQTT Message ---
        handleMqttMessage: function(topic, data) {
            var updates = {};

            switch (topic) {
                case CONFIG.MQTT_TOPICS.SENSOR_SHT31:
                    if (data.temperature !== undefined) {
                        updates['temperature'] = data.temperature;
                        updates['temperatureValid'] = true;
                    }
                    if (data.humidity !== undefined) {
                        updates['humidity'] = data.humidity;
                        updates['humidityValid'] = true;
                    }
                    this.state.dataReceived = true;
                    break;

                case CONFIG.MQTT_TOPICS.SENSOR_MQ135:
                    if (data.nh3 !== undefined) {
                        updates['nh3'] = data.nh3;
                        updates['nh3Valid'] = true;
                    }
                    this.state.dataReceived = true;
                    break;

                case CONFIG.MQTT_TOPICS.ACTUATOR_STATUS:
                    if (data.peltierOn !== undefined) updates['peltierOn'] = data.peltierOn;
                    if (data.peltierPwm !== undefined) updates['peltierPwm'] = data.peltierPwm;
                    if (data.fanOn !== undefined) updates['fanOn'] = data.fanOn;
                    if (data.fanPwm !== undefined) updates['fanPwm'] = data.fanPwm;
                    if (data.coolFanOn !== undefined) updates['coolFanOn'] = data.coolFanOn;
                    if (data.coolFanPwm !== undefined) updates['coolFanPwm'] = data.coolFanPwm;
                    if (data.mode !== undefined) updates['mode'] = data.mode;
                    break;

                case CONFIG.MQTT_TOPICS.FUZZY_STATUS:
                    if (data.setpointTemp !== undefined) updates['setpointTemp'] = data.setpointTemp;
                    if (data.setpointHum !== undefined) updates['setpointHum'] = data.setpointHum;
                    if (data.kp !== undefined) updates['kp'] = data.kp;
                    if (data.ki !== undefined) updates['ki'] = data.ki;
                    if (data.kd !== undefined) updates['kd'] = data.kd;
                    if (data.error !== undefined) updates['error'] = data.error;
                    if (data.deltaError !== undefined) updates['deltaError'] = data.deltaError;
                    if (data.output !== undefined) updates['outputPid'] = data.output;
                    break;

                case CONFIG.MQTT_TOPICS.SYSTEM_STATE:
                    // State sync from other clients
                    this.syncState(data);
                    break;

                case CONFIG.MQTT_TOPICS.SYSTEM_HEARTBEAT:
                    if (data.status === 'online') {
                        this.state.esp32Status = 'online';
                        this.state.lastHeartbeat = new Date();
                    }
                    break;

                case CONFIG.MQTT_TOPICS.SYSTEM_ALARM:
                    if (data.alarm) {
                        var alarm = {
                            message: data.alarm,
                            active: data.active !== false,
                            timestamp: data.timestamp || new Date().toISOString(),
                            category: data.category || 'general',
                        };
                        if (alarm.active) {
                            this.state.alarms.unshift(alarm);
                            if (this.state.alarms.length > 50) this.state.alarms.pop();
                        } else {
                            var existing = this.state.alarms.find(function(a) { return a.message === alarm.message; });
                            if (existing) {
                                existing.active = false;
                            }
                        }
                        this.state.alarmCount = this.state.alarms.filter(function(a) { return a.active; }).length;
                    }
                    break;

                case CONFIG.MQTT_TOPICS.SELF_MONITOR:
                    if (data.components) {
                        for (var key in data.components) {
                            if (this.state.components[key]) {
                                this.state.components[key].status = data.components[key].status || 'online';
                                this.state.components[key].lastSeen = new Date();
                            }
                        }
                    }
                    if (data.health !== undefined) updates['health'] = data.health;
                    break;

                default:
                    break;
            }

            // Apply updates
            if (Object.keys(updates).length > 0) {
                this.update(updates, { publish: false, sync: false, silent: true });
            }
        },

        // --- Sync State from Other Clients ---
        syncState: function(data) {
            var updates = {};
            var keys = ['temperature', 'humidity', 'nh3', 'peltierOn', 'peltierPwm', 'fanOn', 'fanPwm', 'coolFanOn',
                'coolFanPwm', 'mode', 'setpointTemp', 'setpointHum', 'kp', 'ki', 'kd', 'error', 'deltaError', 'outputPid',
                'health'
            ];

            for (var i = 0; i < keys.length; i++) {
                var key = keys[i];
                if (data[key] !== undefined) {
                    updates[key] = data[key];
                }
            }

            if (Object.keys(updates).length > 0) {
                this.update(updates, { publish: false, sync: false, silent: true });
            }
        },

        // --- Publish State Update ---
        publishStateUpdate: function(key, value, options) {
            var payload = {};
            payload[key] = value;
            MqttManager.publish(CONFIG.MQTT_TOPICS.SYSTEM_STATE, JSON.stringify(payload), { qos: 1, retain: true });
        },

        // --- Listeners ---
        addListener: function(key, callback) {
            if (typeof callback !== 'function') return;
            if (!this.listeners[key]) {
                this.listeners[key] = [];
            }
            this.listeners[key].push(callback);
        },

        removeListener: function(key, callback) {
            if (this.listeners[key]) {
                var index = this.listeners[key].indexOf(callback);
                if (index !== -1) {
                    this.listeners[key].splice(index, 1);
                }
            }
        },

        notifyListeners: function(key, value, oldValue) {
            if (this.listeners[key]) {
                this.listeners[key].forEach(function(callback) {
                    try {
                        callback(value, oldValue);
                    } catch (e) {
                        console.error('[StateManager] Listener error:', e);
                    }
                });
            }

            // Wildcard listeners
            if (this.listeners['*']) {
                this.listeners['*'].forEach(function(callback) {
                    try {
                        callback(key, value, oldValue);
                    } catch (e) {
                        console.error('[StateManager] Wildcard listener error:', e);
                    }
                });
            }
        },

        // --- Reset State ---
        reset: function() {
            this.state = State;
            this.state.dataReceived = false;
            this.state.isInitialized = true;
            this.notifyListeners('reset', this.state, null);
        },
    };

    // ================================================================
    // 5. SPREADSHEET MANAGER
    // ================================================================

    var SpreadsheetManager = {
        isConnected: false,
        isChecking: false,
        healthCheckInterval: null,
        lastHealthCheck: null,
        checkTimer: null,
        retryCount: 0,
        maxRetries: 3,

        // --- Init ---
        init: function() {
            console.log('[Spreadsheet] Initializing...');
            this.healthCheck();
            this.setupHealthCheck();
        },

        // --- Health Check ---
        healthCheck: function() {
            if (this.isChecking) return;
            this.isChecking = true;

            var apiUrl = CONFIG.SPREADSHEET_API || '';

            if (!apiUrl || apiUrl.includes('YOUR_DEPLOYMENT_ID')) {
                console.warn('[Spreadsheet] API URL not configured');
                this.isConnected = false;
                State.spreadsheetStatus = 'error';
                State.isSpreadsheetConnected = false;
                this.isChecking = false;
                this.updateStatus();
                return;
            }

            var url = apiUrl + '?action=health';
            fetch(url, {
                method: 'GET',
                headers: { 'Accept': 'application/json' },
                timeout: 5000,
            })
            .then(function(response) {
                if (response.ok) {
                    return response.json();
                }
                throw new Error('HTTP ' + response.status);
            })
            .then(function(data) {
                if (data && data.success !== false) {
                    this.isConnected = true;
                    this.retryCount = 0;
                    State.spreadsheetStatus = 'online';
                    State.isSpreadsheetConnected = true;
                    console.log('[Spreadsheet] Health check OK');
                } else {
                    throw new Error('Invalid response');
                }
            }.bind(this))
            .catch(function(error) {
                console.warn('[Spreadsheet] Health check failed:', error);
                this.isConnected = false;
                State.spreadsheetStatus = 'offline';
                State.isSpreadsheetConnected = false;
                this.retryCount++;

                if (this.retryCount < this.maxRetries) {
                    setTimeout(function() {
                        this.healthCheck();
                    }.bind(this), 5000 * this.retryCount);
                }
            }.bind(this))
            .finally(function() {
                this.isChecking = false;
                this.lastHealthCheck = new Date();
                this.updateStatus();
            }.bind(this));
        },

        // --- Setup Health Check Interval ---
        setupHealthCheck: function() {
            if (this.healthCheckInterval) {
                clearInterval(this.healthCheckInterval);
                this.healthCheckInterval = null;
            }

            this.healthCheckInterval = setInterval(function() {
                this.healthCheck();
            }.bind(this), CONFIG.SPREADSHEET_HEALTH_INTERVAL);
        },

        // --- Log Data to Spreadsheet ---
        logData: function(data) {
            var apiUrl = CONFIG.SPREADSHEET_API;

            if (!apiUrl || apiUrl.includes('YOUR_DEPLOYMENT_ID')) {
                console.warn('[Spreadsheet] API URL not configured, data not logged');
                return;
            }

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
                alarm: State.alarms.filter(function(a) { return a.active; }).map(function(a) { return a.message; }).join(
                ','),
                status: State.health > 80 ? 'Normal' : State.health > 60 ? 'Warning' : 'Danger',
            };

            fetch(apiUrl, {
                method: 'POST',
                mode: 'no-cors',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload),
            })
            .catch(function(error) {
                console.warn('[Spreadsheet] Log error:', error);
            });
        },

        // --- Get History Data ---
        getHistory: function(params) {
            params = params || {};
            var apiUrl = CONFIG.SPREADSHEET_API;

            if (!apiUrl || apiUrl.includes('YOUR_DEPLOYMENT_ID')) {
                return Promise.reject(new Error('API not configured'));
            }

            var url = apiUrl + '?action=getHistory';
            if (params.limit) url += '&limit=' + params.limit;
            if (params.from) url += '&from=' + params.from;
            if (params.to) url += '&to=' + params.to;

            return fetch(url, {
                method: 'GET',
                headers: { 'Accept': 'application/json' },
            })
            .then(function(response) {
                if (!response.ok) throw new Error('HTTP ' + response.status);
                return response.json();
            })
            .then(function(data) {
                if (data && data.success !== false) {
                    return data.data || [];
                }
                throw new Error('Invalid response');
            });
        },

        // --- Get Stats ---
        getStats: function() {
            var apiUrl = CONFIG.SPREADSHEET_API;

            if (!apiUrl || apiUrl.includes('YOUR_DEPLOYMENT_ID')) {
                return Promise.reject(new Error('API not configured'));
            }

            return fetch(apiUrl + '?action=stats', {
                method: 'GET',
                headers: { 'Accept': 'application/json' },
            })
            .then(function(response) {
                if (!response.ok) throw new Error('HTTP ' + response.status);
                return response.json();
            });
        },

        // --- Update Status ---
        updateStatus: function() {
            State.spreadsheetStatus = this.isConnected ? 'online' : 'offline';
            State.isSpreadsheetConnected = this.isConnected;
        },

        // --- Get Status ---
        getStatus: function() {
            return {
                connected: this.isConnected,
                lastHealthCheck: this.lastHealthCheck,
                retryCount: this.retryCount,
            };
        },
    };

    // ================================================================
    // 6. TELEGRAM MANAGER
    // ================================================================

    var TelegramManager = {
        botToken: '',
        chatId: '',
        isEnabled: false,
        queue: [],
        isProcessing: false,
        lastSent: null,
        rateLimit: 5000, // 5 seconds between messages
        maxRetries: 3,

        // --- Init ---
        init: function(token, chatId) {
            this.botToken = token || '';
            this.chatId = chatId || '';
            this.isEnabled = !!(this.botToken && this.chatId);

            if (this.isEnabled) {
                console.log('[Telegram] Initialized');
            } else {
                console.warn('[Telegram] Not configured');
            }
        },

        // --- Send Message ---
        sendMessage: function(message, options) {
            options = options || { priority: 'normal', retry: 0 };

            if (!this.isEnabled) {
                console.warn('[Telegram] Not enabled');
                return;
            }

            // Rate limiting
            if (this.lastSent && (Date.now() - this.lastSent) < this.rateLimit) {
                var delay = this.rateLimit - (Date.now() - this.lastSent);
                setTimeout(function() {
                    this.sendMessage(message, options);
                }.bind(this), delay + 100);
                return;
            }

            var payload = {
                chat_id: this.chatId,
                text: message,
                parse_mode: 'HTML',
            };

            var url = 'https://api.telegram.org/bot' + this.botToken + '/sendMessage';

            fetch(url, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload),
            })
            .then(function(response) {
                if (!response.ok) {
                    throw new Error('HTTP ' + response.status);
                }
                return response.json();
            })
            .then(function(data) {
                if (data && data.ok) {
                    this.lastSent = Date.now();
                    console.log('[Telegram] Message sent:', message.substring(0, 50) + '...');
                    State.telegramLastSent = new Date();
                } else {
                    throw new Error(data.description || 'Unknown error');
                }
            }.bind(this))
            .catch(function(error) {
                console.error('[Telegram] Send error:', error);

                if (options.retry < this.maxRetries) {
                    options.retry++;
                    setTimeout(function() {
                        this.sendMessage(message, options);
                    }.bind(this), 2000 * options.retry);
                } else {
                    // Add to queue for later retry
                    this.queue.push({
                        message: message,
                        options: options,
                        timestamp: Date.now()
                    });
                    this.processQueue();
                }
            }.bind(this));
        },

        // --- Process Queue ---
        processQueue: function() {
            if (this.isProcessing || this.queue.length === 0) return;

            this.isProcessing = true;
            var msg = this.queue.shift();

            // Check if too old (more than 1 hour)
            if ((Date.now() - msg.timestamp) > 3600000) {
                console.log('[Telegram] Queue message expired, dropping');
                this.isProcessing = false;
                this.processQueue();
                return;
            }

            this.sendMessage(msg.message, msg.options);
            this.isProcessing = false;

            // Process next after delay
            setTimeout(function() {
                this.processQueue();
            }.bind(this), 1000);
        },

        // --- Send Alarm ---
        sendAlarm: function(message, category) {
            category = category || 'general';
            var icon = '🔔';
            var prefix = '';

            switch (category) {
                case 'critical':
                    icon = '🚨';
                    prefix = '<b>KRITIS</b> ⚠️\n';
                    break;
                case 'warning':
                    icon = '⚠️';
                    prefix = '<b>WARNING</b>\n';
                    break;
                case 'info':
                    icon = 'ℹ️';
                    prefix = '<b>INFORMASI</b>\n';
                    break;
                default:
                    prefix = '';
                    break;
            }

            var fullMessage = icon + ' ' + prefix + message;
            this.sendMessage(fullMessage, { priority: category });
        },

        // --- Send Notification ---
        sendNotification: function(message) {
            this.sendMessage('📢 ' + message, { priority: 'info' });
        },

        // --- Send Error ---
        sendError: function(message) {
            this.sendMessage('❌ ' + message, { priority: 'warning' });
        },

        // --- Get Status ---
        getStatus: function() {
            return {
                enabled: this.isEnabled,
                queueSize: this.queue.length,
                lastSent: this.lastSent,
            };
        },
    };

    // ================================================================
    // 7. BUZZER MANAGER
    // ================================================================

    var BuzzerManager = {
        isEnabled: true,
        isMuted: false,
        currentPattern: null,
        patternTimer: null,
        patterns: {
            normal: { times: 1, duration: 100, interval: 200 },
            warning: { times: 2, duration: 150, interval: 200 },
            critical: { times: 3, duration: 200, interval: 300 },
            emergency: { times: 5, duration: 300, interval: 200 },
            test: { times: 2, duration: 100, interval: 100 },
        },

        // --- Play Pattern ---
        play: function(pattern, options) {
            if (this.isMuted || !this.isEnabled) return;

            options = options || {};
            var patternDef = this.patterns[pattern] || this.patterns.normal;

            var times = options.times || patternDef.times;
            var duration = options.duration || patternDef.duration;
            var interval = options.interval || patternDef.interval;

            this.stop();

            var count = 0;

            function beep() {
                if (count >= times || this.isMuted) {
                    this.stop();
                    return;
                }

                // Trigger buzzer via MQTT
                MqttManager.publish(CONFIG.MQTT_TOPICS.SYSTEM_CONTROL, JSON.stringify({
                    command: 'buzzer',
                    action: 'on',
                    duration: duration,
                }), { qos: 1, retain: false });

                count++;

                if (count < times) {
                    this.patternTimer = setTimeout(function() {
                        // Turn off
                        MqttManager.publish(CONFIG.MQTT_TOPICS.SYSTEM_CONTROL, JSON.stringify({
                            command: 'buzzer',
                            action: 'off',
                        }), { qos: 1, retain: false });

                        setTimeout(function() {
                            beep.call(this);
                        }.bind(this), interval);
                    }.bind(this), duration);
                } else {
                    // Turn off after last beep
                    setTimeout(function() {
                        MqttManager.publish(CONFIG.MQTT_TOPICS.SYSTEM_CONTROL, JSON.stringify({
                            command: 'buzzer',
                            action: 'off',
                        }), { qos: 1, retain: false });
                    }.bind(this), duration);

                    this.currentPattern = null;
                    this.patternTimer = null;
                }
            }

            this.currentPattern = pattern;
            beep.call(this);
        },

        // --- Stop ---
        stop: function() {
            if (this.patternTimer) {
                clearTimeout(this.patternTimer);
                this.patternTimer = null;
            }

            MqttManager.publish(CONFIG.MQTT_TOPICS.SYSTEM_CONTROL, JSON.stringify({
                command: 'buzzer',
                action: 'off',
            }), { qos: 1, retain: false });

            this.currentPattern = null;
        },

        // --- Mute ---
        mute: function() {
            this.isMuted = true;
            this.stop();
            StateManager.set('buzzerMuted', true, { publish: true, sync: true });
        },

        // --- Unmute ---
        unmute: function() {
            this.isMuted = false;
            StateManager.set('buzzerMuted', false, { publish: true, sync: true });
        },

        // --- Toggle ---
        toggle: function() {
            if (this.isMuted) {
                this.unmute();
            } else {
                this.mute();
            }
            return this.isMuted;
        },

        // --- Test ---
        test: function() {
            this.play('test');
        },

        // --- Get Status ---
        getStatus: function() {
            return {
                enabled: this.isEnabled,
                muted: this.isMuted,
                currentPattern: this.currentPattern,
            };
        },
    };

    // ================================================================
    // 8. CORE INIT
    // ================================================================

    var Core = {
        initialized: false,

        // --- Init ---
        init: function(options) {
            if (this.initialized) {
                console.warn('[Core] Already initialized');
                return;
            }

            options = options || {};

            // Merge config
            if (options.config) {
                Object.assign(CONFIG, options.config);
            }

            // Init MQTT
            MqttManager.init();

            // Init Spreadsheet
            SpreadsheetManager.init();

            // Init Telegram
            if (options.telegram) {
                TelegramManager.init(options.telegram.token, options.telegram.chatId);
            }

            // Init State
            StateManager.reset();

            // Setup alarm monitoring
            this.setupAlarmMonitoring();

            // Setup auto logging
            this.setupAutoLogging();

            this.initialized = true;

            console.log('[Core] Initialized successfully');
            console.log('[Core] Version: 3.0.0');

            return this;
        },

        // --- Setup Alarm Monitoring ---
        setupAlarmMonitoring: function() {
            var lastAlarms = {};

            setInterval(function() {
                var activeAlarms = State.alarms.filter(function(a) { return a.active; });

                activeAlarms.forEach(function(alarm) {
                    var key = alarm.message;
                    if (!lastAlarms[key] || (Date.now() - lastAlarms[key]) > 60000) {
                        lastAlarms[key] = Date.now();

                        // Send Telegram
                        TelegramManager.sendAlarm(alarm.message, alarm.category || 'warning');

                        // Play Buzzer
                        var pattern = alarm.category === 'critical' ? 'critical' :
                            alarm.category === 'emergency' ? 'emergency' : 'warning';
                        BuzzerManager.play(pattern);

                        // Log to console
                        console.warn('[Alarm]', alarm.message);
                    }
                });

                // Clear old alarms
                for (var key in lastAlarms) {
                    if (lastAlarms.hasOwnProperty(key)) {
                        var stillActive = activeAlarms.some(function(a) { return a.message === key; });
                        if (!stillActive) {
                            delete lastAlarms[key];
                        }
                    }
                }
            }, 10000);
        },

        // --- Setup Auto Logging ---
        setupAutoLogging: function() {
            setInterval(function() {
                if (State.dataReceived) {
                    SpreadsheetManager.logData({
                        temperature: State.temperature,
                        humidity: State.humidity,
                        nh3: State.nh3,
                        peltierPwm: State.peltierPwm,
                        fanPwm: State.fanPwm,
                        coolFanPwm: State.coolFanPwm,
                        kp: State.kp,
                        ki: State.ki,
                        kd: State.kd,
                        mode: State.mode,
                    });
                }
            }, (State.settings.loggingInterval || 30) * 1000);
        },

        // --- Get Status ---
        getStatus: function() {
            return {
                initialized: this.initialized,
                mqtt: MqttManager.getStatus(),
                spreadsheet: SpreadsheetManager.getStatus(),
                telegram: TelegramManager.getStatus(),
                buzzer: BuzzerManager.getStatus(),
                state: {
                    dataReceived: State.dataReceived,
                    lastUpdate: State.lastStateUpdate,
                    alarmCount: State.alarmCount,
                    errorCount: State.errorCount,
                },
            };
        },

        // --- Expose Managers ---
        getMqttManager: function() { return MqttManager; },
        getStateManager: function() { return StateManager; },
        getSpreadsheetManager: function() { return SpreadsheetManager; },
        getTelegramManager: function() { return TelegramManager; },
        getBuzzerManager: function() { return BuzzerManager; },
        getState: function() { return StateManager.getState(); },
        getConfig: function() { return CONFIG; },
    };

    // ================================================================
    // 9. EXPOSE TO GLOBAL
    // ================================================================

    global.Core = Core;
    global.MqttManager = MqttManager;
    global.StateManager = StateManager;
    global.SpreadsheetManager = SpreadsheetManager;
    global.TelegramManager = TelegramManager;
    global.BuzzerManager = BuzzerManager;
    global.CONFIG = CONFIG;

    console.log('[Core] Module loaded successfully');

    // Auto-init if DOM ready
    if (document.readyState === 'complete' || document.readyState === 'interactive') {
        // Wait for other scripts to load
        setTimeout(function() {
            if (!Core.initialized) {
                Core.init();
            }
        }, 500);
    } else {
        document.addEventListener('DOMContentLoaded', function() {
            setTimeout(function() {
                if (!Core.initialized) {
                    Core.init();
                }
            }, 500);
        });
    }

})(window);