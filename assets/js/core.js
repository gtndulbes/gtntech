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
        SPREADSHEET_API: 'https://script.google.com/macros/s/AKfycbzju3OUArlxEQReHnDWdPNA4A7Sc3PZ4FO7t5DFMH3HEMnkZ_RH2bobso40_yKh4eYqPA/exec',
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
    // 2. STATE - Single Source of Truth
    // ================================================================

    var State = {
        temperature: null,
        humidity: null,
        nh3: null,
        temperatureValid: false,
        humidityValid: false,
        nh3Valid: false,

        peltierOn: false,
        peltierPwm: 0,
        peltierMode: 'standby',
        fanOn: false,
        fanPwm: 0,
        coolFanOn: false,
        coolFanPwm: 0,
        mode: 'AUTO',

        setpointTemp: 27.5,
        setpointHum: 75,
        kp: 2.0,
        ki: 0.5,
        kd: 0.1,
        error: 0,
        deltaError: 0,
        outputPid: 0,

        health: 100,
        uptime: 0,
        esp32Status: 'online',
        wifiStatus: 'online',
        mqttStatus: 'offline',
        spreadsheetStatus: 'online',
        lastHeartbeat: null,
        lastStateUpdate: null,

        alarms: [],
        errors: [],
        alarmCount: 0,
        errorCount: 0,
        notifications: [],
        logs: [],

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

        telegramEnabled: true,
        telegramLastSent: null,
        telegramQueue: [],

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

        isInitialized: false,
        isMqttConnected: false,
        isSpreadsheetConnected: false,
        dataReceived: false,
        reconnectAttempts: 0,
        lastReconnectTime: null,
        offlineQueue: [],
        subscribers: [],
        buzzerMuted: false,
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
        debug: CONFIG.DEBUG,

        init: function() {
            if (this.client) {
                this.log('Already initialized');
                return;
            }
            this.log('Initializing...');
            this.connect();
            this.setupHeartbeat();
            this.setupStateSync();
        },

        connect: function() {
            if (this.isConnecting) {
                this.log('Already connecting...');
                return;
            }
            if (this.client && this.isConnected) {
                this.log('Already connected');
                return;
            }

            this.isConnecting = true;
            this.log('Connecting to:', CONFIG.MQTT_BROKER);

            try {
                var options = Object.assign({}, CONFIG.MQTT_OPTIONS);
                options.clientId = 'core_' + Date.now().toString(36) + '_' + Math.random().toString(36).slice(2, 6);

                if (typeof mqtt === 'undefined') {
                    this.log('MQTT library not found globally');
                    this.isConnecting = false;
                    this.scheduleReconnect();
                    return;
                }

                this.client = mqtt.connect(CONFIG.MQTT_BROKER, options);

                this.client.on('connect', this.onConnect.bind(this));
                this.client.on('reconnect', this.onReconnect.bind(this));
                this.client.on('close', this.onClose.bind(this));
                this.client.on('offline', this.onOffline.bind(this));
                this.client.on('error', this.onError.bind(this));
                this.client.on('message', this.onMessage.bind(this));

            } catch (error) {
                this.log('Connection error:', error);
                this.isConnecting = false;
                this.scheduleReconnect();
            }
        },

        disconnect: function() {
            if (this.client) {
                try {
                    this.client.end(true);
                } catch (e) {
                    this.log('Disconnect error:', e);
                }
                this.client = null;
            }
            this.isConnected = false;
            this.isConnecting = false;
            this.updateStatus();
            this.log('Disconnected');
        },

        scheduleReconnect: function() {
            if (this.reconnectTimer) {
                clearTimeout(this.reconnectTimer);
                this.reconnectTimer = null;
            }

            if (this.reconnectAttempts >= CONFIG.MAX_RECONNECT_ATTEMPTS) {
                this.log('Max reconnect attempts reached, giving up');
                State.mqttStatus = 'offline';
                this.updateStatus();
                return;
            }

            var delay = CONFIG.RECONNECT_BASE_DELAY * Math.pow(1.5, this.reconnectAttempts);
            delay = Math.min(delay, 60000);

            this.log('Scheduling reconnect in', delay / 1000, 'seconds (attempt', this.reconnectAttempts + 1, ')');

            this.reconnectTimer = setTimeout(function() {
                this.reconnectAttempts++;
                this.connect();
            }.bind(this), delay);
        },

        onConnect: function() {
            this.isConnected = true;
            this.isConnecting = false;
            this.reconnectAttempts = 0;
            State.mqttStatus = 'online';
            State.isMqttConnected = true;

            this.log('Connected successfully');

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

            var requiredTopics = [
                CONFIG.MQTT_TOPICS.SENSOR_SHT31,
                CONFIG.MQTT_TOPICS.SENSOR_MQ135,
                CONFIG.MQTT_TOPICS.ACTUATOR_STATUS,
                CONFIG.MQTT_TOPICS.FUZZY_STATUS,
                CONFIG.MQTT_TOPICS.SYSTEM_ALARM,
                CONFIG.MQTT_TOPICS.SELF_MONITOR,
                CONFIG.MQTT_TOPICS.SYSTEM_HEARTBEAT,
                CONFIG.MQTT_TOPICS.SYSTEM_STATUS,
                CONFIG.MQTT_TOPICS.SYSTEM_STATE,
                CONFIG.MQTT_TOPICS.SETTING_UPDATE,
            ];

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

        onReconnect: function() {
            this.reconnectAttempts++;
            this.log('Reconnecting... Attempt:', this.reconnectAttempts);
        },

        onClose: function() {
            this.isConnected = false;
            this.isConnecting = false;
            State.mqttStatus = 'offline';
            State.isMqttConnected = false;
            this.updateStatus();
            this.log('Connection closed');
            this.notifySubscribers('close', { connected: false });
        },

        onOffline: function() {
            this.isConnected = false;
            State.mqttStatus = 'offline';
            State.isMqttConnected = false;
            this.updateStatus();
            this.log('Client offline');
            this.notifySubscribers('offline', { connected: false });
        },

        onError: function(error) {
            this.log('MQTT error:', error);
            this.notifySubscribers('error', { error: error });
        },

        onMessage: function(topic, message) {
            var payloadStr = message.toString();
            var data;

            try {
                data = JSON.parse(payloadStr);
            } catch (e) {
                data = payloadStr;
            }

            this.log('Message received:', topic, data);

            if (this.messageHandlers[topic]) {
                this.messageHandlers[topic].forEach(function(handler) {
                    try {
                        handler(data, topic);
                    } catch (e) {
                        console.error('[MQTT] Handler error for topic', topic, e);
                    }
                });
            }

            if (typeof StateManager !== 'undefined' && typeof StateManager.handleMqttMessage === 'function') {
                StateManager.handleMqttMessage(topic, data);
            }

            this.notifySubscribers('message', { topic: topic, data: data });
        },

        subscribe: function(topic, handler) {
            if (!this.messageHandlers[topic]) {
                this.messageHandlers[topic] = [];
            }

            if (handler && typeof handler === 'function') {
                this.messageHandlers[topic].push(handler);
            }

            if (this.client && this.isConnected) {
                this.client.subscribe(topic, { qos: 1 }, function(err) {
                    if (err) {
                        this.log('Subscribe error for', topic, err);
                    } else {
                        this.log('Subscribed to', topic);
                    }
                }.bind(this));
            }
        },

        unsubscribe: function(topic, handler) {
            if (this.messageHandlers[topic]) {
                if (handler) {
                    this.messageHandlers[topic] = this.messageHandlers[topic].filter(function(h) { return h !== handler; });
                } else {
                    delete this.messageHandlers[topic];
                }
            }

            if (!this.messageHandlers[topic] || this.messageHandlers[topic].length === 0) {
                if (this.client && this.isConnected) {
                    this.client.unsubscribe(topic);
                }
            }
        },

        publish: function(topic, payload, options) {
            options = options || { qos: 1, retain: false };
            var messageStr = typeof payload === 'string' ? payload : JSON.stringify(payload);

            if (this.client && this.isConnected) {
                this.client.publish(topic, messageStr, options, function(err) {
                    if (err) {
                        this.log('Publish error:', err);
                        this.queueOffline(topic, messageStr, options);
                    }
                }.bind(this));
                return true;
            } else {
                this.queueOffline(topic, messageStr, options);
                return false;
            }
        },

        queueOffline: function(topic, payload, options) {
            if (this.offlineQueue.length >= CONFIG.MAX_OFFLINE_QUEUE) {
                this.offlineQueue.shift();
            }
            this.offlineQueue.push({ topic: topic, payload: payload, options: options });
            this.log('Queued offline message for topic:', topic);
        },

        processQueue: function() {
            if (this.isProcessingQueue || this.offlineQueue.length === 0) return;
            this.isProcessingQueue = true;

            this.log('Processing offline queue count:', this.offlineQueue.length);

            while (this.offlineQueue.length > 0 && this.isConnected) {
                var item = this.offlineQueue.shift();
                this.publish(item.topic, item.payload, item.options);
            }

            this.isProcessingQueue = false;
        },

        publishState: function() {
            var stateCopy = Object.assign({}, State);
            delete stateCopy.subscribers;
            delete stateCopy.offlineQueue;
            delete stateCopy.logs;

            this.publish(CONFIG.MQTT_TOPICS.SYSTEM_STATE, JSON.stringify(stateCopy), { qos: 1, retain: true });
        },

        setupHeartbeat: function() {
            if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);

            this.heartbeatTimer = setInterval(function() {
                if (this.isConnected) {
                    this.publish(CONFIG.MQTT_TOPICS.SYSTEM_HEARTBEAT, JSON.stringify({
                        timestamp: new Date().toISOString(),
                        uptime: Math.floor(performance.now() / 1000),
                        health: State.health,
                    }), { qos: 0, retain: false });
                }
            }.bind(this), CONFIG.HEARTBEAT_INTERVAL);
        },

        setupStateSync: function() {
            if (this.stateSyncTimer) clearInterval(this.stateSyncTimer);

            this.stateSyncTimer = setInterval(function() {
                if (this.isConnected) {
                    this.publishState();
                }
            }.bind(this), CONFIG.STATE_SYNC_INTERVAL);
        },

        updateStatus: function() {
            State.isMqttConnected = this.isConnected;
            if (State.components && State.components.mqtt) {
                State.components.mqtt.status = this.isConnected ? 'online' : 'offline';
                State.components.mqtt.lastSeen = new Date();
            }
        },

        notifySubscribers: function(event, data) {
            this.subscribers.forEach(function(sub) {
                try {
                    sub(event, data);
                } catch (e) {
                    console.error('[MQTT] Subscriber error:', e);
                }
            });
        },

        subscribeSystem: function(callback) {
            if (typeof callback === 'function') {
                this.subscribers.push(callback);
            }
        },

        getStatus: function() {
            return {
                connected: this.isConnected,
                connecting: this.isConnecting,
                reconnectAttempts: this.reconnectAttempts,
                queueLength: this.offlineQueue.length,
            };
        },

        log: function() {
            if (this.debug) {
                var args = Array.prototype.slice.call(arguments);
                args.unshift('[MQTT]');
                console.log.apply(console, args);
            }
        }
    };

    // ================================================================
    // 4. STATE MANAGER
    // ================================================================

    var StateManager = {
        state: State,
        listeners: {},

        get: function(key) {
            if (!key) return this.state;
            return this.state[key];
        },

        getState: function() {
            return Object.assign({}, this.state);
        },

        set: function(key, value, options) {
            options = options || { publish: false, sync: false, silent: false };
            var oldValue = this.state[key];

            if (oldValue !== value) {
                this.state[key] = value;
                this.state.lastStateUpdate = new Date().toISOString();

                if (!options.silent) {
                    this.notifySubscribers(key, value, oldValue);
                }

                if (options.publish && typeof MqttManager !== 'undefined' && MqttManager.isConnected) {
                    var payload = {};
                    payload[key] = value;
                    MqttManager.publish(CONFIG.MQTT_TOPICS.SYSTEM_STATE, JSON.stringify(payload));
                }
            }
        },

        update: function(updates, options) {
            options = options || { publish: false, sync: false, silent: false };
            var changed = false;

            for (var key in updates) {
                if (updates.hasOwnProperty(key)) {
                    var oldValue = this.state[key];
                    var newValue = updates[key];

                    if (JSON.stringify(oldValue) !== JSON.stringify(newValue)) {
                        this.state[key] = newValue;
                        changed = true;

                        if (!options.silent) {
                            this.notifySubscribers(key, newValue, oldValue);
                        }
                    }
                }
            }

            if (changed) {
                this.state.lastStateUpdate = new Date().toISOString();

                if (options.publish && typeof MqttManager !== 'undefined' && MqttManager.isConnected) {
                    MqttManager.publish(CONFIG.MQTT_TOPICS.SYSTEM_STATE, JSON.stringify(updates));
                }
            }
        },

        handleMqttMessage: function(topic, data) {
            if (!data || typeof data !== 'object') return;
            var updates = {};

            switch (topic) {
                case CONFIG.MQTT_TOPICS.SENSOR_SHT31:
                    if (data.temperature !== undefined) updates.temperature = data.temperature;
                    if (data.humidity !== undefined) updates.humidity = data.humidity;
                    updates.dataReceived = true;
                    break;

                case CONFIG.MQTT_TOPICS.SENSOR_MQ135:
                    if (data.nh3 !== undefined) updates.nh3 = data.nh3;
                    updates.dataReceived = true;
                    break;

                case CONFIG.MQTT_TOPICS.ACTUATOR_STATUS:
                    if (data.peltierOn !== undefined) updates.peltierOn = data.peltierOn;
                    if (data.peltierPwm !== undefined) updates.peltierPwm = data.peltierPwm;
                    if (data.fanOn !== undefined) updates.fanOn = data.fanOn;
                    if (data.fanPwm !== undefined) updates.fanPwm = data.fanPwm;
                    if (data.coolFanOn !== undefined) updates.coolFanOn = data.coolFanOn;
                    if (data.coolFanPwm !== undefined) updates.coolFanPwm = data.coolFanPwm;
                    if (data.mode !== undefined) updates.mode = data.mode;
                    break;

                case CONFIG.MQTT_TOPICS.FUZZY_STATUS:
                    if (data.kp !== undefined) updates.kp = data.kp;
                    if (data.ki !== undefined) updates.ki = data.ki;
                    if (data.kd !== undefined) updates.kd = data.kd;
                    if (data.error !== undefined) updates.error = data.error;
                    if (data.deltaError !== undefined) updates.deltaError = data.deltaError;
                    if (data.outputPid !== undefined) updates.outputPid = data.outputPid;
                    break;

                case CONFIG.MQTT_TOPICS.SYSTEM_ALARM:
                    if (Array.isArray(data.alarms)) {
                        updates.alarms = data.alarms;
                    } else if (data.message) {
                        var existingAlarms = this.state.alarms.slice();
                        existingAlarms.push(data);
                        updates.alarms = existingAlarms;
                    }
                    break;

                case CONFIG.MQTT_TOPICS.SELF_MONITOR:
                    if (data.components && typeof data.components === 'object') {
                        var comps = this.state.components;
                        for (var key in data.components) {
                            if (data.components.hasOwnProperty(key)) {
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
                        updates.health = data.health;
                    }
                    break;

                case CONFIG.MQTT_TOPICS.SETTING_UPDATE:
                    if (data.settings && typeof data.settings === 'object') {
                        updates.settings = Object.assign({}, this.state.settings, data.settings);
                    }
                    if (data.setpointTemp !== undefined) updates.setpointTemp = data.setpointTemp;
                    if (data.setpointHum !== undefined) updates.setpointHum = data.setpointHum;
                    break;

                case CONFIG.MQTT_TOPICS.SYSTEM_STATE:
                    for (var k in data) {
                        if (data.hasOwnProperty(k) && k !== 'subscribers' && k !== 'logs') {
                            updates[k] = data[k];
                        }
                    }
                    break;

                default:
                    break;
            }

            if (Object.keys(updates).length > 0) {
                this.update(updates, { publish: false, sync: false, silent: false });
            }
        },

        reset: function() {
            this.state.temperature = null;
            this.state.humidity = null;
            this.state.nh3 = null;
            this.state.peltierOn = false;
            this.state.peltierPwm = 0;
            this.state.fanOn = false;
            this.state.fanPwm = 0;
            this.state.coolFanOn = false;
            this.state.coolFanPwm = 0;
            this.state.mode = 'AUTO';
            this.state.alarms = [];
            this.state.notifications = [];
            this.state.logs = [];
        },

        addListener: function(key, callback) {
            if (!this.listeners[key]) {
                this.listeners[key] = [];
            }
            if (typeof callback === 'function') {
                this.listeners[key].push(callback);
            }
        },

        removeListener: function(key, callback) {
            if (this.listeners[key]) {
                if (callback) {
                    this.listeners[key] = this.listeners[key].filter(function(cb) { return cb !== callback; });
                } else {
                    delete this.listeners[key];
                }
            }
        },

        notifySubscribers: function(key, newValue, oldValue) {
            if (this.listeners['*']) {
                this.listeners['*'].forEach(function(cb) {
                    try { cb(key, newValue, oldValue); } catch (e) { console.error('[State] Listener error:', e); }
                });
            }

            if (this.listeners[key]) {
                this.listeners[key].forEach(function(cb) {
                    try { cb(newValue, oldValue); } catch (e) { console.error('[State] Listener error:', e); }
                });
            }
        },

        addNotification: function(type, message) {
            var item = { id: Date.now(), type: type, message: message, timestamp: new Date().toISOString() };
            var list = this.state.notifications.slice();
            list.unshift(item);
            if (list.length > 50) list.pop();
            this.set('notifications', list);
        },

        addLog: function(level, message, details) {
            var item = { id: Date.now(), level: level, message: message, details: details, timestamp: new Date().toISOString() };
            var list = this.state.logs.slice();
            list.unshift(item);
            if (list.length > 100) list.pop();
            this.set('logs', list);
        }
    };

    // ================================================================
    // 5. SPREADSHEET MANAGER
    // ================================================================

    var SpreadsheetManager = {
        isConfigured: true,

        init: function() {
            if (!CONFIG.SPREADSHEET_API) {
                this.isConfigured = false;
                console.warn('[Spreadsheet] URL API Spreadsheet tidak dikonfigurasi');
            } else {
                this.isConfigured = true;
                console.log('[Spreadsheet] Configured with API:', CONFIG.SPREADSHEET_API);
            }
        },

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
                return response.text();
            })
            .then(function(text) {
                console.log('[Spreadsheet] Log success:', text);
            })
            .catch(function(error) {
                console.warn('[Spreadsheet] Log error:', error.message);
            });
        },

        checkHealth: function() {
            if (!this.isConfigured) return;
            fetch(CONFIG.SPREADSHEET_API + '?ping=1', { method: 'GET', mode: 'cors' })
                .then(function(res) {
                    State.spreadsheetStatus = res.ok ? 'online' : 'offline';
                    State.isSpreadsheetConnected = res.ok;
                })
                .catch(function() {
                    State.spreadsheetStatus = 'offline';
                    State.isSpreadsheetConnected = false;
                });
        },

        getStatus: function() {
            return {
                configured: this.isConfigured,
                status: State.spreadsheetStatus,
                connected: State.isSpreadsheetConnected,
            };
        }
    };

    // ================================================================
    // 6. TELEGRAM MANAGER
    // ================================================================

    var TelegramManager = {
        token: '',
        chatId: '',
        enabled: false,

        init: function(token, chatId) {
            this.token = token || '';
            this.chatId = chatId || '';
            this.enabled = !!(this.token && this.chatId);
            if (this.enabled) {
                console.log('[Telegram] Initialized with chat ID:', this.chatId);
            }
        },

        send: function(message, parseMode) {
            if (!this.enabled) return Promise.resolve(false);
            var url = 'https://api.telegram.org/bot' + this.token + '/sendMessage';
            var body = {
                chat_id: this.chatId,
                text: message,
                parse_mode: parseMode || 'HTML'
            };

            return fetch(url, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(body)
            })
            .then(function(res) { return res.json(); })
            .then(function(data) {
                console.log('[Telegram] Message sent:', data.ok);
                return data.ok;
            })
            .catch(function(err) {
                console.error('[Telegram] Error:', err);
                return false;
            });
        },

        sendAlarm: function(alarmMessage, category) {
            var icon = category === 'critical' ? '🔴' : category === 'warning' ? '⚠️' : 'ℹ️';
            var text = '<b>' + icon + ' GTN MealSense Alarm</b>\n' +
                       'Message: ' + alarmMessage + '\n' +
                       'Time: ' + new Date().toLocaleString();
            return this.send(text);
        },

        getStatus: function() {
            return {
                enabled: this.enabled,
                hasToken: !!this.token,
                hasChatId: !!this.chatId
            };
        }
    };

    // ================================================================
    // 7. BUZZER MANAGER
    // ================================================================

    var BuzzerManager = {
        isEnabled: true,
        isMuted: false,
        currentPattern: null,
        patternTimer: null,
        debug: CONFIG.DEBUG,

        patterns: {
            beep: [100],
            warning: [200, 100, 200],
            critical: [500, 100, 500, 100, 500],
            emergency: [1000, 200, 1000, 200, 1000],
            test: [100, 50, 100, 50, 100],
            success: [100, 50, 200]
        },

        play: function(patternName) {
            if (!this.isEnabled || this.isMuted) return;

            var pattern = this.patterns[patternName] || this.patterns.beep;
            this.stop();

            var index = 0;
            var self = this;

            function step() {
                if (index >= pattern.length) {
                    self.stop();
                    return;
                }

                var duration = pattern[index];
                MqttManager.publish(CONFIG.MQTT_TOPICS.SYSTEM_CONTROL, JSON.stringify({
                    command: 'buzzer',
                    action: 'on'
                }), { qos: 1, retain: false });

                self.patternTimer = setTimeout(function() {
                    MqttManager.publish(CONFIG.MQTT_TOPICS.SYSTEM_CONTROL, JSON.stringify({
                        command: 'buzzer',
                        action: 'off'
                    }), { qos: 1, retain: false });

                    index++;
                    if (index < pattern.length) {
                        self.patternTimer = setTimeout(step, 100);
                    } else {
                        self.currentPattern = null;
                    }
                }, duration);
            }

            this.currentPattern = patternName;
            step();
        },

        stop: function() {
            if (this.patternTimer) {
                clearTimeout(this.patternTimer);
                this.patternTimer = null;
            }

            MqttManager.publish(CONFIG.MQTT_TOPICS.SYSTEM_CONTROL, JSON.stringify({
                command: 'buzzer',
                action: 'off'
            }), { qos: 1, retain: false });

            this.currentPattern = null;
        },

        mute: function() {
            this.isMuted = true;
            this.stop();
            StateManager.set('buzzerMuted', true, { publish: true, sync: true });
        },

        unmute: function() {
            this.isMuted = false;
            StateManager.set('buzzerMuted', false, { publish: true, sync: true });
        },

        toggle: function() {
            if (this.isMuted) {
                this.unmute();
            } else {
                this.mute();
            }
            return this.isMuted;
        },

        test: function() {
            this.play('test');
        },

        getStatus: function() {
            return {
                enabled: this.isEnabled,
                muted: this.isMuted,
                currentPattern: this.currentPattern,
            };
        },

        log: function() {
            if (this.debug) {
                var args = Array.prototype.slice.call(arguments);
                args.unshift('[Buzzer]');
                console.log.apply(console, args);
            }
        }
    };

    // ================================================================
    // 8. CORE INIT
    // ================================================================

    var Core = {
        initialized: false,

        init: function(options) {
            if (this.initialized) {
                console.warn('[Core] Already initialized');
                return this;
            }

            options = options || {};

            if (options.config) {
                Object.assign(CONFIG, options.config);
            }

            MqttManager.init();
            SpreadsheetManager.init();

            if (options.telegram) {
                TelegramManager.init(options.telegram.token, options.telegram.chatId);
            }

            StateManager.reset();

            this.setupAlarmMonitoring();
            this.setupAutoLogging();

            this.initialized = true;

            console.log('[Core] Initialized successfully');
            console.log('[Core] Version: 3.0.1');

            return this;
        },

        setupAlarmMonitoring: function() {
            var lastAlarms = {};

            setInterval(function() {
                var activeAlarms = State.alarms.filter(function(a) { return a.active; });

                activeAlarms.forEach(function(alarm) {
                    var key = alarm.message;
                    if (!lastAlarms[key] || (Date.now() - lastAlarms[key]) > 60000) {
                        lastAlarms[key] = Date.now();

                        TelegramManager.sendAlarm(alarm.message, alarm.category || 'warning');

                        var pattern = alarm.category === 'critical' ? 'critical' :
                            alarm.category === 'emergency' ? 'emergency' : 'warning';
                        BuzzerManager.play(pattern);

                        console.warn('[Alarm]', alarm.message);
                    }
                });

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
    global.mqttManager = MqttManager;
    global.StateManager = StateManager;
    global.SpreadsheetManager = SpreadsheetManager;
    global.TelegramManager = TelegramManager;
    global.BuzzerManager = BuzzerManager;
    global.CONFIG = CONFIG;

    global.MQTT_CONFIG = {
        BROKER: CONFIG.MQTT_BROKER,
        OPTIONS: CONFIG.MQTT_OPTIONS,
        TOPICS: CONFIG.MQTT_TOPICS,
        DEBUG: CONFIG.DEBUG,
    };

    console.log('[Core] Module loaded successfully');

    if (typeof document !== 'undefined') {
        if (document.readyState === 'complete' || document.readyState === 'interactive') {
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
    }

})(typeof window !== 'undefined' ? window : this);