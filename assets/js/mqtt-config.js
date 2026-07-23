// ============================================================
// MQTT Configuration & Proxy Layer
// ============================================================
// File ini menghubungkan legacy window.mqttManager dan window.MQTT_CONFIG
// dengan Core System (core.js).
// ============================================================

(function(global) {
    'use strict';

    var MAX_RETRY = 10;
    var retryCount = 0;
    var retryInterval = null;

    function setupFromCore() {
        var core = global.Core || (global.window && global.window.Core);
        var stateMgr = global.StateManager || (core && core.getStateManager ? core.getStateManager() : null);
        var mqttMgr = global.MqttManager || (core && core.getMqttManager ? core.getMqttManager() : null);

        if (!mqttMgr) {
            if (retryCount < MAX_RETRY) {
                retryCount++;
                if (retryInterval) clearTimeout(retryInterval);
                retryInterval = setTimeout(setupFromCore, 300);
                return;
            }
            createFallback();
            return;
        }

        var config = global.CONFIG || (core && core.getConfig ? core.getConfig() : null);

        if (config) {
            global.MQTT_CONFIG = {
                BROKER: config.MQTT_BROKER,
                OPTIONS: config.MQTT_OPTIONS,
                TOPICS: config.MQTT_TOPICS,
                DEBUG: config.DEBUG || true,
            };
        }

        var wrapper = {
            connect: function() {
                if (typeof mqttMgr.connect === 'function') return mqttMgr.connect();
            },
            disconnect: function() {
                if (typeof mqttMgr.disconnect === 'function') return mqttMgr.disconnect();
            },
            publish: function(topic, data, options) {
                var payload = typeof data === 'string' ? data : JSON.stringify(data);
                if (typeof mqttMgr.publish === 'function') {
                    return mqttMgr.publish(topic, payload, options);
                }
                return false;
            },
            subscribe: function(topic, callback) {
                if (typeof mqttMgr.subscribe === 'function') {
                    return mqttMgr.subscribe(topic, callback);
                }
            },
            unsubscribe: function(topic, callback) {
                if (typeof mqttMgr.unsubscribe === 'function') {
                    return mqttMgr.unsubscribe(topic, callback);
                }
            },
            on: function(topicOrEvent, callback) {
                var eventNames = ['connect', 'reconnect', 'close', 'offline', 'error', 'message', 'status'];
                if (eventNames.indexOf(topicOrEvent) !== -1) {
                    if (typeof mqttMgr.subscribeSystem === 'function') {
                        return mqttMgr.subscribeSystem(function(evt, data) {
                            if (evt === topicOrEvent && callback) callback(data);
                        });
                    }
                    if (mqttMgr.client && typeof mqttMgr.client.on === 'function') {
                        return mqttMgr.client.on(topicOrEvent, callback);
                    }
                } else {
                    if (typeof mqttMgr.subscribe === 'function') {
                        return mqttMgr.subscribe(topicOrEvent, callback);
                    }
                }
            },
            onStatusChange: function(callback) {
                if (typeof mqttMgr.subscribeSystem === 'function') {
                    return mqttMgr.subscribeSystem(callback);
                }
            },
            get connected() {
                return mqttMgr.isConnected || false;
            },
            get status() {
                if (typeof mqttMgr.getStatus === 'function') {
                    return mqttMgr.getStatus();
                }
                return { connected: mqttMgr.isConnected || false };
            },
            get client() {
                return mqttMgr.client || null;
            }
        };

        global.mqttManager = wrapper;
        if (global.window) global.window.mqttManager = wrapper;

        console.log('[mqtt-config] Proxy mqttManager terhubung ke Core System');

        if (retryInterval) {
            clearTimeout(retryInterval);
            retryInterval = null;
        }
    }

    function createFallback() {
        console.warn('[mqtt-config] Core tidak siap, menggunakan fallback');

        global.MQTT_CONFIG = {
            BROKER: 'wss://broker.emqx.io:8084/mqtt',
            OPTIONS: {
                clientId: 'dashboard_' + Math.random().toString(16).substr(2, 8),
                clean: true,
                reconnectPeriod: 3000,
                connectTimeout: 5000
            },
            TOPICS: {
                SENSOR_SHT31: 'sensor/sht31',
                SENSOR_MQ135: 'sensor/mq135',
                SENSOR_STATUS: 'sensor/status',
                ACTUATOR_STATUS: 'actuator/status',
                ACTUATOR_CONTROL: 'actuator/control',
                SYSTEM_CONTROL: 'system/control',
                SYSTEM_ALARM: 'system/alarm',
                SELF_MONITOR: 'system/selfmonitor',
                SETTING_UPDATE: 'setting/update',
                SYSTEM_STATE: 'system/state'
            },
            DEBUG: true,
        };

        var fallbackManager = {
            connected: false,
            connect: function() {},
            disconnect: function() {},
            publish: function() { return false; },
            subscribe: function() {},
            unsubscribe: function() {},
            on: function() {},
            onStatusChange: function() {},
            get status() { return { connected: false }; },
            get client() { return null; }
        };

        global.mqttManager = fallbackManager;
        if (global.window) global.window.mqttManager = fallbackManager;
    }

    if (document.readyState === 'complete' || document.readyState === 'interactive') {
        setTimeout(setupFromCore, 100);
    } else {
        document.addEventListener('DOMContentLoaded', function() {
            setTimeout(setupFromCore, 100);
        });
    }

})(typeof window !== 'undefined' ? window : this);
