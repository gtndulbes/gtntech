// ============================================================
// MQTT Configuration - Legacy / Compatibility Layer
// ============================================================
// File ini hanya sebagai proxy untuk mengakses konfigurasi dan
// manager dari core.js. Semua logika ada di core.js.
// ============================================================

(function(global) {
    'use strict';

    // Jika core.js sudah dimuat, kita gunakan referensi dari sana
    if (global.Core && global.Core.getConfig && global.Core.getMqttManager) {
        var config = global.Core.getConfig();
        var mqttMgr = global.Core.getMqttManager();

        // Ekspos dengan nama yang sama seperti sebelumnya
        global.MQTT_CONFIG = {
            BROKER: config.MQTT_BROKER,
            OPTIONS: config.MQTT_OPTIONS,
            TOPICS: config.MQTT_TOPICS,
            DEBUG: config.DEBUG || true,
        };

        // Ekspos mqttManager sebagai wrapper (agar bisa pakai on/publish/subscribe)
        global.mqttManager = {
            connect: function() { return mqttMgr.connect(); },
            disconnect: function() { return mqttMgr.disconnect(); },
            publish: function(topic, data, options) {
                var payload = typeof data === 'string' ? data : JSON.stringify(data);
                return mqttMgr.publish(topic, payload, options);
            },
            subscribe: function(topic, callback) { return mqttMgr.subscribe(topic, callback); },
            unsubscribe: function(topic, callback) { return mqttMgr.unsubscribe(topic, callback); },
            on: function(topic, callback) { return mqttMgr.subscribe(topic, callback); },
            onStatusChange: function(callback) { return mqttMgr.subscribeSystem(callback); },
            get connected() { return mqttMgr.isConnected; },
            get status() { return mqttMgr.getStatus(); },
        };

        console.log('[mqtt-config] Using core.js as single source of truth');
        return;
    }

    // Jika core.js belum dimuat, kita buat sendiri (fallback minimal)
    console.warn('[mqtt-config] core.js not loaded, using fallback config');

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

    // Fallback mqttManager minimal (hanya agar tidak error)
    global.mqttManager = {
        connected: false,
        connect: function() { console.warn('[mqttManager] Not connected (fallback)'); },
        disconnect: function() {},
        publish: function() { return false; },
        subscribe: function() {},
        unsubscribe: function() {},
        on: function() {},
        onStatusChange: function() {},
        get status() { return { connected: false }; },
    };

})(window);