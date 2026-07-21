// ============================================================
// MQTT Configuration - Legacy / Compatibility Layer
// ============================================================
// File ini hanya sebagai proxy untuk mengakses konfigurasi dan
// manager dari core.js. Semua logika ada di core.js.
//
// CATATAN: File ini SEBENARNYA TIDAK DIPERLUKAN karena core.js
// sudah mengekspos window.mqttManager dan window.MQTT_CONFIG.
// Namun jika masih digunakan untuk kompatibilitas, file ini
// akan meneruskan semua panggilan ke core.js.
// ============================================================

(function(global) {
    'use strict';

    var MAX_RETRY = 10;
    var retryCount = 0;
    var retryInterval = null;

    function setupFromCore() {
        // Pastikan Core tersedia dan sudah diinisialisasi
        if (!global.Core || !global.Core.initialized) {
            if (retryCount < MAX_RETRY) {
                retryCount++;
                console.log('[mqtt-config] Menunggu Core siap... (attempt ' + retryCount + '/' + MAX_RETRY + ')');
                if (retryInterval) clearTimeout(retryInterval);
                retryInterval = setTimeout(setupFromCore, 500);
                return;
            }
            console.warn('[mqtt-config] Core tidak siap setelah ' + MAX_RETRY + ' percobaan, gunakan fallback');
            createFallback();
            return;
        }

        var config = global.Core.getConfig ? global.Core.getConfig() : null;
        var mqttMgr = global.Core.getMqttManager ? global.Core.getMqttManager() : null;

        if (!config || !mqttMgr) {
            console.warn('[mqtt-config] Core tidak menyediakan config atau mqttManager');
            createFallback();
            return;
        }

        // ============================================================
        // 1. EKSPOS MQTT_CONFIG
        // ============================================================
        global.MQTT_CONFIG = {
            BROKER: config.MQTT_BROKER,
            OPTIONS: config.MQTT_OPTIONS,
            TOPICS: config.MQTT_TOPICS,
            DEBUG: config.DEBUG || true,
        };

        // ============================================================
        // 2. EKSPOS mqttManager sebagai wrapper
        // ============================================================
        global.mqttManager = {
            connect: function() {
                if (typeof mqttMgr.connect === 'function') return mqttMgr.connect();
                console.warn('[mqttManager] connect() tidak tersedia');
            },
            disconnect: function() {
                if (typeof mqttMgr.disconnect === 'function') return mqttMgr.disconnect();
                console.warn('[mqttManager] disconnect() tidak tersedia');
            },
            publish: function(topic, data, options) {
                var payload = typeof data === 'string' ? data : JSON.stringify(data);
                if (typeof mqttMgr.publish === 'function') {
                    return mqttMgr.publish(topic, payload, options);
                }
                console.warn('[mqttManager] publish() tidak tersedia');
                return false;
            },
            subscribe: function(topic, callback) {
                if (typeof mqttMgr.subscribe === 'function') {
                    return mqttMgr.subscribe(topic, callback);
                }
                console.warn('[mqttManager] subscribe() tidak tersedia');
            },
            unsubscribe: function(topic, callback) {
                if (typeof mqttMgr.unsubscribe === 'function') {
                    return mqttMgr.unsubscribe(topic, callback);
                }
                console.warn('[mqttManager] unsubscribe() tidak tersedia');
            },
            on: function(topic, callback) {
                if (typeof mqttMgr.subscribe === 'function') {
                    return mqttMgr.subscribe(topic, callback);
                }
                console.warn('[mqttManager] on() tidak tersedia');
            },
            onStatusChange: function(callback) {
                if (typeof mqttMgr.subscribeSystem === 'function') {
                    return mqttMgr.subscribeSystem(callback);
                }
                console.warn('[mqttManager] onStatusChange() tidak tersedia');
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
            // Tambahkan client langsung untuk akses lower-level jika diperlukan
            get client() {
                return mqttMgr.client || null;
            }
        };

        // Jika core.js sudah mengekspos mqttManager, timpa dengan wrapper kita
        // (tapi core.js sebenarnya sudah mengekspos MqttManager secara langsung)
        console.log('[mqtt-config] ✅ Berhasil terhubung ke Core System');
        console.log('[mqtt-config] mqttManager wrapper siap');

        if (retryInterval) {
            clearTimeout(retryInterval);
            retryInterval = null;
        }
    }

    // ============================================================
    // 3. FALLBACK (jika Core tidak tersedia)
    // ============================================================
    function createFallback() {
        console.warn('[mqtt-config] Menggunakan fallback konfigurasi');

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
            connect: function() { console.warn('[mqttManager] Fallback: connect() tidak tersedia'); },
            disconnect: function() {},
            publish: function() { console.warn('[mqttManager] Fallback: publish() tidak tersedia'); return false; },
            subscribe: function() {},
            unsubscribe: function() {},
            on: function() {},
            onStatusChange: function() {},
            get status() { return { connected: false }; },
            get client() { return null; }
        };

        if (retryInterval) {
            clearTimeout(retryInterval);
            retryInterval = null;
        }
    }

    // ============================================================
    // 4. START
    // ============================================================
    // Tunggu hingga Core siap, atau fallback jika tidak ada
    if (document.readyState === 'complete' || document.readyState === 'interactive') {
        setTimeout(setupFromCore, 200);
    } else {
        document.addEventListener('DOMContentLoaded', function() {
            setTimeout(setupFromCore, 200);
        });
    }

    // Fallback terakhir: jika setelah 5 detik Core belum siap, paksa fallback
    setTimeout(function() {
        if (!global.MQTT_CONFIG || !global.mqttManager || !global.mqttManager.connected) {
            // Cek apakah Core sudah ada tapi belum initialized
            if (global.Core && !global.Core.initialized) {
                // Masih menunggu, biarkan setupFromCore melanjutkan
                return;
            }
            // Jika Core tidak ada sama sekali, paksa fallback
            if (!global.Core) {
                createFallback();
            }
        }
    }, 5000);

    console.log('[mqtt-config] Module loaded (menunggu Core System...)');

})(window);