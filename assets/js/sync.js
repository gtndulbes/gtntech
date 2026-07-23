/**
 * sync.js - Modul Sinkronisasi Multi-User via MQTT
 * Terintegrasi dengan Core System (StateManager, MqttManager)
 *
 * @author Muhammad Gatan Rifani
 * @version 3.0.1
 */

(function(global) {
    'use strict';

    // ============================================================
    // 1. KONFIGURASI
    // ============================================================
    var TOPIC_STATE = 'system/state';
    var TOPIC_CONTROL = 'actuator/control';
    var TOPIC_SETPOINT = 'setting/update';

    // ============================================================
    // 2. STATE REFERENCE (dari Core)
    // ============================================================
    var _stateManager = null;
    var _mqttManager = null;
    var _onStateChange = null;
    var _isInitialized = false;
    var _initRetries = 0;
    var MAX_INIT_RETRIES = 15;

    // ============================================================
    // 3. INISIALISASI (dengan pengecekan Core siap)
    // ============================================================
    function init() {
        if (_isInitialized) return;

        // Tunggu Core System siap (maksimal 15 percobaan)
        if (typeof global.Core === 'undefined' || !global.Core.initialized) {
            if (_initRetries < MAX_INIT_RETRIES) {
                _initRetries++;
                setTimeout(init, 300);
                return;
            }
            console.warn('[Sync] Core belum siap setelah batas waktu, menggunakan fallback');
        }

        // Ambil referensi dari global Core
        _stateManager = global.Core.getStateManager ? global.Core.getStateManager() : null;
        _mqttManager = global.Core.getMqttManager ? global.Core.getMqttManager() : null;

        // Fallback: cari global StateManager/MqttManager (jika core tidak ditemukan)
        if (!_stateManager && typeof global.StateManager !== 'undefined') {
            _stateManager = global.StateManager;
        }
        if (!_mqttManager && typeof global.MqttManager !== 'undefined') {
            _mqttManager = global.MqttManager;
        }

        // Jika masih tidak ada, coba dari window (untuk kompatibilitas)
        if (!_stateManager && global.window && global.window.StateManager) {
            _stateManager = global.window.StateManager;
        }
        if (!_mqttManager && global.window && global.window.MqttManager) {
            _mqttManager = global.window.MqttManager;
        }

        // Fallback terakhir: gunakan mqttManager yang diekspos core.js ke window
        if (!_mqttManager && global.window && global.window.mqttManager) {
            _mqttManager = global.window.mqttManager;
        }

        if (_stateManager) {
            // Daftarkan listener ke StateManager
            if (typeof _stateManager.addListener === 'function') {
                _stateManager.addListener('*', function(key, value, oldValue) {
                    if (_onStateChange) {
                        var state = _stateManager.getState ? _stateManager.getState() : _stateManager.state;
                        _onStateChange(state, key, value, oldValue);
                    }
                });
                console.log('[Sync] Terintegrasi dengan StateManager');
            } else {
                console.warn('[Sync] StateManager tidak memiliki addListener');
            }
        } else {
            console.warn('[Sync] StateManager tidak ditemukan, menggunakan localState fallback');
        }

        if (_mqttManager) {
            // Subscribe ke topik state
            if (typeof _mqttManager.subscribe === 'function') {
                _mqttManager.subscribe(TOPIC_STATE, function(data) {
                    handleStateMessage(TOPIC_STATE, data);
                });
            } else if (_mqttManager.client && typeof _mqttManager.client.subscribe === 'function') {
                _mqttManager.client.subscribe(TOPIC_STATE);
            }
            console.log('[Sync] Terintegrasi dengan MqttManager');
        } else {
            console.warn('[Sync] MqttManager tidak ditemukan');
        }

        _isInitialized = true;
    }

    // ============================================================
    // 4. AMBIL STATE (dari Core atau localState fallback)
    // ============================================================
    function getState() {
        if (_stateManager) {
            if (typeof _stateManager.getState === 'function') {
                return _stateManager.getState();
            }
            if (_stateManager.state) {
                return _stateManager.state;
            }
        }
        // Fallback ke localState jika core tidak tersedia
        return _localState;
    }

    // ============================================================
    // 5. UPDATE STATE (via StateManager atau localState fallback)
    // ============================================================
    function updateState(updates, options) {
        options = options || { publish: true, sync: true, silent: false };

        if (_stateManager) {
            if (typeof _stateManager.update === 'function') {
                _stateManager.update(updates, options);
                return;
            }
            if (typeof _stateManager.set === 'function') {
                for (var key in updates) {
                    if (updates.hasOwnProperty(key)) {
                        _stateManager.set(key, updates[key], options);
                    }
                }
                return;
            }
        }

        // Fallback: update localState langsung
        _localState = _localState || {};
        for (var key in updates) {
            if (updates.hasOwnProperty(key)) {
                _localState[key] = updates[key];
            }
        }
        if (!options.silent && _onStateChange) {
            _onStateChange(_localState, 'update', updates);
        }
        // Publish via MQTT jika ada
        if (options.publish) {
            publishState();
        }
    }

    // ============================================================
    // 6. PUBLISH STATE VIA MQTT (DIPERBAIKI: stringify object)
    // ============================================================
    function publishState() {
        var state = getState();
        var target = _mqttManager || (global.window && global.window.mqttManager);

        if (!target) {
            console.warn('[Sync] Tidak ada MQTT manager untuk publish state');
            return;
        }

        // Pastikan payload berupa string JSON (MQTT hanya menerima string/buffer)
        var payload = typeof state === 'string' ? state : JSON.stringify(state);

        // Pilih metode publish yang tersedia
        if (typeof target.publish === 'function') {
            target.publish(TOPIC_STATE, payload);
        } else if (target.client && typeof target.client.publish === 'function') {
            target.client.publish(TOPIC_STATE, payload);
        } else {
            console.warn('[Sync] Tidak ada metode publish yang tersedia');
            return;
        }

        console.log('📤 Sync: State published');
    }

    // ============================================================
    // 7. SUBSCRIBE STATE VIA MQTT
    // ============================================================
    function subscribeState() {
        var target = _mqttManager || (global.window && global.window.mqttManager);

        if (!target) {
            console.warn('[Sync] Tidak ada MQTT manager untuk subscribe state');
            return;
        }

        if (typeof target.subscribe === 'function') {
            target.subscribe(TOPIC_STATE, function(data) {
                handleStateMessage(TOPIC_STATE, data);
            });
        } else if (typeof target.on === 'function') {
            target.on(TOPIC_STATE, function(data) {
                handleStateMessage(TOPIC_STATE, data);
            });
        } else if (target.client && typeof target.client.subscribe === 'function') {
            target.client.subscribe(TOPIC_STATE);
        }

        console.log('📥 Sync: Subscribed to ' + TOPIC_STATE);
    }

    // ============================================================
    // 8. HANDLER STATE DARI MQTT
    // ============================================================
    function handleStateMessage(topic, payload) {
        if (topic !== TOPIC_STATE) return;

        try {
            var data = typeof payload === 'string' ? JSON.parse(payload) : payload;
            if (!data || typeof data !== 'object') return;

            // Jika data diterima dari MQTT, update state via StateManager
            if (_stateManager && typeof _stateManager.update === 'function') {
                // Jangan publish balik agar tidak infinite loop
                _stateManager.update(data, { publish: false, sync: false, silent: false });
            } else {
                // Fallback: update localState
                var changed = false;
                for (var key in data) {
                    if (data.hasOwnProperty(key)) {
                        if (JSON.stringify(_localState[key]) !== JSON.stringify(data[key])) {
                            _localState[key] = data[key];
                            changed = true;
                        }
                    }
                }
                if (changed && _onStateChange) {
                    _onStateChange(_localState, 'sync', data);
                }
            }

            console.log('📥 Sync: State received from MQTT', data);
        } catch (e) {
            console.warn('[Sync] Error processing state message:', e);
        }
    }

    // ============================================================
    // 9. UPDATE LOCAL STATE (dipanggil dari UI) - DIPERBAIKI: stringify
    // ============================================================
    function updateLocalState(newState, transport) {
        if (!newState || typeof newState !== 'object') return;

        // Gabungkan dengan state saat ini
        var currentState = getState();
        var updates = {};

        for (var key in newState) {
            if (newState.hasOwnProperty(key)) {
                if (JSON.stringify(currentState[key]) !== JSON.stringify(newState[key])) {
                    updates[key] = newState[key];
                }
            }
        }

        if (Object.keys(updates).length === 0) return;

        // Update via StateManager
        updateState(updates, { publish: true, sync: true, silent: false });

        // Kirim ke transport jika diberikan dan berbeda
        if (transport && transport !== _mqttManager) {
            var payload = typeof updates === 'string' ? updates : JSON.stringify(updates);
            if (typeof transport.publish === 'function') {
                transport.publish(TOPIC_STATE, payload);
            } else if (transport.client && typeof transport.client.publish === 'function') {
                transport.client.publish(TOPIC_STATE, payload);
            }
        }
    }

    // ============================================================
    // 10. SET CALLBACK UNTUK PERUBAHAN STATE
    // ============================================================
    function setOnStateChange(callback) {
        _onStateChange = callback;

        // Jika StateManager sudah ada, kita sudah pasang listener di init()
        // Tapi kita juga bisa langsung panggil callback dengan state awal
        if (_stateManager) {
            var state = getState();
            if (state && callback) {
                callback(state, 'init', null);
            }
        }
    }

    // ============================================================
    // 11. LOCAL STATE FALLBACK
    // ============================================================
    var _localState = {
        mode: 'AUTO',
        peltierOn: false,
        peltierPwm: 0,
        fanOn: false,
        fanPwm: 0,
        coolFanOn: false,
        coolFanPwm: 0,
        setpointTemp: 27.5,
        setpointHum: 75,
        kp: 2.0,
        ki: 0.5,
        kd: 0.1,
        error: 0,
        deltaError: 0,
        outputPid: 0,
        temp: 27.5,
        hum: 75,
        nh3: 5,
        health: 97,
        alarms: [],
        notifications: [],
        logs: [],
        mqttConnected: false,
        wifiConnected: true,
        sheetConnected: true,
        timestamp: null
    };

    // ============================================================
    // 12. PUBLIC API
    // ============================================================
    var SyncModule = {
        init: init,
        getState: getState,
        updateState: updateState,
        updateLocalState: updateLocalState,
        publishState: publishState,
        subscribeState: subscribeState,
        handleStateMessage: handleStateMessage,
        setOnStateChange: setOnStateChange,
        TOPIC_STATE: TOPIC_STATE,
        TOPIC_CONTROL: TOPIC_CONTROL,
        TOPIC_SETPOINT: TOPIC_SETPOINT,
        _localState: _localState, // Untuk debug
    };

    // ============================================================
    // 13. EXPOSE KE GLOBAL
    // ============================================================
    if (typeof global !== 'undefined') {
        global.SyncModule = SyncModule;
    }
    if (typeof window !== 'undefined') {
        window.SyncModule = SyncModule;
    }

    // ============================================================
    // 14. AUTO-INIT (dengan pengecekan document ready)
    // ============================================================
    // Tunggu Core siap (core.js akan memuat dan menjalankan auto-init juga)
    if (document.readyState === 'complete' || document.readyState === 'interactive') {
        setTimeout(function() {
            SyncModule.init();
        }, 300);
    } else {
        document.addEventListener('DOMContentLoaded', function() {
            setTimeout(function() {
                SyncModule.init();
            }, 300);
        });
    }

    console.log('[Sync] Module loaded');

    return SyncModule;

})(window);