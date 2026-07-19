/**
 * sync.js - Modul Sinkronisasi Multi-User via MQTT
 * Digunakan di semua halaman dashboard
 */

var SyncModule = (function() {
    // ============================================================
    // 1. Konfigurasi
    // ============================================================
    var TOPIC_STATE = 'system/state';
    var TOPIC_CONTROL = 'actuator/control';
    var TOPIC_SETPOINT = 'setting/update';

    // ============================================================
    // 2. State lokal (akan digabung dengan state utama)
    // ============================================================
    var localState = {
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
    // 3. Callback yang akan dipanggil saat state berubah dari MQTT
    // ============================================================
    var onStateChange = null;

    // ============================================================
    // 4. Publish state ke MQTT (dipanggil saat ada perubahan lokal)
    // ============================================================
    function publishState(transport) {
        var target = transport || (typeof globalThis !== 'undefined' && globalThis.mqttManager ? globalThis.mqttManager : null);
        if (!target) return;
        if (target.publish) {
            target.publish(TOPIC_STATE, localState);
        } else if (target.client && target.client.connected && target.client.publish) {
            target.client.publish(TOPIC_STATE, JSON.stringify(localState));
        }
        console.log('📤 Sync: State published', localState);
    }

    // ============================================================
    // 5. Subscribe ke topik state
    // ============================================================
    function subscribeState(transport) {
        var target = transport || (typeof globalThis !== 'undefined' && globalThis.mqttManager ? globalThis.mqttManager : null);
        if (!target) return;
        if (target.on) {
            target.on(TOPIC_STATE, function(data) {
                handleStateMessage(TOPIC_STATE, data);
            });
        }
        if (target.subscribe) {
            target.subscribe(TOPIC_STATE);
        } else if (target.client && target.client.connected && target.client.subscribe) {
            target.client.subscribe(TOPIC_STATE);
        }
        console.log('📥 Sync: Subscribed to ' + TOPIC_STATE);
    }

    // ============================================================
    // 6. Handler saat menerima state dari MQTT
    // ============================================================
    function handleStateMessage(topic, payload) {
        if (topic !== TOPIC_STATE) return;
        try {
            var data = typeof payload === 'string' ? JSON.parse(payload) : payload;
            if (!data || typeof data !== 'object') return;
            var changed = false;
            for (var key in data) {
                if (data.hasOwnProperty(key)) {
                    if (JSON.stringify(localState[key]) !== JSON.stringify(data[key])) {
                        localState[key] = data[key];
                        changed = true;
                    }
                }
            }
            if (changed) {
                localState.timestamp = new Date().toISOString();
                if (onStateChange) onStateChange(localState);
            }
            console.log('📥 Sync: State received', data);
        } catch (e) {
            console.warn('Sync: Invalid JSON', e);
        }
    }

    // ============================================================
    // 7. Update state lokal (dipanggil dari UI)
    // ============================================================
    function updateLocalState(newState, transport) {
        if (!newState || typeof newState !== 'object') return;
        var changed = false;
        for (var key in newState) {
            if (newState.hasOwnProperty(key)) {
                if (JSON.stringify(localState[key]) !== JSON.stringify(newState[key])) {
                    localState[key] = newState[key];
                    changed = true;
                }
            }
        }
        if (changed) {
            localState.timestamp = new Date().toISOString();
            if (onStateChange) onStateChange(localState);
            publishState(transport);
        }
    }

    // ============================================================
    // 8. Set callback untuk perubahan state
    // ============================================================
    function setOnStateChange(callback) {
        onStateChange = callback;
    }

    // ============================================================
    // 9. Public API
    // ============================================================
    return {
        subscribeState: subscribeState,
        handleStateMessage: handleStateMessage,
        updateLocalState: updateLocalState,
        setOnStateChange: setOnStateChange,
        getState: function() { return localState; },
        TOPIC_STATE: TOPIC_STATE,
        TOPIC_CONTROL: TOPIC_CONTROL,
        TOPIC_SETPOINT: TOPIC_SETPOINT
    };
})();

if (typeof window !== 'undefined') {
    window.SyncModule = SyncModule;
}