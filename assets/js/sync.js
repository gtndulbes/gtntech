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
        health: 97,
        alarms: []
    };

    // ============================================================
    // 3. Callback yang akan dipanggil saat state berubah dari MQTT
    // ============================================================
    var onStateChange = null;

    // ============================================================
    // 4. Publish state ke MQTT (dipanggil saat ada perubahan lokal)
    // ============================================================
    function publishState(mqttClient) {
        if (!mqttClient || !mqttClient.connected) return;
        var payload = JSON.stringify(localState);
        mqttClient.publish(TOPIC_STATE, payload);
        console.log('📤 Sync: State published', localState);
    }

    // ============================================================
    // 5. Subscribe ke topik state
    // ============================================================
    function subscribeState(mqttClient) {
        if (!mqttClient) return;
        mqttClient.subscribe(TOPIC_STATE);
        console.log('📥 Sync: Subscribed to ' + TOPIC_STATE);
    }

    // ============================================================
    // 6. Handler saat menerima state dari MQTT
    // ============================================================
    function handleStateMessage(topic, payload) {
        if (topic !== TOPIC_STATE) return;
        try {
            var data = JSON.parse(payload);
            // Update local state
            var changed = false;
            for (var key in data) {
                if (data.hasOwnProperty(key) && localState[key] !== undefined) {
                    if (JSON.stringify(localState[key]) !== JSON.stringify(data[key])) {
                        localState[key] = data[key];
                        changed = true;
                    }
                }
            }
            if (changed && onStateChange) {
                onStateChange(localState);
            }
            console.log('📥 Sync: State received', data);
        } catch (e) {
            console.warn('Sync: Invalid JSON', e);
        }
    }

    // ============================================================
    // 7. Update state lokal (dipanggil dari UI)
    // ============================================================
    function updateLocalState(newState, mqttClient) {
        var changed = false;
        for (var key in newState) {
            if (newState.hasOwnProperty(key) && localState[key] !== undefined) {
                if (JSON.stringify(localState[key]) !== JSON.stringify(newState[key])) {
                    localState[key] = newState[key];
                    changed = true;
                }
            }
        }
        if (changed) {
            if (onStateChange) onStateChange(localState);
            publishState(mqttClient);
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