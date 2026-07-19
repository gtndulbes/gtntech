// ============================================================
// MQTT Configuration - IoT Kandang Ulat Hongkong
// ============================================================

const MQTT_CONFIG = {
    BROKER: 'wss://broker.emqx.io:8084/mqtt', // atau gunakan broker Anda
    OPTIONS: {
        clientId: 'dashboard_' + Math.random().toString(16).substr(2, 8),
        clean: true,
        reconnectPeriod: 3000,
        connectTimeout: 5000,
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
    },
    // Untuk debug
    DEBUG: true,
};

// ============================================================
// MQTT Client Wrapper
// ============================================================
class MqttManager {
    constructor(config) {
        this.config = config;
        this.client = null;
        this.connected = false;
        this.messageHandlers = {};
        this.statusCallbacks = [];
        this.reconnectTimer = null;
    }

    // Inisialisasi koneksi
    connect() {
        if (this.client && this.connected) return;
        try {
            this.client = mqtt.connect(this.config.BROKER, this.config.OPTIONS);
            this.client.on('connect', () => {
                this.connected = true;
                this.log('MQTT Connected');
                this.triggerStatusCallbacks(true);
                // Subscribe ke semua topic yang dibutuhkan
                const topics = Object.values(this.config.TOPICS);
                topics.forEach(topic => {
                    this.client.subscribe(topic, (err) => {
                        if (err) this.log('Subscribe error: ' + topic, err);
                        else this.log('Subscribed to: ' + topic);
                    });
                });
                // Kirim status online
                this.publish(this.config.TOPICS.SENSOR_STATUS, {
                    dashboard: 'online',
                    timestamp: new Date().toISOString()
                });
            });

            this.client.on('message', (topic, payload) => {
                try {
                    const data = JSON.parse(payload.toString());
                    this.handleMessage(topic, data);
                } catch (e) {
                    // payload bukan JSON
                    this.log('Raw message: ' + topic + ' => ' + payload.toString());
                }
            });

            this.client.on('close', () => {
                this.connected = false;
                this.log('MQTT Disconnected');
                this.triggerStatusCallbacks(false);
            });

            this.client.on('error', (err) => {
                this.log('MQTT Error:', err);
                this.connected = false;
                this.triggerStatusCallbacks(false);
            });

            this.client.on('reconnect', () => {
                this.log('MQTT Reconnecting...');
            });

        } catch (e) {
            this.log('MQTT Init Error:', e);
        }
    }

    // Register handler untuk topic tertentu
    on(topic, callback) {
        if (!this.messageHandlers[topic]) {
            this.messageHandlers[topic] = [];
        }
        this.messageHandlers[topic].push(callback);
    }

    // Handle incoming message
    handleMessage(topic, data) {
        this.log('Message: ' + topic, data);
        // Panggil handler spesifik topic
        if (this.messageHandlers[topic]) {
            this.messageHandlers[topic].forEach(cb => cb(data, topic));
        }
        // Panggil handler wildcard (untuk semua topic)
        if (this.messageHandlers['*']) {
            this.messageHandlers['*'].forEach(cb => cb(data, topic));
        }
    }

    // Publish data
    publish(topic, data, options) {
        if (!this.connected) {
            this.log('Cannot publish, not connected');
            return false;
        }
        const payload = typeof data === 'string' ? data : JSON.stringify(data);
        this.client.publish(topic, payload, options || { qos: 0, retain: false });
        return true;
    }

    // Subscribe tambahan
    subscribe(topic, callback) {
        if (!this.connected) return;
        this.client.subscribe(topic, (err) => {
            if (!err && callback) {
                // Jika ada callback khusus untuk subscribe ini
                this.on(topic, callback);
            }
        });
    }

    // Status change callback
    onStatusChange(callback) {
        this.statusCallbacks.push(callback);
    }

    triggerStatusCallbacks(connected) {
        this.statusCallbacks.forEach(cb => cb(connected));
    }

    // Disconnect
    disconnect() {
        if (this.client) {
            this.client.end();
            this.connected = false;
        }
    }

    // Logging
    log(...args) {
        if (this.config.DEBUG) {
            console.log('[MQTT]', ...args);
        }
    }
}

// ============================================================
// Inisialisasi instance global
// ============================================================
const mqttManager = new MqttManager(MQTT_CONFIG);