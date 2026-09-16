````markdown
# 🪱 Smart Ulat Hongkong IoT Dashboard

Sistem **Monitoring & Kontrol Kandang Ulat Hongkong Berbasis IoT** dengan **Fuzzy-PID** dan **Self-Monitoring System**.

Bagian dari Tugas Akhir:

> *"Rancang Bangun Sistem Monitoring dan Kontrol Kandang Ulat Hongkong Berbasis IoT dengan Fuzzy-PID dan Self-Monitoring Sistem"*

---

## 📐 Arsitektur Sistem

```text
ESP32-C3 (Kandang)
        │
        │ MQTT over TLS
        ▼
EMQX Cloud Broker
        │
        ▼
Node.js + Express (Backend)
        │
        ├── WebSocket ──► Dashboard
        │                  (Semua User, Realtime)
        │
        └── HTTPS POST ──► Google Apps Script
                              │
                              ▼
                         Google Sheets
                         (Historical Data)
````

---

## 🛠️ Stack Teknologi

| Layer               | Teknologi                                                                                                    |
| ------------------- | ------------------------------------------------------------------------------------------------------------ |
| **IoT Device**      | ESP32-C3 Super Mini, SHT31-D, MQ-135, INA219, Peltier TEC1-12706, BTS7960, Exhaust Fan, Heatsink Fan, Buzzer |
| **MQTT Broker**     | EMQX Cloud (TLS, port 8883)                                                                                  |
| **Backend**         | Node.js, Express, MQTT.js, WebSocket (ws), dotenv                                                            |
| **Frontend**        | HTML5, Tailwind CSS, Vanilla JavaScript, Chart.js                                                            |
| **Historical Data** | Google Apps Script → Google Sheets                                                                           |
| **Alert**           | Telegram Bot API                                                                                             |
| **Deployment**      | Render                                                                                                       |

---

## 📁 Struktur Project

```text
smart-ulat-dashboard/
│
├── backend/                    → Node.js + Express
│   ├── server.js
│   ├── package.json
│   ├── .env
│   ├── .env.example
│   │
│   ├── config/                 → Konfigurasi terpusat
│   ├── mqtt/                   → MQTT client, topics, handlers, publisher
│   ├── websocket/              → WebSocket server
│   ├── api/                    → REST API routes
│   ├── services/               → Business logic
│   ├── validation/             → Validasi command
│   ├── utils/                  → Logger dan helper
│   └── public/                 → Reserved
│
├── frontend/                   → Static dashboard
│   ├── index.html
│   ├── pages/                  → Halaman per fitur
│   ├── css/                    → Styling SCADA theme
│   ├── js/                     → Logic halaman dan core
│   ├── assets/                 → Icons dan images
│   ├── manifest.json           → PWA manifest
│   └── service-worker.js       → PWA service worker
│
├── google-apps-script/         → Historical logging
│   └── Code.gs
│
├── .gitignore
├── README.md
└── package.json
```

---

## 🚧 Progress Development

Pengembangan sistem dilakukan dalam 20 tahap:

| #  | Step                        |     Status     |
| -- | --------------------------- | :------------: |
| 1  | Project Foundation          | 🔄 In Progress |
| 2  | EMQX + MQTT                 |    ⬜ Pending   |
| 3  | Central State               |    ⬜ Pending   |
| 4  | WebSocket                   |    ⬜ Pending   |
| 5  | Frontend Foundation         |    ⬜ Pending   |
| 6  | Realtime Dashboard          |    ⬜ Pending   |
| 7  | Realtime Charts             |    ⬜ Pending   |
| 8  | Fuzzy-PID Page              |    ⬜ Pending   |
| 9  | Power + Self-Monitoring     |    ⬜ Pending   |
| 10 | Control                     |    ⬜ Pending   |
| 11 | Configuration               |    ⬜ Pending   |
| 12 | Google Apps Script + Sheets |    ⬜ Pending   |
| 13 | History                     |    ⬜ Pending   |
| 14 | Alert System                |    ⬜ Pending   |
| 15 | PWA                         |    ⬜ Pending   |
| 16 | Responsive Optimization     |    ⬜ Pending   |
| 17 | Security + Error Handling   |    ⬜ Pending   |
| 18 | Integration Test            |    ⬜ Pending   |
| 19 | Render Deployment           |    ⬜ Pending   |
| 20 | Final Audit                 |    ⬜ Pending   |

---

## 💻 Persyaratan Sistem

Sebelum menjalankan project, pastikan sistem memiliki:

* **Node.js** v18.x atau lebih baru
* **npm** v9.x atau lebih baru
* **Git** (opsional)
* **Akun EMQX Cloud**
* **Akun Google** untuk Google Sheets dan Apps Script
* **Bot Telegram** (opsional untuk sistem alert)

### Download Node.js

[https://nodejs.org](https://nodejs.org)

### EMQX Cloud

[https://www.emqx.com/en/cloud](https://www.emqx.com/en/cloud)

---

# 🚀 Quick Start

## 1. Clone / Download Project

Jika menggunakan Git:

```bash
git clone <repo-url> smart-ulat-dashboard
cd smart-ulat-dashboard
```

Atau project dapat di-download kemudian masuk ke folder:

```bash
cd smart-ulat-dashboard
```

---

## 2. Setup Backend

Masuk ke folder backend:

```bash
cd backend
```

Kemudian install dependency:

```bash
npm install
```

---

## 3. Konfigurasi Environment

Buat file `.env` berdasarkan `.env.example`.

### Linux / macOS

```bash
cp .env.example .env
```

### Windows PowerShell

```powershell
Copy-Item .env.example .env
```

### Windows CMD

```cmd
copy .env.example .env
```

Kemudian buka:

```text
backend/.env
```

dan isi konfigurasi sesuai kebutuhan.

---

## 4. Jalankan Server

### Development

Jika tersedia script `dev`:

```bash
npm run dev
```

### Production

```bash
npm start
```

Jika server berhasil berjalan, backend akan tersedia pada:

```text
http://localhost:3000
```

---

## 5. Buka Dashboard

Buka browser dan akses:

```text
http://localhost:3000
```

Health check:

```text
http://localhost:3000/api/health
```

---

# 🔐 Environment Variables

Seluruh credential dan konfigurasi sensitif disimpan di:

```text
backend/.env
```

Contoh:

```env
# ==========================================
# SERVER
# ==========================================

PORT=3000
NODE_ENV=development


# ==========================================
# MQTT - EMQX CLOUD
# ==========================================

MQTT_BROKER_URL=
MQTT_USERNAME=
MQTT_PASSWORD=
MQTT_CLIENT_ID=smart-ulat-backend


# ==========================================
# GOOGLE APPS SCRIPT
# ==========================================

GAS_WEB_APP_URL=


# ==========================================
# TELEGRAM
# ==========================================

TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=


# ==========================================
# SIMULATION
# ==========================================

SIMULATION_MODE=true
```

> ⚠️ **PENTING:** Jangan pernah melakukan commit terhadap file `.env`.

File yang boleh di-commit:

```text
.env.example
```

File yang tidak boleh di-commit:

```text
.env
```

---

# 📡 MQTT Topics

Sistem menggunakan MQTT sebagai protokol komunikasi antara ESP32-C3 dengan backend.

| Topic                      | Arah            | Isi                                  |
| -------------------------- | --------------- | ------------------------------------ |
| `smartulat/telemetry`      | ESP32 → Backend | Suhu, kelembaban, NH₃                |
| `smartulat/actuator`       | ESP32 → Backend | Status Peltier, Exhaust, Heatsink    |
| `smartulat/power`          | ESP32 → Backend | Voltage, Current, Power              |
| `smartulat/selfmonitoring` | ESP32 → Backend | Status sensor, WiFi, MQTT, SAFE_MODE |
| `smartulat/status`         | ESP32 → Backend | Mode, uptime, heap                   |
| `smartulat/alert`          | ESP32 → Backend | Pesan alarm                          |
| `smartulat/control`        | Backend → ESP32 | Manual control                       |
| `smartulat/config`         | Backend → ESP32 | Update konfigurasi                   |
| `smartulat/response`       | ESP32 → Backend | Response dari command                |

---

# 🌡️ Parameter Monitoring

Parameter utama yang dipantau sistem:

| Parameter            | Sensor / Sumber | Satuan                 |
| -------------------- | --------------- | ---------------------- |
| Suhu                 | SHT31-D         | °C                     |
| Kelembaban           | SHT31-D         | %RH                    |
| NH₃ / kualitas udara | MQ-135          | ppm                    |
| Tegangan             | INA219          | V                      |
| Arus                 | INA219          | A                      |
| Daya                 | INA219          | W                      |
| Status Peltier       | ESP32-C3        | ON/OFF                 |
| Status Exhaust Fan   | ESP32-C3        | ON/OFF                 |
| Status Heatsink Fan  | ESP32-C3        | ON/OFF                 |
| Status WiFi          | ESP32-C3        | Connected/Disconnected |
| Status MQTT          | ESP32-C3        | Connected/Disconnected |
| Mode Sistem          | ESP32-C3        | AUTO/MANUAL/SAFE       |

---

# 🧠 Fuzzy-PID

Sistem kontrol menggunakan pendekatan **Fuzzy-PID** untuk membantu mempertahankan kondisi lingkungan kandang pada rentang yang ditentukan.

Input kontrol utama berasal dari parameter lingkungan:

```text
Suhu
  │
  ├── Error Suhu
  │
  └── Delta Error Suhu
          │
          ▼
     Fuzzy Controller
          │
          ▼
       PID Control
          │
          ▼
   PWM Peltier / Aktuator
```

Fuzzy controller digunakan untuk menentukan parameter kontrol berdasarkan kondisi aktual kandang.

---

# ⚡ Self-Monitoring System

Self-monitoring digunakan untuk memantau kondisi sistem secara internal.

Parameter yang diperiksa antara lain:

* Status sensor
* Status WiFi
* Status MQTT
* Tegangan
* Arus
* Daya
* Status aktuator
* Kondisi komunikasi
* Heap memory
* Uptime
* SAFE_MODE

Contoh status:

```text
SYSTEM STATUS
────────────────────────
WiFi       : CONNECTED
MQTT       : CONNECTED
SHT31      : OK
MQ-135     : OK
INA219     : OK
Peltier    : OFF
Exhaust    : ON
Heatsink   : ON
SAFE MODE  : FALSE
────────────────────────
```

---

# 🌐 REST API Endpoints

Backend menyediakan REST API untuk komunikasi dengan dashboard.

| Method | Endpoint       | Fungsi                        |
| ------ | -------------- | ----------------------------- |
| `GET`  | `/api/health`  | Health check server           |
| `GET`  | `/api/status`  | Status sistem saat ini        |
| `GET`  | `/api/config`  | Mendapatkan konfigurasi aktif |
| `POST` | `/api/config`  | Memperbarui konfigurasi       |
| `POST` | `/api/control` | Manual control aktuator       |
| `GET`  | `/api/history` | Mengambil historical data     |

---

# 🔄 WebSocket

Dashboard menggunakan WebSocket untuk mendapatkan data realtime.

Alur komunikasi:

```text
ESP32-C3
   │
   │ MQTT
   ▼
EMQX Cloud
   │
   │ MQTT
   ▼
Node.js Backend
   │
   │ WebSocket
   ▼
Dashboard
```

Dengan WebSocket, data sensor dapat diperbarui tanpa melakukan refresh halaman secara manual.

---

# 📊 Dashboard

Dashboard dirancang menggunakan konsep **SCADA-style monitoring interface**.

Informasi utama yang ditampilkan:

* Suhu realtime
* Kelembaban realtime
* NH₃
* Tegangan
* Arus
* Daya
* Status aktuator
* Status MQTT
* Status WiFi
* Status sistem
* Grafik realtime
* Grafik historical
* Fuzzy-PID
* Self-monitoring
* Manual control
* Konfigurasi sistem
* Alert

---

# 📈 Historical Data

Data historis dikirim dari backend menuju:

```text
Node.js Backend
      │
      │ HTTPS POST
      ▼
Google Apps Script
      │
      ▼
Google Sheets
```

Historical data dapat digunakan untuk:

* Analisis perubahan suhu
* Analisis kelembaban
* Analisis kualitas udara
* Analisis konsumsi daya
* Evaluasi performa kontrol
* Evaluasi sistem dalam jangka panjang

---

# 🚨 Alert System

Sistem dapat menggunakan **Telegram Bot API** sebagai media pemberitahuan eksternal.

Contoh kondisi yang dapat menghasilkan alert:

```text
⚠️ ALERT

Suhu kandang melebihi batas.

Suhu   : 31.5 °C
Batas  : 29.0 °C
Status : HIGH
```

Alert juga dapat digunakan untuk kondisi:

* Sensor error
* MQTT disconnected
* WiFi disconnected
* NH₃ tinggi
* Overcurrent
* Tegangan abnormal
* SAFE_MODE aktif
* Aktuator mengalami kondisi abnormal

---

# 🧪 Testing

## Health Check

Gunakan command berikut:

```bash
curl http://localhost:3000/api/health
```

Contoh response:

```json
{
  "status": "ok",
  "service": "smart-ulat-backend",
  "version": "1.0.0",
  "env": "development",
  "uptime": 12.34,
  "timestamp": "2026-01-01T00:00:00.000Z"
}
```

---

## Frontend Test

Buka:

```text
http://localhost:3000
```

Dashboard harus dapat menampilkan:

```text
SERVER OK
```

dengan indikator status server aktif.

---

# 🔍 Troubleshooting

| Gejala                         | Kemungkinan Penyebab         | Solusi                           |
| ------------------------------ | ---------------------------- | -------------------------------- |
| `Cannot find module 'express'` | Dependency belum di-install  | Jalankan `npm install`           |
| `EADDRINUSE :::3000`           | Port 3000 sedang digunakan   | Ubah `PORT` atau hentikan proses |
| Tailwind tidak muncul          | CDN tidak dapat diakses      | Periksa koneksi internet         |
| Server indicator merah         | Backend tidak aktif          | Jalankan server                  |
| MQTT disconnected              | Credential atau broker salah | Periksa `.env`                   |
| Dashboard tidak realtime       | WebSocket bermasalah         | Periksa koneksi WebSocket        |
| Data sensor tidak muncul       | ESP32 tidak publish MQTT     | Periksa koneksi ESP32 dan topic  |
| Historical tidak tersimpan     | GAS URL salah                | Periksa `GAS_WEB_APP_URL`        |
| Telegram tidak menerima alert  | Token/chat ID salah          | Periksa konfigurasi Telegram     |

---

# 🔒 Security

Credential berikut bersifat rahasia:

```text
MQTT_USERNAME
MQTT_PASSWORD
GAS_WEB_APP_URL
TELEGRAM_BOT_TOKEN
TELEGRAM_CHAT_ID
```

Jangan menuliskan credential secara langsung di source code.

Gunakan:

```text
.env
```

dan pastikan `.env` masuk ke `.gitignore`.

Contoh:

```gitignore
.env
node_modules/
*.log
```

---

# 📱 Progressive Web App

Dashboard dirancang agar dapat dikembangkan menjadi **Progressive Web App (PWA)**.

Komponen PWA:

```text
manifest.json
service-worker.js
```

Fitur yang direncanakan:

* Install ke perangkat
* Responsive layout
* Cached assets
* Offline fallback
* App-like experience

---

# ☁️ Deployment

Backend direncanakan untuk di-deploy menggunakan:

**Render**

Arsitektur deployment:

```text
ESP32-C3
    │
    ▼
EMQX Cloud
    │
    ▼
Render
    │
    ├── Node.js Backend
    │
    └── WebSocket
           │
           ▼
       Web Dashboard
```

Sebelum deployment, ubah:

```env
NODE_ENV=production
```

dan pastikan seluruh environment variables sudah dikonfigurasi pada platform deployment.

---

# 🗂️ Development Workflow

Urutan pengembangan:

```text
1. Project Foundation
        ↓
2. EMQX + MQTT
        ↓
3. Central State
        ↓
4. WebSocket
        ↓
5. Frontend Foundation
        ↓
6. Realtime Dashboard
        ↓
7. Realtime Charts
        ↓
8. Fuzzy-PID
        ↓
9. Power + Self-Monitoring
        ↓
10. Control
        ↓
11. Configuration
        ↓
12. Google Apps Script
        ↓
13. History
        ↓
14. Alert System
        ↓
15. PWA
        ↓
16. Responsive Optimization
        ↓
17. Security + Error Handling
        ↓
18. Integration Test
        ↓
19. Render Deployment
        ↓
20. Final Audit
```

---

# 📋 Final Audit Checklist

Sebelum sistem dinyatakan selesai, pastikan:

* [ ] Backend dapat berjalan tanpa error
* [ ] MQTT berhasil terhubung ke EMQX
* [ ] ESP32 dapat publish telemetry
* [ ] Backend menerima data MQTT
* [ ] WebSocket berjalan realtime
* [ ] Dashboard menampilkan data sensor
* [ ] Grafik realtime berjalan
* [ ] Fuzzy-PID dapat ditampilkan
* [ ] Power monitoring berjalan
* [ ] Self-monitoring berjalan
* [ ] Manual control berjalan
* [ ] Configuration berjalan
* [ ] Google Sheets menerima historical data
* [ ] History dapat ditampilkan
* [ ] Telegram alert berjalan
* [ ] PWA dapat di-install
* [ ] Tampilan responsive
* [ ] Error handling berjalan
* [ ] Security configuration diperiksa
* [ ] Integration test berhasil
* [ ] Deployment Render berhasil
* [ ] Final audit selesai

---

# 📚 Referensi Teknologi

* Node.js
  [https://nodejs.org](https://nodejs.org)

* EMQX Cloud
  [https://www.emqx.com/en/cloud](https://www.emqx.com/en/cloud)

* Express.js
  [https://expressjs.com](https://expressjs.com)

* MQTT.js
  [https://github.com/mqttjs/MQTT.js](https://github.com/mqttjs/MQTT.js)

* Chart.js
  [https://www.chartjs.org](https://www.chartjs.org)

* Google Apps Script
  [https://developers.google.com/apps-script](https://developers.google.com/apps-script)

* Telegram Bot API
  [https://core.telegram.org/bots/api](https://core.telegram.org/bots/api)

* Render
  [https://render.com](https://render.com)

---

# 📄 Lisensi

Project ini dibuat untuk keperluan akademik sebagai bagian dari **Tugas Akhir**.

**Universitas Wijayakusuma Purwokerto**
Program Studi Teknik Elektro
Fakultas Teknik
2026

---

# 👨‍💻 Author

**Muhammad Gatan Rifani**

NIM: **23410300617**

Program Studi Teknik Elektro
Universitas Wijayakusuma Purwokerto

---

## 📌 Project Status

**Current Version:** `1.0.0`

**Development Status:** 🔄 In Development

**Platform:** ESP32-C3 + EMQX Cloud + Node.js + Web Dashboard

**Control Method:** Fuzzy-PID

**Monitoring:** Realtime + Historical

**Communication:** MQTT over TLS + WebSocket + HTTPS
