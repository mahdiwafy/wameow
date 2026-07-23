# 🐱 Wameow

**Wameow** is a lightweight, human-simulated WhatsApp HTTP REST API bridge written in Go using [`whatsmeow`](https://github.com/tulir/whatsmeow).

Designed as a safer, low-resource alternative to heavy Node/Puppeteer/WAHA engines, Wameow provides human typing simulation, permanent SQLite message archiving, and multi-session management.

---

## ✨ Features

- 🤖 **Human Typing Simulation:** Realistic typing delays (30–50 WPM), interactive chat presence (`composing`/`paused`), automatic read receipts, and randomized inter-session check-in delays to prevent account restrictions.
- 💾 **Permanent SQLite Message Archiving:** Automatically archives all incoming messages, outgoing responses, and initial `HistorySync` batches into an indexed SQLite database (`wameow.db`).
- 🔍 **Full History Search API:** Search archived chats and messages by contact name, phone number, username (LID), or message body text.
- ⚡ **Lightweight & Fast:** Single compiled Go binary running comfortably within `GOMEMLIMIT=128MiB`.
- 📷 **Direct PNG QR Code API:** `/png-qr/<session>` returns stable, un-flickering QR codes directly for instant dashboard integration.
- 📲 **Multi-Session Support:** Run multiple WhatsApp accounts (`wa1`, `wa2`, `wa3`) under a single process.

---

## 🚀 REST API Endpoints

| Endpoint | Method | Description |
| :--- | :--- | :--- |
| `POST /send` | `POST` | Send text message with human typing simulation |
| `POST /send-media` | `POST` | Send images, videos, audio, or document files |
| `POST /typing` | `POST` | Manually trigger typing indicator (`composing`/`paused`) |
| `POST /revoke` | `POST` | Revoke/delete a sent message |
| `GET /png-qr/:session` | `GET` | Render stable QR code as PNG image |
| `GET /sessions` | `GET` | List active sessions and connection statuses |
| `GET /history/chats?session=wa1` | `GET` | List archived chats with message counts & latest message |
| `GET /history/messages?session=wa1&chatId=...` | `GET` | Get historical messages for a specific chat |
| `GET /history/search?q=query&session=wa1` | `GET` | Search archived messages by name, number, username, or text |
| `POST /ping` | `POST` | Trigger human-simulated heartbeat across active sessions |
| `GET /health` | `GET` | Service uptime and connection health status |

---

## ⚙️ Usage

### Build & Run

```bash
# Build binary
go build -o wameow .

# Run with sessions
./wameow \
  --sessions=wa1,wa2,wa3 \
  --jids=6285156487274,6285111041956,6281807177704 \
  --webhook=http://localhost:8090/api/waha/webhook \
  --listen=:52135
```

---

## 📜 Systemd Service Setup

```ini
[Unit]
Description=Wameow — WhatsApp bridge (whatsmeow, 3 sessions)
After=network-online.target

[Service]
Type=simple
User=mahdiwafy
WorkingDirectory=/home/mahdiwafy/wameow
ExecStart=/home/mahdiwafy/wameow/wameow --sessions=wa1,wa2,wa3 --jids=6285156487274,6285111041956,6281807177704 --webhook=http://localhost:8090/api/waha/webhook --listen=:52135
Restart=always
Environment=GOMEMLIMIT=128MiB

[Install]
WantedBy=multi-user.target
```
