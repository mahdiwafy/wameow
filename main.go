package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"
	qrcode "github.com/skip2/go-qrcode"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
)

var (
	clients      = map[string]*whatsmeow.Client{}
	container    *sqlstore.Container
	webhookURL   *string
	listenAddr   *string
	sessionNames []string
	ownPhones    []string // our WA numbers (wa1/wa2/wa3) — skip as group authors
	pingGroup    = types.JID{}
	startedAt    = time.Now()

	qrMu      sync.Mutex
	qrCodes   = map[string][]string{}
	qrWaiting = map[string]chan struct{}{}
	qrActive  = map[string]bool{}

	msgDB *sql.DB
)

func initMsgDB() {
	var err error
	msgDB, err = sql.Open("sqlite3", "file:wameow.db?_journal_mode=WAL&_foreign_keys=on")
	if err != nil {
		log.Printf("initMsgDB: %v", err)
		return
	}
	_, err = msgDB.Exec(`
		CREATE TABLE IF NOT EXISTS messages (
			id TEXT PRIMARY KEY,
			session TEXT NOT NULL,
			chat_id TEXT NOT NULL,
			sender TEXT NOT NULL,
			sender_name TEXT,
			from_me INTEGER NOT NULL,
			timestamp INTEGER NOT NULL,
			body TEXT,
			msg_type TEXT,
			has_media INTEGER
		);
		CREATE INDEX IF NOT EXISTS idx_messages_chat ON messages(session, chat_id, timestamp DESC);
		CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp DESC);
		CREATE INDEX IF NOT EXISTS idx_messages_body ON messages(body);
	`)
	if err != nil {
		log.Printf("initMsgDB schema err: %v", err)
	} else {
		log.Println("wameow: permanent message archive DB initialized")
	}
	// Add optional columns for existing databases (ignore errors if already exist)
	msgDB.Exec(`ALTER TABLE messages ADD COLUMN file_name TEXT`)
	msgDB.Exec(`ALTER TABLE messages ADD COLUMN file_size INTEGER`)
	msgDB.Exec(`ALTER TABLE messages ADD COLUMN local_path TEXT`)
}

func saveMessage(session string, id string, chatId string, sender string, pushName string, fromMe bool, timestamp int64, body string, msgType string, hasMedia bool, extra ...map[string]interface{}) {
	if msgDB == nil || id == "" {
		return
	}
	fm := 0
	if fromMe {
		fm = 1
	}
	hm := 0
	if hasMedia {
		hm = 1
	}
	if msgType == "" {
		msgType = "text"
	}

	fileName := ""
	fileSize := int64(0)
	localPath := ""
	if len(extra) > 0 {
		if v, ok := extra[0]["fileName"]; ok {
			fileName, _ = v.(string)
		}
		if v, ok := extra[0]["fileSize"]; ok {
			fileSize, _ = v.(int64)
		}
		if v, ok := extra[0]["localPath"]; ok {
			localPath, _ = v.(string)
		}
	}

	_, _ = msgDB.Exec(`
		INSERT INTO messages (id, session, chat_id, sender, sender_name, from_me, timestamp, body, msg_type, has_media, file_name, file_size, local_path)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			body=excluded.body,
			sender_name=COALESCE(NULLIF(excluded.sender_name, ''), messages.sender_name),
			file_name=COALESCE(NULLIF(excluded.file_name, ''), messages.file_name),
			file_size=COALESCE(NULLIF(excluded.file_size, 0), messages.file_size),
			local_path=COALESCE(NULLIF(excluded.local_path, ''), messages.local_path)
	`, id, session, chatId, sender, pushName, fm, timestamp, body, msgType, hm, fileName, fileSize, localPath)
}

func main() {
	webhookURL = flag.String("webhook", "http://localhost:8090/api/waha/webhook", "Webhook URL")
	listenAddr = flag.String("listen", ":52135", "HTTP API address")
	sn := flag.String("sessions", "wa1,wa2,wa3", "Session names (comma)")
	sj := flag.String("jids", "6285156487274,6285111041956,6281807177704", "Phone numbers (comma)")
	pg := flag.String("ping-group", "120363426307758670@g.us", "Ping group JID")
	flag.Parse()

	sessionNames = splitCSV(*sn)
	ownPhones = splitCSV(*sj)
	if len(sessionNames) != len(ownPhones) {
		log.Fatalf("session names and jids count mismatch")
	}

	var err error
	pingGroup, err = types.ParseJID(*pg)
	if err != nil {
		log.Fatalf("invalid ping-group: %v", err)
	}

	ctx := context.Background()

	initMsgDB()

	container, err = sqlstore.New(ctx, "sqlite3", "file:wameow.db?_journal_mode=WAL&_foreign_keys=on", waLog.Stdout("DB", "WARN", true))
	if err != nil {
		log.Fatalf("sqlstore.New: %v", err)
	}
	if err := container.Upgrade(ctx); err != nil {
		log.Fatalf("container.Upgrade: %v", err)
	}

	devices := make(map[string]*store.Device)

	// Try GetDevice by JID first
	for i, name := range sessionNames {
		jid, _ := types.ParseJID(ownPhones[i] + "@s.whatsapp.net")
		dev, _ := container.GetDevice(ctx, jid)
		if dev != nil {
			devices[name] = dev
			log.Printf("%s: existing device (JID=%s)", name, dev.ID.User)
		}
	}

	// Fallback: get ALL devices and match by stored jid
	if len(devices) < len(sessionNames) {
		allDevs, _ := container.GetAllDevices(ctx)
		assigned := map[string]bool{}
		for _, d := range devices {
			assigned[d.ID.User] = true
		}
		for _, dev := range allDevs {
			if dev.ID == nil || assigned[dev.ID.User] {
				continue
			}
			// Match by jid suffix
			for i, num := range ownPhones {
				name := sessionNames[i]
				if _, ok := devices[name]; ok {
					continue
				}
				if strings.HasSuffix(dev.ID.User, num) {
					devices[name] = dev
					assigned[dev.ID.User] = true
					log.Printf("%s: recovered device (JID=%s)", name, dev.ID.User)
					break
				}
			}
		}
	}

	for _, name := range sessionNames {
		if _, ok := devices[name]; !ok {
			devices[name] = container.NewDevice()
			log.Printf("%s: new device (unpaired)", name)
		}
	}

	for _, name := range sessionNames {
		dev := devices[name]
		cli := whatsmeow.NewClient(dev, waLog.Stdout(name, "INFO", true))
		cli.AddEventHandler(handlerFor(name, cli))
		clients[name] = cli
		if dev.ID != nil {
			if err := cli.Connect(); err != nil {
				log.Printf("%s: Connect: %v — retrying in background", name, err)
				go func(n string, c *whatsmeow.Client) {
					for {
						time.Sleep(10 * time.Second)
						if c.IsConnected() {
							return
						}
						if err := c.Connect(); err != nil {
							log.Printf("%s: retry Connect: %v", n, err)
						} else {
							log.Printf("%s: connected (on retry)", n)
							return
						}
					}
				}(name, cli)
			} else {
				log.Printf("%s: connected", name)
			}
		} else {
			log.Printf("%s: unpaired", name)
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/send", handleSend)
	mux.HandleFunc("/typing", handleTyping)
	mux.HandleFunc("/send-media", handleSendMedia)
	mux.HandleFunc("/edit", handleEdit)
	mux.HandleFunc("/revoke", handleRevoke)
	mux.HandleFunc("/reconnect/", handleReconnect)
	mux.HandleFunc("/qr/", handleQR)
	mux.HandleFunc("/png-qr/", handlePNGQR)
	mux.HandleFunc("/sessions", handleSessions)
	mux.HandleFunc("/chats/", handleChats)
	mux.HandleFunc("/contacts/", handleContactLookup)
	mux.HandleFunc("/health", handleHealth)
	mux.HandleFunc("/ping", handlePing)
	mux.HandleFunc("/history/messages", handleHistoryMessages)
	mux.HandleFunc("/history/chats", handleHistoryChats)
	mux.HandleFunc("/history/search", handleHistorySearch)

	server := &http.Server{Addr: *listenAddr, Handler: mux}
	go func() {
		log.Printf("HTTP on %s", *listenAddr)
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("HTTP: %v", err)
		}
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	log.Println("shutting down...")
	server.Shutdown(ctx)
	for name, cli := range clients {
		cli.Disconnect()
		log.Printf("%s: disconnected", name)
	}
}

func handlerFor(session string, cli *whatsmeow.Client) whatsmeow.EventHandler {
	return func(evt interface{}) {
		ctx := context.Background()
		switch v := evt.(type) {
		case *events.Message:
			info := v.Info
			// Skip messages we ourselves sent on this session (prevents self-echo).
			if info.IsFromMe {
				return
			}
			// Group sender JID (human). Empty for DMs.
			sender := info.Sender.ToNonAD().String()
			// Skip if the group author is one of OUR numbers (other linked sessions /
			// bot replies mirrored into the group). Prevents multi-session loops.
			if senderPhone := info.Sender.User; senderPhone != "" {
				for _, own := range ownPhones {
					if senderPhone == own || strings.HasSuffix(senderPhone, own) {
						log.Printf("%s: skip own-number author %s", session, senderPhone)
						return
					}
				}
			}
			payload := map[string]interface{}{
				"from":        info.Chat.String(),
				"fromMe":      false,
				"id":          info.ID,
				"pushName":    info.PushName,
				"timestamp":   info.Timestamp.Unix(),
				"participant": sender,
				"author":      sender,
			}
			msg := v.Message
			if msg == nil {
				return
			}
			if conv := msg.GetConversation(); conv != "" {
				payload["body"] = conv
			}
			if ext := msg.GetExtendedTextMessage(); ext != nil {
				if t := ext.GetText(); t != "" {
					payload["body"] = t
				}
			}
			if img := msg.GetImageMessage(); img != nil {
				payload["body"] = img.GetCaption()
				payload["hasMedia"] = true
				payload["type"] = "image"
				data, err := cli.Download(ctx, img)
				if err == nil {
					mime := img.GetMimetype()
					if mime == "image/*" || mime == "" {
						mime = "image/jpeg"
					}
					payload["media"] = map[string]interface{}{
						"url":      fmt.Sprintf("data:%s;base64,%s", mime, base64.StdEncoding.EncodeToString(data)),
						"mimetype": mime,
					}
				}
			}
			if vid := msg.GetVideoMessage(); vid != nil {
				payload["body"] = vid.GetCaption()
				payload["hasMedia"] = true
				payload["type"] = "video"
			}
			if aud := msg.GetAudioMessage(); aud != nil {
				payload["hasMedia"] = true
				payload["type"] = "audio"
				payload["isPtt"] = aud.GetPTT()
				if data, err := cli.Download(ctx, aud); err == nil {
					mime := "audio/ogg"
					if m := aud.GetMimetype(); m != "" {
						mime = m
					}
					payload["media"] = map[string]interface{}{
						"url":      fmt.Sprintf("data:%s;base64,%s", mime, base64.StdEncoding.EncodeToString(data)),
						"mimetype": mime,
					}
					log.Printf("wameow: downloaded audio msg_id=%s size=%d", info.ID, len(data))
				} else {
					log.Printf("wameow: audio download failed msg_id=%s err=%v, forwarding text-only", info.ID, err)
					payload["body"] = "[voice note — download gagal, ketik manual]"
				}
			}
			if doc := msg.GetDocumentMessage(); doc != nil {
				payload["body"] = doc.GetCaption()
				payload["hasMedia"] = true
				payload["type"] = "document"
				docName := doc.GetFileName()
				if docName == "" {
					docName = fmt.Sprintf("doc_%s", info.ID)
				}
				payload["fileName"] = docName
				if data, err := cli.Download(ctx, doc); err == nil {
					os.MkdirAll("/home/mahdiwafy/.hermes/cache/documents", 0755)
					savePath := fmt.Sprintf("/home/mahdiwafy/.hermes/cache/documents/%s_%s", info.ID, docName)
					if err := os.WriteFile(savePath, data, 0644); err == nil {
						log.Printf("wameow: saved document to %s", savePath)
						payload["body"] = fmt.Sprintf("[Document saved: %s]", savePath)
						payload["localPath"] = savePath
						payload["fileSize"] = len(data)
					}
				} else {
					log.Printf("wameow: doc download failed msg_id=%s err=%v", info.ID, err)
				}
			}

			// Archive incoming message to SQLite
			bodyStr := ""
			if b, ok := payload["body"].(string); ok {
				bodyStr = b
			}
			msgTypeStr := "text"
			if t, ok := payload["type"].(string); ok {
				msgTypeStr = t
			}
			hasMediaBool := false
			if hm, ok := payload["hasMedia"].(bool); ok {
				hasMediaBool = hm
			}
			extraMap := map[string]interface{}{}
			if fn, ok := payload["fileName"].(string); ok {
				extraMap["fileName"] = fn
			}
			if lp, ok := payload["localPath"].(string); ok {
				extraMap["localPath"] = lp
			}
			if fs, ok := payload["fileSize"].(int); ok {
				extraMap["fileSize"] = int64(fs)
			}
			saveMessage(session, info.ID, info.Chat.String(), sender, info.PushName, false, info.Timestamp.Unix(), bodyStr, msgTypeStr, hasMediaBool, extraMap)

			go forwardWebhook(session, payload)

		case *events.HistorySync:
			if v.Data == nil {
				return
			}
			count := 0
			for _, conv := range v.Data.GetConversations() {
				chatID := conv.GetID()
				for _, hmsg := range conv.GetMessages() {
					webMsg := hmsg.GetMessage()
					if webMsg == nil || webMsg.GetKey() == nil {
						continue
					}
					key := webMsg.GetKey()
					msgID := key.GetID()
					fromMe := key.GetFromMe()
					senderJID := key.GetParticipant()
					if senderJID == "" {
						senderJID = key.GetRemoteJID()
					}
					ts := int64(webMsg.GetMessageTimestamp())
					pushName := webMsg.GetPushName()

					m := webMsg.GetMessage()
					if m == nil {
						continue
					}
					body := m.GetConversation()
					msgType := "text"
					hasMedia := false

					if ext := m.GetExtendedTextMessage(); ext != nil && ext.GetText() != "" {
						body = ext.GetText()
					}
					if img := m.GetImageMessage(); img != nil {
						body = img.GetCaption()
						msgType = "image"
						hasMedia = true
					}
					if vid := m.GetVideoMessage(); vid != nil {
						body = vid.GetCaption()
						msgType = "video"
						hasMedia = true
					}
					if aud := m.GetAudioMessage(); aud != nil {
						msgType = "audio"
						hasMedia = true
					}
					if doc := m.GetDocumentMessage(); doc != nil {
						body = doc.GetCaption()
						msgType = "document"
						hasMedia = true
					}

					saveMessage(session, msgID, chatID, senderJID, pushName, fromMe, ts, body, msgType, hasMedia)
					count++
				}
			}
			log.Printf("%s: HistorySync archived %d past messages into database", session, count)

		case *events.QR:
			qrMu.Lock()
			qrCodes[session] = append(qrCodes[session], v.Codes...)
			ch := qrWaiting[session]
			qrMu.Unlock()
			log.Printf("%s: %d QR segments", session, len(v.Codes))
			if ch != nil {
				close(ch)
			}

		case *events.PairSuccess:
			qrMu.Lock()
			delete(qrCodes, session)
			delete(qrActive, session)
			delete(qrWaiting, session)
			qrMu.Unlock()
			log.Printf("%s: PAIRED as %s / LID=%s", session, v.ID.String(), v.LID.String())

		case *events.Connected:
			log.Printf("%s: connected", session)

		case *events.Disconnected:
			log.Printf("%s: disconnected", session)
			go func() {
				time.Sleep(5 * time.Second)
				if c, ok := clients[session]; ok {
					c.Connect()
				}
			}()

		case *events.LoggedOut:
			log.Printf("%s: LOGGED OUT", session)
		}
	}
}

func forwardWebhook(session string, payload map[string]interface{}) {
	body := map[string]interface{}{
		"event":   "message",
		"session": session,
		"payload": payload,
	}
	data, _ := json.Marshal(body)
	resp, err := http.Post(*webhookURL, "application/json", bytes.NewReader(data))
	if err != nil {
		log.Printf("%s: webhook fail: %v", session, err)
		return
	}
	resp.Body.Close()
}

// sendWithHumanPresence strictly enforces realistic human typing speed (30 WPM to 50 WPM, i.e., 240ms-400ms per character),
// read receipts, and continuous interactive typing presence refreshment until the message is completely typed and sent.
func sendWithHumanPresence(ctx context.Context, cli *whatsmeow.Client, jid types.JID, msg *proto.Message, textLen int, replyMsgID ...types.MessageID) (whatsmeow.SendResponse, error) {
	// 1. Subscribe & Online Presence (Must subscribe to peer presence so WhatsApp server broadcasts typing status)
	_ = cli.SubscribePresence(ctx, jid)
	_ = cli.SendPresence(ctx, types.PresenceAvailable)
	time.Sleep(time.Duration(300+rand.Intn(400)) * time.Millisecond)

	// 2. Read Receipt (Mark message as read if reply ID provided or active chat)
	if len(replyMsgID) > 0 && replyMsgID[0] != "" {
		_ = cli.MarkRead(ctx, replyMsgID, time.Now(), jid, jid)
		time.Sleep(time.Duration(400+rand.Intn(500)) * time.Millisecond)
	}

	// 3. Calculate Realistic Human Typing Duration based on 30 WPM - 50 WPM:
	// 50 WPM = 240ms/char (fastest human speed)
	// 30 WPM = 400ms/char (relaxed human speed)
	charMs := 240 + rand.Intn(120)
	totalTypingMs := textLen * charMs
	if totalTypingMs < 1500 { // minimum typing duration for very short text/media (1.5s - 2.2s)
		totalTypingMs = 1500 + rand.Intn(700)
	}

	// 4. Interactive Typing Simulation: Keep "composing" status alive on WhatsApp servers
	// Refresh ChatPresenceComposing every 1.5s so typing bubble never disappears/expires on recipient screen
	log.Printf("TYPING SIMULATION: peer=%s %d chars @ ~%dms/char -> total duration %dms", jid.String(), textLen, charMs, totalTypingMs)
	
	elapsed := 0
	for elapsed < totalTypingMs {
		if err := cli.SendChatPresence(ctx, jid, types.ChatPresenceComposing, types.ChatPresenceMediaText); err != nil {
			log.Printf("SendChatPresence error for %s: %v", jid.String(), err)
		}
		
		// Refresh every 1.5s (1500ms) to ensure continuous "typing..." bubble on receiver screen
		chunk := 1500
		if totalTypingMs-elapsed < chunk {
			chunk = totalTypingMs - elapsed
		}
		time.Sleep(time.Duration(chunk) * time.Millisecond)
		elapsed += chunk
	}

	// 5. Brief pause before hit send (200ms-400ms) + Clear Composing State
	_ = cli.SendChatPresence(ctx, jid, types.ChatPresencePaused, types.ChatPresenceMediaText)
	time.Sleep(time.Duration(200+rand.Intn(200)) * time.Millisecond)

	// 6. Send the actual message
	return cli.SendMessage(ctx, jid, msg)
}

func handleSend(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "POST only", 405)
		return
	}
	var req struct {
		Session    string `json:"session"`
		ChatID     string `json:"chatId"`
		Text       string `json:"text"`
		ReplyMsgID string `json:"replyMsgId,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	cli, ok := clients[req.Session]
	if !ok {
		http.Error(w, `{"error":"session not found"}`, 404)
		return
	}
	if !cli.IsConnected() {
		http.Error(w, `{"error":"not connected"}`, 503)
		return
	}

	jid, err := resolveJID(cli, req.ChatID)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"resolve: %s"}`, err.Error()), 400)
		return
	}

	msg := &proto.Message{Conversation: &req.Text}
	var replyIDs []types.MessageID
	if req.ReplyMsgID != "" {
		replyIDs = append(replyIDs, types.MessageID(req.ReplyMsgID))
	}
	resp, err := sendWithHumanPresence(context.Background(), cli, jid, msg, len(req.Text), replyIDs...)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"send: %s"}`, err.Error()), 500)
		return
	}
	ownJID := ""
	if cli.Store.ID != nil {
		ownJID = cli.Store.ID.User
	}
	saveMessage(req.Session, resp.ID, jid.String(), ownJID, "Me", true, resp.Timestamp.Unix(), req.Text, "text", false)

	json.NewEncoder(w).Encode(map[string]interface{}{
		"id":        resp.ID,
		"timestamp": resp.Timestamp.Unix(),
	})
}

// handleTyping sends a chat-presence indicator (composing/paused) so replies
// look human. state="composing" shows the "typing…" bubble to the recipient;
// state="paused" clears it. Best-effort: presence failures never block sends.
func handleTyping(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "POST only", 405)
		return
	}
	var req struct {
		Session string `json:"session"`
		ChatID  string `json:"chatId"`
		State   string `json:"state"` // "composing" (default) or "paused"
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	cli, ok := clients[req.Session]
	if !ok {
		http.Error(w, `{"error":"session not found"}`, 404)
		return
	}
	if !cli.IsConnected() {
		http.Error(w, `{"error":"not connected"}`, 503)
		return
	}
	jid, err := resolveJID(cli, req.ChatID)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"resolve: %s"}`, err.Error()), 400)
		return
	}
	state := types.ChatPresenceComposing
	if req.State == "paused" {
		state = types.ChatPresencePaused
	}
	// Presence requires subscribing to the contact first (best-effort).
	_ = cli.SendPresence(context.Background(), types.PresenceAvailable)
	if err := cli.SendChatPresence(context.Background(), jid, state, types.ChatPresenceMediaText); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"presence: %s"}`, err.Error()), 500)
		return
	}
	json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok", "state": string(state)})
}

func handleSendMedia(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "POST only", 405)
		return
	}
	var req struct {
		Session  string `json:"session"`
		ChatID   string `json:"chatId"`
		FilePath string `json:"filePath,omitempty"`
		Base64   string `json:"base64,omitempty"`
		Caption  string `json:"caption,omitempty"`
		Mimetype string `json:"mimetype,omitempty"`
		FileName string `json:"fileName,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	cli, ok := clients[req.Session]
	if !ok {
		http.Error(w, `{"error":"session not found"}`, 404)
		return
	}
	if !cli.IsConnected() {
		http.Error(w, `{"error":"not connected"}`, 503)
		return
	}

	jid, err := resolveJID(cli, req.ChatID)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"resolve: %s"}`, err.Error()), 400)
		return
	}

	var data []byte
	if req.FilePath != "" {
		data, err = os.ReadFile(req.FilePath)
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"read file: %s"}`, err.Error()), 400)
			return
		}
	} else if req.Base64 != "" {
		// handle optional "data:image/png;base64," prefix
		b64 := req.Base64
		if idx := strings.Index(b64, "base64,"); idx != -1 {
			b64 = b64[idx+7:]
		}
		data, err = base64.StdEncoding.DecodeString(b64)
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"decode base64: %s"}`, err.Error()), 400)
			return
		}
	} else {
		http.Error(w, `{"error":"must provide filePath or base64"}`, 400)
		return
	}

	mime := req.Mimetype
	if mime == "" {
		mime = http.DetectContentType(data)
	}

	ctx := context.Background()
	var msg *proto.Message

	switch {
	case strings.HasPrefix(mime, "image/"):
		resp, err := cli.Upload(ctx, data, whatsmeow.MediaImage)
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"upload: %s"}`, err.Error()), 500)
			return
		}
		msg = &proto.Message{
			ImageMessage: &proto.ImageMessage{
				Caption:       &req.Caption,
				Mimetype:      &mime,
				URL:           &resp.URL,
				DirectPath:    &resp.DirectPath,
				MediaKey:      resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    &resp.FileLength,
			},
		}
	case strings.HasPrefix(mime, "video/"):
		resp, err := cli.Upload(ctx, data, whatsmeow.MediaVideo)
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"upload: %s"}`, err.Error()), 500)
			return
		}
		msg = &proto.Message{
			VideoMessage: &proto.VideoMessage{
				Caption:       &req.Caption,
				Mimetype:      &mime,
				URL:           &resp.URL,
				DirectPath:    &resp.DirectPath,
				MediaKey:      resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    &resp.FileLength,
			},
		}
	case strings.HasPrefix(mime, "audio/"):
		resp, err := cli.Upload(ctx, data, whatsmeow.MediaAudio)
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"upload: %s"}`, err.Error()), 500)
			return
		}
		msg = &proto.Message{
			AudioMessage: &proto.AudioMessage{
				Mimetype:      &mime,
				URL:           &resp.URL,
				DirectPath:    &resp.DirectPath,
				MediaKey:      resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    &resp.FileLength,
			},
		}
	default:
		// Document fallback
		resp, err := cli.Upload(ctx, data, whatsmeow.MediaDocument)
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"upload: %s"}`, err.Error()), 500)
			return
		}
		fname := req.FileName
		if fname == "" {
			fname = "document"
		}
		msg = &proto.Message{
			DocumentMessage: &proto.DocumentMessage{
				Caption:       &req.Caption,
				Mimetype:      &mime,
				URL:           &resp.URL,
				DirectPath:    &resp.DirectPath,
				MediaKey:      resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    &resp.FileLength,
				FileName:      &fname,
			},
		}
	}

	sendResp, err := sendWithHumanPresence(ctx, cli, jid, msg, len(req.Caption))
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"send: %s"}`, err.Error()), 500)
		return
	}

	ownJID := ""
	if cli.Store.ID != nil {
		ownJID = cli.Store.ID.User
	}
	extraMap := map[string]interface{}{
		"fileName": req.FileName,
		"fileSize": int64(len(data)),
	}
	if req.FilePath != "" {
		extraMap["localPath"] = req.FilePath
	}
	saveMessage(req.Session, sendResp.ID, jid.String(), ownJID, "Me", true, sendResp.Timestamp.Unix(), req.Caption, mime, true, extraMap)

	json.NewEncoder(w).Encode(map[string]interface{}{
		"id":        sendResp.ID,
		"timestamp": sendResp.Timestamp.Unix(),
	})
}

func handleEdit(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "POST only", 405)
		return
	}
	var req struct {
		Session   string `json:"session"`
		ChatID    string `json:"chatId"`
		MessageID string `json:"messageId"` // ID pesan lama
		Text      string `json:"text"`      // Text baru
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	cli, ok := clients[req.Session]
	if !ok || !cli.IsConnected() {
		http.Error(w, `{"error":"session not found or disconnected"}`, 400)
		return
	}

	jid, err := resolveJID(cli, req.ChatID)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"resolve: %s"}`, err.Error()), 400)
		return
	}

	newMsg := &proto.Message{Conversation: &req.Text}
	editMsg := cli.BuildEdit(jid, types.MessageID(req.MessageID), newMsg)

	resp, err := sendWithHumanPresence(context.Background(), cli, jid, editMsg, len(req.Text))
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"edit: %s"}`, err.Error()), 500)
		return
	}
	json.NewEncoder(w).Encode(map[string]interface{}{
		"id":        resp.ID,
		"timestamp": resp.Timestamp.Unix(),
	})
}

func handleRevoke(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "POST only", 405)
		return
	}
	var req struct {
		Session   string `json:"session"`
		ChatID    string `json:"chatId"`
		MessageID string `json:"messageId"` // ID pesan yang mau dihapus
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	cli, ok := clients[req.Session]
	if !ok || !cli.IsConnected() {
		http.Error(w, `{"error":"session not found or disconnected"}`, 400)
		return
	}

	jid, err := resolveJID(cli, req.ChatID)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"resolve: %s"}`, err.Error()), 400)
		return
	}

	// For group chats, if revoking another user's message (as admin), sender is required
	// But since this is a simple bot interface, we assume revoking our own messages for now
	revokeMsg := cli.BuildRevoke(jid, types.EmptyJID, types.MessageID(req.MessageID))

	resp, err := sendWithHumanPresence(context.Background(), cli, jid, revokeMsg, 0)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"revoke: %s"}`, err.Error()), 500)
		return
	}
	json.NewEncoder(w).Encode(map[string]interface{}{
		"id":        resp.ID,
		"timestamp": resp.Timestamp.Unix(),
	})
}

func isAllDigits(s string) bool {
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return len(s) > 0
}

func resolveJID(cli *whatsmeow.Client, target string) (types.JID, error) {
	// Try phone number + @s.whatsapp.net
	if isAllDigits(target) {
		if jid, err := types.ParseJID(target + "@s.whatsapp.net"); err == nil {
			return jid, nil
		}
	}
	// Try full JID (xxx@s.whatsapp.net or xxx@g.us)
	if jid, err := types.ParseJID(target); err == nil && (jid.Server == "s.whatsapp.net" || jid.Server == "g.us") {
		return jid, nil
	}
	// Try name lookup
	targetLower := strings.ToLower(target)
	contacts, err := cli.Store.Contacts.GetAllContacts(context.Background())
	if err != nil {
		return types.JID{}, err
	}
	var matches []types.JID
	for jid, info := range contacts {
		name := strings.ToLower(info.FullName)
		if name == "" {
			name = strings.ToLower(info.PushName)
		}
		if strings.Contains(name, targetLower) {
			matches = append(matches, jid)
		}
	}
	if len(matches) == 1 {
		return matches[0], nil
	}
	if len(matches) > 1 {
		return types.JID{}, fmt.Errorf("nama '%s' cocok dengan %d kontak: %s. Coba lebih spesifik", target, len(matches), joinNames(cli, matches))
	}
	return types.JID{}, fmt.Errorf("kontak '%s' tidak ditemukan di %s", target, cli.Store.ID.User)
}

func joinNames(cli *whatsmeow.Client, jids []types.JID) string {
	names := []string{}
	for _, jid := range jids {
		info, _ := cli.Store.Contacts.GetContact(context.Background(), jid)
		name := info.FullName
		if name == "" {
			name = info.PushName
		}
		if name == "" {
			name = jid.User
		}
		names = append(names, name)
	}
	return strings.Join(names, ", ")
}

func startQRGeneration(session string, cli *whatsmeow.Client) error {
	qrMu.Lock()
	if qrActive[session] {
		qrMu.Unlock()
		return nil
	}
	qrActive[session] = true
	qrCodes[session] = nil
	ch := make(chan struct{})
	qrWaiting[session] = ch
	qrMu.Unlock()

	cli.Disconnect()
	_, err := cli.GetQRChannel(context.Background())
	if err != nil {
		qrMu.Lock()
		delete(qrActive, session)
		delete(qrWaiting, session)
		qrMu.Unlock()
		return err
	}
	if err := cli.Connect(); err != nil {
		qrMu.Lock()
		delete(qrActive, session)
		delete(qrWaiting, session)
		qrMu.Unlock()
		return err
	}

	select {
	case <-ch:
		return nil
	case <-time.After(30 * time.Second):
		qrMu.Lock()
		delete(qrActive, session)
		delete(qrWaiting, session)
		qrMu.Unlock()
		return fmt.Errorf("QR generation timeout")
	}
}

func getNextQR(session string, cli *whatsmeow.Client) string {
	if err := startQRGeneration(session, cli); err != nil {
		return ""
	}
	qrMu.Lock()
	codes := qrCodes[session]
	var code string
	if len(codes) > 0 {
		// Return the latest active QR code WITHOUT popping it, so it remains stable for 20-30s scanning
		code = codes[len(codes)-1]
	}
	qrMu.Unlock()
	return code
}

func handleQR(w http.ResponseWriter, r *http.Request) {
	session := r.URL.Path[len("/qr/"):]
	if session == "" {
		http.Error(w, `{"error":"specify session: /qr/wa1"}`, 400)
		return
	}
	cli, ok := clients[session]
	if !ok {
		http.Error(w, `{"error":"session not found"}`, 404)
		return
	}
	hasJID := cli.Store.ID != nil && cli.Store.ID.User != ""
	if hasJID && cli.IsConnected() {
		json.NewEncoder(w).Encode(map[string]interface{}{"status": "connected"})
		return
	}

	code := getNextQR(session, cli)
	if code == "" {
		if hasJID && cli.IsConnected() {
			json.NewEncoder(w).Encode(map[string]interface{}{"status": "connected"})
		} else {
			json.NewEncoder(w).Encode(map[string]interface{}{"status": "waiting"})
		}
		return
	}
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "scan_qr_code",
		"qr":     code,
	})
}

// handleReconnect forces QR re-generation by disconnecting and reconnecting the session.
// POST /reconnect/<session> -> {"status": "reconnecting"} or {"status": "connected"}
func handleReconnect(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "POST only", 405)
		return
	}
	session := r.URL.Path[len("/reconnect/"):]
	if session == "" {
		http.Error(w, `{"error":"specify session: /reconnect/wa1"}`, 400)
		return
	}
	cli, ok := clients[session]
	if !ok {
		http.Error(w, `{"error":"session not found"}`, 404)
		return
	}
	hasJID := cli.Store.ID != nil && cli.Store.ID.User != ""
	if cli.IsConnected() && hasJID {
		json.NewEncoder(w).Encode(map[string]interface{}{"status": "connected"})
		return
	}
	// If QR generation is already active, don't restart it
	qrMu.Lock()
	alreadyActive := qrActive[session]
	qrMu.Unlock()
	if alreadyActive {
		json.NewEncoder(w).Encode(map[string]interface{}{"status": "qr_active"})
		return
	}
	err := startQRGeneration(session, cli)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"reconnect: %s"}`, err.Error()), 500)
		return
	}
	json.NewEncoder(w).Encode(map[string]interface{}{"status": "reconnecting"})
}

// handlePNGQR returns QR code as PNG directly — no Python, single request.
// GET /png-qr/<session> -> image/png
func handlePNGQR(w http.ResponseWriter, r *http.Request) {
	session := r.URL.Path[len("/png-qr/"):]
	if session == "" {
		http.Error(w, "use /png-qr/wa1", 400)
		return
	}
	cli, ok := clients[session]
	if !ok {
		http.Error(w, "session not found", 404)
		return
	}
	hasJID := cli.Store.ID != nil && cli.Store.ID.User != ""
	if hasJID && cli.IsConnected() {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"status": "connected"})
		return
	}

	code := getNextQR(session, cli)
	if code == "" {
		if hasJID && cli.IsConnected() {
			json.NewEncoder(w).Encode(map[string]interface{}{"status": "connected"})
		} else {
			http.Error(w, "QR not ready", 503)
		}
		return
	}

	img, err := qrcode.Encode(code, qrcode.Medium, 400)
	if err != nil {
		http.Error(w, fmt.Sprintf("QR encode: %v", err), 500)
		return
	}
	w.Header().Set("Content-Type", "image/png")
	w.Write(img)
}

func handleSessions(w http.ResponseWriter, r *http.Request) {
	result := []map[string]interface{}{}
	for name, cli := range clients {
		hasJID := cli.Store.ID != nil && cli.Store.ID.User != ""
		status := "disconnected"
		if cli.IsConnected() && hasJID {
			status = "working"
		} else if !hasJID {
			status = "scan_qr_code"
		}
		jid := ""
		if hasJID {
			jid = cli.Store.ID.User
		}
		result = append(result, map[string]interface{}{
			"name":   name,
			"status": status,
			"jid":    jid,
		})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func handleContactLookup(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/contacts/")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) != 2 {
		http.Error(w, `{"error":"use /contacts/<session>/<name>"}`, 400)
		return
	}
	session, query := parts[0], strings.ToLower(parts[1])

	cli, ok := clients[session]
	if !ok {
		http.Error(w, `{"error":"session not found"}`, 404)
		return
	}

	contacts, err := cli.Store.Contacts.GetAllContacts(context.Background())
	if err != nil {
		http.Error(w, `{"error":"failed"}`, 500)
		return
	}

	type match struct {
		ID       string `json:"id"`
		Name     string `json:"name"`
		PushName string `json:"pushName,omitempty"`
		FullName string `json:"fullName,omitempty"`
	}
	var results []match
	for jid, info := range contacts {
		name := info.FullName
		if name == "" {
			name = info.PushName
		}
		if name == "" {
			continue
		}
		if strings.Contains(strings.ToLower(name), query) {
			results = append(results, match{
				ID:       jid.String(),
				Name:     name,
				PushName: info.PushName,
				FullName: info.FullName,
			})
		}
	}

	sort.Slice(results, func(i, j int) bool {
		ai := strings.ToLower(results[i].Name) == query
		bj := strings.ToLower(results[j].Name) == query
		if ai != bj {
			return ai
		}
		return len(results[i].Name) < len(results[j].Name)
	})

	if len(results) > 15 {
		results = results[:15]
	}

	json.NewEncoder(w).Encode(results)
}

func handleChats(w http.ResponseWriter, r *http.Request) {
	session := r.URL.Path[len("/chats/"):]
	if session == "" {
		http.Error(w, `{"error":"specify session"}`, 400)
		return
	}
	cli, ok := clients[session]
	if !ok {
		http.Error(w, `{"error":"session not found"}`, 404)
		return
	}
	contacts, err := cli.Store.Contacts.GetAllContacts(context.Background())
	if err != nil {
		http.Error(w, `[]`, 200)
		return
	}
	result := []map[string]interface{}{}
	for jid, info := range contacts {
		name := jid.User
		if info.FullName != "" {
			name = info.FullName
		}
		result = append(result, map[string]interface{}{
			"id":          jid.String(),
			"displayName": name,
		})
	}
	json.NewEncoder(w).Encode(result)
}

func handlePing(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "POST only", 405)
		return
	}
	log.Println("PING: human-simulated heartbeat starting for all connected sessions...")

	ts := time.Now().Format("15:04")
	results := []string{}

	// Natural, non-static message templates to avoid pattern detection
	templates := []string{
		"ping %s [%s] ok",
		"%s online status: active (%s)",
		"heartbeat check %s @ %s - clear",
		"%s status check %s: operational",
		"session %s alive [%s]",
	}

	// Ordered list for human-like sequential check-in with typing simulation & random delays
	sessionOrder := []string{"wa1", "wa2", "wa3"}
	activeCount := 0

	for idx, name := range sessionOrder {
		cli, ok := clients[name]
		hasJID := ok && cli.Store.ID != nil && cli.Store.ID.User != ""

		if !ok || !cli.IsConnected() || !hasJID {
			results = append(results, fmt.Sprintf("%s: FAIL (not connected/paired)", name))
			log.Printf("PING [%s]: SKIPPED (not connected)", name)
			continue
		}

		activeCount++

		// Select dynamic non-static message template
		ctx := context.Background()
		template := templates[rand.Intn(len(templates))]
		text := fmt.Sprintf(template, name, ts)
		msg := &proto.Message{Conversation: &text}

		// Send message using mandatory 4-step human presence pipeline
		_, err := sendWithHumanPresence(ctx, cli, pingGroup, msg, len(text))
		if err != nil {
			results = append(results, fmt.Sprintf("%s: FAIL (%s)", name, err.Error()))
			log.Printf("PING [%s]: FAIL (%v)", name, err)
		} else {
			results = append(results, fmt.Sprintf("%s: OK", name))
			log.Printf("PING [%s]: OK", name)
		}

		// 7. Random inter-session delay (2s to 4.5s) so messages are not burst-fired simultaneously
		if idx < len(sessionOrder)-1 {
			interDelayMs := 2000 + rand.Intn(2500)
			time.Sleep(time.Duration(interDelayMs) * time.Millisecond)
		}
	}

	log.Printf("PING FINISHED: %s", strings.Join(results, ", "))
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":      "ok",
		"sent":        ts,
		"activeCount": activeCount,
		"results":     results,
	})
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	status := "ok"
	for name, cli := range clients {
		if !cli.IsConnected() {
			status = "degraded"
			log.Printf("health: %s disconnected", name)
		}
	}
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    status,
		"uptime":    time.Since(startedAt).String(),
		"sessions":  len(clients),
		"connected": countConnected(),
	})
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := []string{}
	for _, p := range bytes.Split([]byte(s), []byte(",")) {
		parts = append(parts, string(bytes.TrimSpace(p)))
	}
	return parts
}

func handleHistoryMessages(w http.ResponseWriter, r *http.Request) {
	session := r.URL.Query().Get("session")
	chatID := r.URL.Query().Get("chatId")
	if session == "" || chatID == "" {
		http.Error(w, `{"error":"must specify session and chatId"}`, 400)
		return
	}
	limit := 50
	if l := r.URL.Query().Get("limit"); l != "" {
		fmt.Sscanf(l, "%d", &limit)
	}
	offset := 0
	if o := r.URL.Query().Get("offset"); o != "" {
		fmt.Sscanf(o, "%d", &offset)
	}

	rows, err := msgDB.Query(`
		SELECT id, session, chat_id, sender, sender_name, from_me, timestamp, body, msg_type, has_media,
		       COALESCE(file_name,''), COALESCE(file_size,0), COALESCE(local_path,'')
		FROM messages
		WHERE session=? AND chat_id=?
		ORDER BY timestamp DESC
		LIMIT ? OFFSET ?
	`, session, chatID, limit, offset)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"query: %v"}`, err), 500)
		return
	}
	defer rows.Close()

	var msgs []map[string]interface{}
	for rows.Next() {
		var id, sess, cID, sender, senderName, body, msgType, fileName, localPath string
		var fromMe, timestamp, hasMedia int
		var fileSize int64
		if err := rows.Scan(&id, &sess, &cID, &sender, &senderName, &fromMe, &timestamp, &body, &msgType, &hasMedia, &fileName, &fileSize, &localPath); err != nil {
			continue
		}
		row := map[string]interface{}{
			"id":         id,
			"session":    sess,
			"chatId":     cID,
			"sender":     sender,
			"senderName": senderName,
			"fromMe":     fromMe == 1,
			"timestamp":  timestamp,
			"body":       body,
			"type":       msgType,
			"hasMedia":   hasMedia == 1,
		}
		if fileName != "" {
			row["fileName"] = fileName
		}
		if fileSize > 0 {
			row["fileSize"] = fileSize
		}
		if localPath != "" {
			row["localPath"] = localPath
		}
		msgs = append(msgs, row)
	}
	if msgs == nil {
		msgs = []map[string]interface{}{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(msgs)
}

func handleHistoryChats(w http.ResponseWriter, r *http.Request) {
	session := r.URL.Query().Get("session")
	if session == "" {
		http.Error(w, `{"error":"must specify session"}`, 400)
		return
	}

	rows, err := msgDB.Query(`
		SELECT chat_id, MAX(sender_name), MAX(timestamp), COUNT(*), MAX(body)
		FROM messages
		WHERE session=?
		GROUP BY chat_id
		ORDER BY MAX(timestamp) DESC
	`, session)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"query: %v"}`, err), 500)
		return
	}
	defer rows.Close()

	var chats []map[string]interface{}
	for rows.Next() {
		var chatID, senderName, lastBody string
		var maxTs, count int
		if err := rows.Scan(&chatID, &senderName, &maxTs, &count, &lastBody); err != nil {
			continue
		}
		chats = append(chats, map[string]interface{}{
			"chatId":      chatID,
			"displayName": senderName,
			"lastMsg":     lastBody,
			"lastTime":    maxTs,
			"msgCount":    count,
		})
	}
	if chats == nil {
		chats = []map[string]interface{}{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(chats)
}

func handleHistorySearch(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	session := r.URL.Query().Get("session")
	if q == "" {
		http.Error(w, `{"error":"must specify search query q"}`, 400)
		return
	}
	limit := 50
	if l := r.URL.Query().Get("limit"); l != "" {
		fmt.Sscanf(l, "%d", &limit)
	}

	likePattern := "%" + q + "%"
	querySQL := `
		SELECT id, session, chat_id, sender, sender_name, from_me, timestamp, body, msg_type, has_media,
		       COALESCE(file_name,''), COALESCE(file_size,0), COALESCE(local_path,'')
		FROM messages
		WHERE (body LIKE ? OR sender_name LIKE ? OR sender LIKE ? OR chat_id LIKE ?)
	`
	args := []interface{}{likePattern, likePattern, likePattern, likePattern}
	if session != "" {
		querySQL += " AND session=? "
		args = append(args, session)
	}
	querySQL += " ORDER BY timestamp DESC LIMIT ? "
	args = append(args, limit)

	rows, err := msgDB.Query(querySQL, args...)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"search query: %v"}`, err), 500)
		return
	}
	defer rows.Close()

	var msgs []map[string]interface{}
	for rows.Next() {
		var id, sess, cID, sender, senderName, body, msgType, fileName, localPath string
		var fromMe, timestamp, hasMedia int
		var fileSize int64
		if err := rows.Scan(&id, &sess, &cID, &sender, &senderName, &fromMe, &timestamp, &body, &msgType, &hasMedia, &fileName, &fileSize, &localPath); err != nil {
			continue
		}
		row := map[string]interface{}{
			"id":         id,
			"session":    sess,
			"chatId":     cID,
			"sender":     sender,
			"senderName": senderName,
			"fromMe":     fromMe == 1,
			"timestamp":  timestamp,
			"body":       body,
			"type":       msgType,
			"hasMedia":   hasMedia == 1,
		}
		if fileName != "" {
			row["fileName"] = fileName
		}
		if fileSize > 0 {
			row["fileSize"] = fileSize
		}
		if localPath != "" {
			row["localPath"] = localPath
		}
		msgs = append(msgs, row)
	}
	if msgs == nil {
		msgs = []map[string]interface{}{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(msgs)
}

func countConnected() int {
	n := 0
	for _, cli := range clients {
		if cli.IsConnected() {
			n++
		}
	}
	return n
}
