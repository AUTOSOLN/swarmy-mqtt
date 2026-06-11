package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/gorilla/mux"

	"github.com/AUTOSOLN/swarmy-mqtt/ui"

	"github.com/AUTOSOLN/swarmy-mqtt/msgstore"
	"github.com/AUTOSOLN/swarmy-mqtt/persist"
)

const (
	urlConnectionPrefix  = "/api/connection"
	urlMqttConnect       = "/api/mqtt/connect"
	urlMqttDisconnect    = "/api/mqtt/disconnect"
	urlMqttConnectOne    = "/api/mqtt/connect/{id}"
	urlMqttDisconnectOne = "/api/mqtt/disconnect/{id}"
	urlMqttEvents        = "/api/mqtt/events"
	urlMqttPublish       = "/api/mqtt/publish/{id}"
	urlMessages          = "/api/messages"

	headerContentType = "Content-Type"
	contentTypeJSON   = "application/json"
)

// --- Data types ---

type TopicQosItem struct {
	Topic string `json:"topic"`
	QoS   uint16 `json:"qos"`
}

type ConnectionItem struct {
	ID                    string         `json:"id"`
	Name                  string         `json:"name"`
	MqttServerUrl         string         `json:"mqttserverurl"`
	MqttKeepAlive         uint16         `json:"keepalive"`
	CleanSession          bool           `json:"cleansession"`
	SessionExpiryInterval uint32         `json:"sessionexpiryinterval"`
	ClientId              string         `json:"clientid"`
	Username              string         `json:"username"`
	Password              string         `json:"password"`
	WillTopic             string         `json:"willtopic"`
	WillPayload           string         `json:"willpayload"`
	WillQoS               byte           `json:"willqos"`
	WillRetain            bool           `json:"willretain"`
	Subscriptions         []TopicQosItem `json:"subs"`
}

// --- SSE ---

type SSEEvent struct {
	Type         string `json:"type"`
	ConnectionID string `json:"connectionId,omitempty"`
	Name         string `json:"name,omitempty"`
	Status       string `json:"status,omitempty"`
	Error        string `json:"error,omitempty"`
	Topic        string `json:"topic,omitempty"`
	Payload      string `json:"payload,omitempty"`
	PayloadBytes []byte `json:"payloadBytes,omitempty"`
	QoS          byte   `json:"qos"`
	Retained     bool   `json:"retained,omitempty"`
	IsSparkplug  bool   `json:"isSparkplug,omitempty"`
	Timestamp    string `json:"timestamp"`
}

type sseBroker struct {
	mu      sync.Mutex
	clients map[chan SSEEvent]struct{}
}

func newSSEBroker() *sseBroker {
	return &sseBroker{clients: make(map[chan SSEEvent]struct{})}
}

func (b *sseBroker) subscribe() chan SSEEvent {
	ch := make(chan SSEEvent, 64)
	b.mu.Lock()
	b.clients[ch] = struct{}{}
	b.mu.Unlock()
	return ch
}

func (b *sseBroker) unsubscribe(ch chan SSEEvent) {
	b.mu.Lock()
	delete(b.clients, ch)
	close(ch)
	b.mu.Unlock()
}

func (b *sseBroker) publish(event SSEEvent) {
	event.Timestamp = time.Now().UTC().Format(time.RFC3339Nano)
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.clients {
		select {
		case ch <- event:
		default: // drop for slow clients
		}
	}
}

// --- MQTT state ---

type mqttConn struct {
	manager *autopaho.ConnectionManager
	cancel  context.CancelFunc
}

// --- Global state ---

var (
	events = newSSEBroker()

	connectionItems   = make(map[string]ConnectionItem)
	connectionItemsMu sync.Mutex

	activeConns   = make(map[string]*mqttConn)
	activeConnsMu sync.Mutex

	store *persist.Store[[]ConnectionItem]
	msgDB *msgstore.Store
)

func generateID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// --- Main ---

func main() {
	port := flag.Int("port", 6080, "HTTP port to listen on")
	stateFile := flag.String("state", "", "path to JSON file for persisting connection state (disabled when empty)")
	dbFile := flag.String("db", "", "path to SQLite database for message persistence (disabled when empty)")
	flag.Parse()

	if *dbFile != "" {
		db, err := msgstore.Open(*dbFile)
		if err != nil {
			log.Fatalf("failed to open message database %q: %v", *dbFile, err)
		}
		msgDB = db
	}

	store = persist.New[[]ConnectionItem](*stateFile)
	if items, err := store.Load(); err != nil {
		log.Fatalf("failed to load state from %q: %v", *stateFile, err)
	} else {
		for _, item := range items {
			connectionItems[item.ID] = item
		}
	}

	r := mux.NewRouter()
	r.Use(corsMiddleware)

	r.HandleFunc(urlConnectionPrefix, getConnections).Methods(http.MethodGet)
	r.HandleFunc(urlConnectionPrefix, createConnection).Methods(http.MethodPost)
	r.HandleFunc(fmt.Sprintf("%s/{id}", urlConnectionPrefix), updateConnection).Methods(http.MethodPut)
	r.HandleFunc(fmt.Sprintf("%s/{id}", urlConnectionPrefix), deleteConnection).Methods(http.MethodDelete)
	r.HandleFunc(urlMqttConnect, mqttConnect).Methods(http.MethodPost)
	r.HandleFunc(urlMqttDisconnect, mqttDisconnect).Methods(http.MethodPost)
	r.HandleFunc(urlMqttConnectOne, mqttConnectOne).Methods(http.MethodPost)
	r.HandleFunc(urlMqttDisconnectOne, mqttDisconnectOne).Methods(http.MethodPost)
	r.HandleFunc(urlMqttPublish, mqttPublish).Methods(http.MethodPost)
	r.HandleFunc(urlMqttEvents, mqttSSEEvent)
	r.HandleFunc(urlMessages, getMessages).Methods(http.MethodGet)

	sub, _ := fs.Sub(ui.StaticFiles, "static")
	r.PathPrefix("/").HandlerFunc(spaHandler(http.FileServer(http.FS(sub))))

	addr := fmt.Sprintf(":%d", *port)
	fmt.Printf("swarmy-mqtt server starting on %s...\n", addr)
	log.Fatal(http.ListenAndServe(addr, r))
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", headerContentType)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func spaHandler(fileServer http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fileServer.ServeHTTP(w, r)
	}
}

// persistState snapshots connectionItems and saves it to disk. No-op when persistence is disabled.
func persistState() {
	connectionItemsMu.Lock()
	items := make([]ConnectionItem, 0, len(connectionItems))
	for _, item := range connectionItems {
		items = append(items, item)
	}
	connectionItemsMu.Unlock()
	if err := store.Save(items); err != nil {
		log.Printf("persist: %v", err)
	}
}

// --- Connection CRUD ---

func getConnections(w http.ResponseWriter, r *http.Request) {
	w.Header().Set(headerContentType, contentTypeJSON)
	connectionItemsMu.Lock()
	defer connectionItemsMu.Unlock()
	items := make([]ConnectionItem, 0, len(connectionItems))
	for _, item := range connectionItems {
		items = append(items, item)
	}
	json.NewEncoder(w).Encode(items)
}

func createConnection(w http.ResponseWriter, r *http.Request) {
	var item ConnectionItem
	if err := json.NewDecoder(r.Body).Decode(&item); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if item.ID == "" {
		item.ID = generateID()
	}
	connectionItemsMu.Lock()
	connectionItems[item.ID] = item
	connectionItemsMu.Unlock()
	w.Header().Set(headerContentType, contentTypeJSON)
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(item)
	persistState()
}

func updateConnection(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	var item ConnectionItem
	if err := json.NewDecoder(r.Body).Decode(&item); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	item.ID = id
	connectionItemsMu.Lock()
	_, exists := connectionItems[id]
	if exists {
		connectionItems[id] = item
	}
	connectionItemsMu.Unlock()
	if !exists {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
	persistState()
}

func deleteConnection(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	connectionItemsMu.Lock()
	_, exists := connectionItems[id]
	if exists {
		delete(connectionItems, id)
	}
	connectionItemsMu.Unlock()
	if !exists {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
	persistState()
}

// --- MQTT connect / disconnect ---

func mqttConnect(w http.ResponseWriter, r *http.Request) {
	connectionItemsMu.Lock()
	items := make([]ConnectionItem, 0, len(connectionItems))
	for _, item := range connectionItems {
		items = append(items, item)
	}
	connectionItemsMu.Unlock()

	activeConnsMu.Lock()
	defer activeConnsMu.Unlock()

	for _, item := range items {
		if _, exists := activeConns[item.ID]; !exists {
			connectOne(item)
		}
	}

	w.WriteHeader(http.StatusNoContent)
}

// connectOne starts an autopaho connection for item. Caller must hold activeConnsMu.
func connectOne(item ConnectionItem) {
	serverURL, err := url.Parse(item.MqttServerUrl)
	if err != nil {
		log.Printf("invalid URL for %q: %v", item.Name, err)
		events.publish(statusEvent(item.ID, item.Name, "error", fmt.Sprintf("invalid server URL: %v", err)))
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	cfg := buildConnConfig(item, serverURL, ctx)

	cm, err := autopaho.NewConnection(ctx, cfg)
	if err != nil {
		cancel()
		log.Printf("failed to create connection for %q: %v", item.Name, err)
		events.publish(statusEvent(item.ID, item.Name, "error", err.Error()))
		return
	}

	activeConns[item.ID] = &mqttConn{manager: cm, cancel: cancel}
	events.publish(statusEvent(item.ID, item.Name, "connecting", ""))
}

func buildConnConfig(item ConnectionItem, serverURL *url.URL, ctx context.Context) autopaho.ClientConfig {
	cfg := autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{serverURL},
		KeepAlive:                     item.MqttKeepAlive,
		CleanStartOnInitialConnection: item.CleanSession,
		SessionExpiryInterval:         item.SessionExpiryInterval,
		ConnectUsername:               item.Username,
		ConnectPassword:               []byte(item.Password),
		OnConnectionUp:                onConnectionUp(item, ctx),
		OnConnectError: func(err error) {
			events.publish(statusEvent(item.ID, item.Name, "error", err.Error()))
		},
		ClientConfig: paho.ClientConfig{
			ClientID:          item.ClientId,
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){onPublishReceived(item)},
			OnClientError: func(err error) {
				events.publish(statusEvent(item.ID, item.Name, "error", err.Error()))
			},
			OnServerDisconnect: func(d *paho.Disconnect) {
				var reason string
				if d.Properties != nil {
					reason = d.Properties.ReasonString
				}
				events.publish(statusEvent(item.ID, item.Name, "disconnected", reason))
			},
		},
	}
	if item.WillTopic != "" {
		cfg.WillMessage = &paho.WillMessage{
			Retain:  item.WillRetain,
			QoS:     item.WillQoS,
			Topic:   item.WillTopic,
			Payload: []byte(item.WillPayload),
		}
		cfg.WillProperties = &paho.WillProperties{}
	}
	return cfg
}

func onConnectionUp(item ConnectionItem, ctx context.Context) func(*autopaho.ConnectionManager, *paho.Connack) {
	return func(cm *autopaho.ConnectionManager, _ *paho.Connack) {
		events.publish(statusEvent(item.ID, item.Name, "connected", ""))
		if len(item.Subscriptions) == 0 {
			return
		}
		subs := make([]paho.SubscribeOptions, len(item.Subscriptions))
		for i, s := range item.Subscriptions {
			subs[i] = paho.SubscribeOptions{Topic: s.Topic, QoS: byte(s.QoS)}
		}
		subCtx, subCancel := context.WithTimeout(ctx, 5*time.Second)
		defer subCancel()
		if _, err := cm.Subscribe(subCtx, &paho.Subscribe{Subscriptions: subs}); err != nil {
			log.Printf("subscribe error for %q: %v", item.Name, err)
		}
	}
}

func isSparkplugTopic(topic string) bool {
	return strings.HasPrefix(topic, "spBv1.0/")
}

func onPublishReceived(item ConnectionItem) func(paho.PublishReceived) (bool, error) {
	return func(pr paho.PublishReceived) (bool, error) {
		isSP := isSparkplugTopic(pr.Packet.Topic)
		events.publish(SSEEvent{
			Type:         "message",
			ConnectionID: item.ID,
			Name:         item.Name,
			Topic:        pr.Packet.Topic,
			Payload:      string(pr.Packet.Payload),
			PayloadBytes: pr.Packet.Payload,
			QoS:          pr.Packet.QoS,
			Retained:     pr.Packet.Retain,
			IsSparkplug:  isSP,
		})
		if err := msgDB.Write(msgstore.Message{
			Timestamp:   time.Now().UTC(),
			ConnID:      item.ID,
			ConnName:    item.Name,
			Topic:       pr.Packet.Topic,
			Payload:     string(pr.Packet.Payload),
			QoS:         int(pr.Packet.QoS),
			Retained:    pr.Packet.Retain,
			IsSparkplug: isSP,
		}); err != nil {
			log.Printf("msgstore write: %v", err)
		}
		return true, nil
	}
}

// --- Message history API ---

type messageJSON struct {
	ID           int64  `json:"id"`
	Type         string `json:"type"`
	ConnectionID string `json:"connectionId"`
	Name         string `json:"name"`
	Topic        string `json:"topic"`
	Payload      string `json:"payload"`
	QoS          int    `json:"qos"`
	Retained     bool   `json:"retained,omitempty"`
	IsSparkplug  bool   `json:"isSparkplug,omitempty"`
	Timestamp    string `json:"timestamp"`
}

type messagesResponse struct {
	Total    int           `json:"total"`
	Limit    int           `json:"limit"`
	Offset   int           `json:"offset"`
	Messages []messageJSON `json:"messages"`
}

func getMessages(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := msgstore.Filter{
		Search: q.Get("q"),
		ConnID: q.Get("conn"),
	}
	if q.Get("retained") == "true" {
		b := true
		f.Retained = &b
	}
	if q.Get("sparkplug") == "true" {
		b := true
		f.Sparkplug = &b
	}
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			f.Limit = n
		}
	}
	if v := q.Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			f.Offset = n
		}
	}

	result, err := msgDB.Query(f)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	msgs := make([]messageJSON, len(result.Messages))
	for i, m := range result.Messages {
		msgs[i] = messageJSON{
			ID:           m.ID,
			Type:         "message",
			ConnectionID: m.ConnID,
			Name:         m.ConnName,
			Topic:        m.Topic,
			Payload:      m.Payload,
			QoS:          m.QoS,
			Retained:     m.Retained,
			IsSparkplug:  m.IsSparkplug,
			Timestamp:    m.Timestamp.UTC().Format(time.RFC3339Nano),
		}
	}

	w.Header().Set(headerContentType, contentTypeJSON)
	json.NewEncoder(w).Encode(messagesResponse{
		Total:    result.Total,
		Limit:    f.Limit,
		Offset:   f.Offset,
		Messages: msgs,
	})
}

func statusEvent(id, name, status, errMsg string) SSEEvent {
	return SSEEvent{
		Type:         "connection_status",
		ConnectionID: id,
		Name:         name,
		Status:       status,
		Error:        errMsg,
	}
}

func mqttDisconnect(w http.ResponseWriter, r *http.Request) {
	activeConnsMu.Lock()
	conns := make(map[string]*mqttConn, len(activeConns))
	for id, conn := range activeConns {
		conns[id] = conn
	}
	activeConns = make(map[string]*mqttConn)
	activeConnsMu.Unlock()

	for id, conn := range conns {
		connectionItemsMu.Lock()
		name := connectionItems[id].Name
		connectionItemsMu.Unlock()

		disconnCtx, disconnCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := conn.manager.Disconnect(disconnCtx); err != nil {
			log.Printf("disconnect error for %q: %v", name, err)
		}
		disconnCancel()
		conn.cancel()

		events.publish(SSEEvent{
			Type:         "connection_status",
			ConnectionID: id,
			Name:         name,
			Status:       "disconnected",
		})
	}

	w.WriteHeader(http.StatusNoContent)
}

func mqttConnectOne(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]

	connectionItemsMu.Lock()
	item, ok := connectionItems[id]
	connectionItemsMu.Unlock()
	if !ok {
		http.Error(w, "connection not found", http.StatusNotFound)
		return
	}

	activeConnsMu.Lock()
	defer activeConnsMu.Unlock()

	if _, exists := activeConns[id]; !exists {
		connectOne(item)
	}
	w.WriteHeader(http.StatusNoContent)
}

func mqttDisconnectOne(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]

	activeConnsMu.Lock()
	conn, exists := activeConns[id]
	if exists {
		delete(activeConns, id)
	}
	activeConnsMu.Unlock()

	if !exists {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	connectionItemsMu.Lock()
	name := connectionItems[id].Name
	connectionItemsMu.Unlock()

	disconnCtx, disconnCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := conn.manager.Disconnect(disconnCtx); err != nil {
		log.Printf("disconnect error for %q: %v", name, err)
	}
	disconnCancel()
	conn.cancel()

	events.publish(SSEEvent{
		Type:         "connection_status",
		ConnectionID: id,
		Name:         name,
		Status:       "disconnected",
	})
	w.WriteHeader(http.StatusNoContent)
}

// --- MQTT publish ---

func mqttPublish(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]

	var req struct {
		Topic   string `json:"topic"`
		Payload string `json:"payload"`
		QoS     byte   `json:"qos"`
		Retain  bool   `json:"retain"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Topic == "" {
		http.Error(w, "topic is required", http.StatusBadRequest)
		return
	}

	activeConnsMu.Lock()
	conn, exists := activeConns[id]
	activeConnsMu.Unlock()
	if !exists {
		http.Error(w, "connection not active", http.StatusNotFound)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	if _, err := conn.manager.Publish(ctx, &paho.Publish{
		QoS:     req.QoS,
		Topic:   req.Topic,
		Payload: []byte(req.Payload),
		Retain:  req.Retain,
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// --- SSE event stream ---

func mqttSSEEvent(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set(headerContentType, "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // disable nginx buffering

	// Initial comment establishes the stream
	fmt.Fprint(w, ": connected\n\n")
	flusher.Flush()

	ch := events.subscribe()
	defer events.unsubscribe(ch)

	ctx := r.Context()
	for {
		select {
		case event, ok := <-ch:
			if !ok {
				return
			}
			data, err := json.Marshal(event)
			if err != nil {
				continue
			}
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		case <-ctx.Done():
			return
		}
	}
}
