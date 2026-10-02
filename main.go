package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
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

	mqttclient "github.com/AUTOSOLN/mqtt-client"
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
	ProtocolVersion       byte           `json:"protocolversion"` // 4 = MQTT 3.1.1, 5 = MQTT 5 (default)
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

	// TLS applies to mqtts://, ssl:// and tls:// server URLs. Certificates
	// and the key are PEM text, persisted with the rest of the item.
	TLSCACert     string `json:"tlscacert"`     // trusted CA(s); empty = system roots
	TLSClientCert string `json:"tlsclientcert"` // client certificate for mutual TLS
	TLSClientKey  string `json:"tlsclientkey"`  // unencrypted private key for TLSClientCert
	TLSInsecure   bool   `json:"tlsinsecure"`   // skip server certificate verification
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
	client *mqttclient.Client
	cancel context.CancelFunc
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

// connectOne starts a connection for item that reconnects until it is
// disconnected or fails permanently. Caller must hold activeConnsMu.
func connectOne(item ConnectionItem) {
	client, err := newClient(item)
	if err != nil {
		events.publish(statusEvent(item.ID, item.Name, "error", fmt.Sprintf("invalid connection settings: %v", err)))
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	conn := &mqttConn{client: client, cancel: cancel}
	activeConns[item.ID] = conn
	events.publish(statusEvent(item.ID, item.Name, "connecting", ""))

	go func() {
		err := client.Run(ctx)
		cancel()
		// Run returns nil after Disconnect and ctx.Err() after cancel; any
		// other error is permanent (refused credentials, protocol error, bad
		// certificate) and Run has stopped retrying. OnConnectError or
		// OnDisconnect has already reported the error itself.
		if err != nil && !errors.Is(err, context.Canceled) {
			publishError(item.ID, item.Name, "stopped reconnecting: %v", err)
		}
		activeConnsMu.Lock()
		if activeConns[item.ID] == conn {
			delete(activeConns, item.ID)
		}
		activeConnsMu.Unlock()
	}()
}

func newClient(item ConnectionItem) (*mqttclient.Client, error) {
	opts, err := clientOptions(item)
	if err != nil {
		return nil, err
	}
	return mqttclient.New(opts, clientHandlers(item))
}

func clientOptions(item ConnectionItem) (mqttclient.Options, error) {
	opts := mqttclient.Options{
		Server:          item.MqttServerUrl,
		ProtocolVersion: protocolVersion(item),
		ClientID:        item.ClientId,
		// An empty client id makes the server assign one, so there is no
		// session to resume; the client requires clean start for it.
		CleanStart: item.CleanSession || item.ClientId == "",
		KeepAlive:  item.MqttKeepAlive,
		Username:   item.Username,
		Password:   []byte(item.Password),
	}
	// Session expiry is a CONNECT property, which MQTT 3.1.1 does not have.
	if opts.ProtocolVersion == mqttclient.MQTT5 && item.SessionExpiryInterval > 0 {
		opts.ConnectProperties = &mqttclient.Properties{
			SessionExpiryInterval:     item.SessionExpiryInterval,
			SessionExpiryIntervalFlag: true,
		}
	}
	if item.WillTopic != "" {
		opts.Will = &mqttclient.Message{
			Topic:   item.WillTopic,
			Payload: []byte(item.WillPayload),
			QoS:     item.WillQoS,
			Retain:  item.WillRetain,
		}
	}
	tlsCfg, err := tlsConfig(item)
	if err != nil {
		return opts, err
	}
	opts.TLSConfig = tlsCfg
	return opts, nil
}

// tlsConfig builds the TLS configuration for item, or returns nil for a
// plain TCP server URL.
func tlsConfig(item ConnectionItem) (*tls.Config, error) {
	hasTLSSettings := item.TLSCACert != "" || item.TLSClientCert != "" || item.TLSClientKey != "" || item.TLSInsecure
	u, err := url.Parse(item.MqttServerUrl)
	if err != nil {
		return nil, err // mqttclient.New reports it in more detail
	}
	switch strings.ToLower(u.Scheme) {
	case "mqtts", "ssl", "tls":
	default:
		if hasTLSSettings {
			return nil, fmt.Errorf("TLS settings need an mqtts:// server URL, not %s://", u.Scheme)
		}
		return nil, nil
	}

	cfg := &tls.Config{
		// The user chose to skip verification, e.g. for a self-signed test broker.
		InsecureSkipVerify: item.TLSInsecure, // #nosec G402
	}
	if item.TLSCACert != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(item.TLSCACert)) {
			return nil, errors.New("CA certificate: no PEM certificate found")
		}
		cfg.RootCAs = pool
	}
	if item.TLSClientCert != "" || item.TLSClientKey != "" {
		if item.TLSClientCert == "" || item.TLSClientKey == "" {
			return nil, errors.New("mutual TLS needs both a client certificate and its key")
		}
		cert, err := tls.X509KeyPair([]byte(item.TLSClientCert), []byte(item.TLSClientKey))
		if err != nil {
			return nil, fmt.Errorf("client certificate/key: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

// protocolVersion returns the item's MQTT version; items saved before the
// option existed have none and use MQTT 5.
func protocolVersion(item ConnectionItem) byte {
	if item.ProtocolVersion == 0 {
		return mqttclient.MQTT5
	}
	return item.ProtocolVersion
}

func clientHandlers(item ConnectionItem) mqttclient.Handlers {
	return mqttclient.Handlers{
		OnConnect: func(c *mqttclient.Client, ack mqttclient.ConnAck) {
			if ack.ReasonCode != 0 {
				return // refusals are reported by OnConnectError
			}
			events.publish(statusEvent(item.ID, item.Name, "connected", ""))
			subscribe(c, item)
		},
		OnConnectError: func(_ *mqttclient.Client, err error) {
			events.publish(statusEvent(item.ID, item.Name, "error", err.Error()))
		},
		OnDisconnect: func(_ *mqttclient.Client, ev mqttclient.DisconnectEvent) {
			var refused *mqttclient.ConnRefusedError
			switch {
			case ev.Err == nil:
				// Disconnect was called; the HTTP handler reports it.
			case errors.As(ev.Err, &refused):
				// Reported by OnConnectError.
			case errors.Is(ev.Err, mqttclient.ErrServerDisconnect):
				reason := mqttclient.ReasonCodeString(ev.ReasonCode)
				if ev.Properties != nil && ev.Properties.ReasonString != "" {
					reason = ev.Properties.ReasonString
				}
				events.publish(statusEvent(item.ID, item.Name, "disconnected", reason))
			default:
				events.publish(statusEvent(item.ID, item.Name, "error", ev.Err.Error()))
			}
		},
		OnMessage: onMessage(item),
	}
}

// subscribe sends the item's subscriptions. It runs from OnConnect, which
// must not block, so the acknowledgement is awaited on another goroutine.
func subscribe(c *mqttclient.Client, item ConnectionItem) {
	if len(item.Subscriptions) == 0 {
		return
	}
	subs := make([]mqttclient.Subscription, len(item.Subscriptions))
	for i, s := range item.Subscriptions {
		subs[i] = mqttclient.Subscription{Topic: s.Topic, QoS: byte(s.QoS)}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	p, err := c.Subscribe(ctx, subs, nil)
	if err != nil {
		cancel()
		publishError(item.ID, item.Name, "subscribe failed: %v", err)
		return
	}
	go func() {
		defer cancel()
		res, err := p.Wait(ctx)
		var refused *mqttclient.ReasonCodeError
		if err != nil && !errors.As(err, &refused) {
			publishError(item.ID, item.Name, "subscribe failed: %v", err)
			return
		}
		// SUBACK has one code per filter; report each refused one.
		for i, code := range res.ReasonCodes {
			if code >= 0x80 && i < len(subs) {
				publishError(item.ID, item.Name, "subscribe to %q refused: %s (0x%02x)",
					subs[i].Topic, mqttclient.ReasonCodeString(code), code)
			}
		}
	}()
}

func isSparkplugTopic(topic string) bool {
	return strings.HasPrefix(topic, "spBv1.0/")
}

func onMessage(item ConnectionItem) func(*mqttclient.Client, *mqttclient.Message) {
	return func(_ *mqttclient.Client, m *mqttclient.Message) {
		isSP := isSparkplugTopic(m.Topic)
		events.publish(SSEEvent{
			Type:         "message",
			ConnectionID: item.ID,
			Name:         item.Name,
			Topic:        m.Topic,
			Payload:      string(m.Payload),
			PayloadBytes: m.Payload,
			QoS:          m.QoS,
			Retained:     m.Retain,
			IsSparkplug:  isSP,
		})
		if err := msgDB.Write(msgstore.Message{
			Timestamp:    time.Now().UTC(),
			ConnID:       item.ID,
			ConnName:     item.Name,
			Topic:        m.Topic,
			Payload:      string(m.Payload),
			PayloadBytes: m.Payload,
			QoS:          int(m.QoS),
			Retained:     m.Retain,
			IsSparkplug:  isSP,
		}); err != nil {
			log.Printf("msgstore write: %v", err)
		}
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
	PayloadBytes []byte `json:"payloadBytes,omitempty"`
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
			PayloadBytes: m.PayloadBytes,
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

// publishError reports an MQTT error for a connection in the event stream.
// Unlike a connection_status event it does not change the connection's
// status, since the connection may still be up (a refused subscription, a
// failed publish).
func publishError(id, name, format string, args ...any) {
	events.publish(SSEEvent{
		Type:         "error",
		ConnectionID: id,
		Name:         name,
		Error:        fmt.Sprintf(format, args...),
	})
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
		if err := conn.client.Disconnect(disconnCtx, 0, nil); err != nil && !errors.Is(err, mqttclient.ErrNoConn) {
			publishError(id, name, "disconnect failed: %v", err)
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
	if err := conn.client.Disconnect(disconnCtx, 0, nil); err != nil && !errors.Is(err, mqttclient.ErrNoConn) {
		publishError(id, name, "disconnect failed: %v", err)
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

	// Wait for the acknowledgement (QoS 1 and 2) so failures reach the caller. On
	// timeout the message stays queued and is still delivered.
	p, err := conn.client.Publish(ctx, &mqttclient.Message{
		QoS:     req.QoS,
		Topic:   req.Topic,
		Payload: []byte(req.Payload),
		Retain:  req.Retain,
	})
	if err == nil {
		_, err = p.Wait(ctx)
	}
	if err != nil {
		connectionItemsMu.Lock()
		name := connectionItems[id].Name
		connectionItemsMu.Unlock()
		publishError(id, name, "publish to %q failed: %v", req.Topic, err)
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
