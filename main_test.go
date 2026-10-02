package main

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"
)

// testPKI is a CA with a server certificate for 127.0.0.1 and a client
// certificate, all as PEM.
type testPKI struct {
	caPEM, serverCertPEM, serverKeyPEM, clientCertPEM, clientKeyPEM string
	caPool                                                          *x509.CertPool
}

func newTestPKI(t *testing.T) testPKI {
	t.Helper()
	caKey := newKey(t)
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, _ := x509.ParseCertificate(caDER)

	issue := func(serial int64, cn string, usage x509.ExtKeyUsage, ips []net.IP) (string, string) {
		key := newKey(t)
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(serial),
			Subject:      pkix.Name{CommonName: cn},
			NotBefore:    time.Now().Add(-time.Hour),
			NotAfter:     time.Now().Add(time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{usage},
			IPAddresses:  ips,
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		keyDER, err := x509.MarshalECPrivateKey(key)
		if err != nil {
			t.Fatal(err)
		}
		return pemString("CERTIFICATE", der), pemString("EC PRIVATE KEY", keyDER)
	}

	p := testPKI{caPEM: pemString("CERTIFICATE", caDER), caPool: x509.NewCertPool()}
	p.caPool.AddCert(ca)
	p.serverCertPEM, p.serverKeyPEM = issue(2, "broker", x509.ExtKeyUsageServerAuth, []net.IP{net.ParseIP("127.0.0.1")})
	p.clientCertPEM, p.clientKeyPEM = issue(3, "swarmy-client", x509.ExtKeyUsageClientAuth, nil)
	return p
}

func newKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func pemString(typ string, der []byte) string {
	return string(pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}))
}

// startTLSBroker accepts one TLS connection, reads CONNECT and accepts it
// with an MQTT 5 CONNACK. It reports the client certificate's common name,
// or the error that ended the connection.
func startTLSBroker(t *testing.T, p testPKI, requireClientCert bool) (string, <-chan string) {
	t.Helper()
	cert, err := tls.X509KeyPair([]byte(p.serverCertPEM), []byte(p.serverKeyPEM))
	if err != nil {
		t.Fatal(err)
	}
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}}
	if requireClientCert {
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
		cfg.ClientCAs = p.caPool
	}
	ln, err := tls.Listen("tcp", "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })

	result := make(chan string, 1)
	go func() {
		nc, err := ln.Accept()
		if err != nil {
			result <- "accept: " + err.Error()
			return
		}
		defer nc.Close()
		tc := nc.(*tls.Conn)
		_ = tc.SetDeadline(time.Now().Add(5 * time.Second))
		if err := tc.Handshake(); err != nil {
			result <- "handshake: " + err.Error()
			return
		}
		if err := readPacket(bufio.NewReader(tc)); err != nil {
			result <- "read CONNECT: " + err.Error()
			return
		}
		if _, err := tc.Write([]byte{0x20, 0x03, 0x00, 0x00, 0x00}); err != nil {
			result <- "write CONNACK: " + err.Error()
			return
		}
		cn := "(no client certificate)"
		if certs := tc.ConnectionState().PeerCertificates; len(certs) > 0 {
			cn = certs[0].Subject.CommonName
		}
		result <- cn
		_, _ = io.Copy(io.Discard, tc) // until the client disconnects
	}()
	return "mqtts://" + ln.Addr().String(), result
}

// readPacket reads and discards one MQTT packet.
func readPacket(r *bufio.Reader) error {
	if _, err := r.ReadByte(); err != nil {
		return err
	}
	n, mult := 0, 1
	for {
		b, err := r.ReadByte()
		if err != nil {
			return err
		}
		n += int(b&0x7F) * mult
		if b&0x80 == 0 {
			break
		}
		mult *= 128
	}
	_, err := io.CopyN(io.Discard, r, int64(n))
	return err
}

func tlsItem(url string) ConnectionItem {
	return ConnectionItem{ID: "t", Name: "tls test", MqttServerUrl: url, ProtocolVersion: 5, ClientId: "tls-test", CleanSession: true}
}

func connectItem(t *testing.T, item ConnectionItem) error {
	t.Helper()
	c, err := newClient(item)
	if err != nil {
		t.Fatalf("newClient: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := c.Connect(ctx); err != nil {
		return err
	}
	return c.Disconnect(ctx, 0, nil)
}

func TestTLSServerVerified(t *testing.T) {
	p := newTestPKI(t)
	url, result := startTLSBroker(t, p, false)
	item := tlsItem(url)
	item.TLSCACert = p.caPEM
	if err := connectItem(t, item); err != nil {
		t.Fatal(err)
	}
	if got := <-result; got != "(no client certificate)" {
		t.Fatalf("broker: %s", got)
	}
}

func TestTLSUntrustedServerRejected(t *testing.T) {
	p := newTestPKI(t)
	url, _ := startTLSBroker(t, p, false)
	err := connectItem(t, tlsItem(url)) // no CA: the test CA is not a system root
	var unknown x509.UnknownAuthorityError
	if !errors.As(err, &unknown) {
		t.Fatalf("err %v, want x509.UnknownAuthorityError", err)
	}
}

func TestTLSInsecureSkipsVerification(t *testing.T) {
	p := newTestPKI(t)
	url, result := startTLSBroker(t, p, false)
	item := tlsItem(url)
	item.TLSInsecure = true
	if err := connectItem(t, item); err != nil {
		t.Fatal(err)
	}
	<-result
}

func TestMutualTLS(t *testing.T) {
	p := newTestPKI(t)
	url, result := startTLSBroker(t, p, true)
	item := tlsItem(url)
	item.TLSCACert = p.caPEM
	item.TLSClientCert = p.clientCertPEM
	item.TLSClientKey = p.clientKeyPEM
	if err := connectItem(t, item); err != nil {
		t.Fatal(err)
	}
	if got := <-result; got != "swarmy-client" {
		t.Fatalf("broker saw client certificate %q", got)
	}
}

func TestMutualTLSWithoutClientCertRejected(t *testing.T) {
	p := newTestPKI(t)
	url, result := startTLSBroker(t, p, true)
	item := tlsItem(url)
	item.TLSCACert = p.caPEM
	if err := connectItem(t, item); err == nil {
		t.Fatal("connected without a client certificate")
	}
	if got := <-result; !strings.HasPrefix(got, "handshake:") {
		t.Fatalf("broker: %s", got)
	}
}

func TestTLSConfigErrors(t *testing.T) {
	p := newTestPKI(t)
	cases := []struct {
		name string
		item ConnectionItem
		want string
	}{
		{"tls settings on plain url", ConnectionItem{MqttServerUrl: "mqtt://h:1883", TLSInsecure: true}, "need an mqtts://"},
		{"bad CA", ConnectionItem{MqttServerUrl: "mqtts://h", TLSCACert: "not pem"}, "CA certificate"},
		{"cert without key", ConnectionItem{MqttServerUrl: "mqtts://h", TLSClientCert: p.clientCertPEM}, "both a client certificate and its key"},
		{"mismatched key", ConnectionItem{MqttServerUrl: "mqtts://h", TLSClientCert: p.clientCertPEM, TLSClientKey: p.serverKeyPEM}, "client certificate/key"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tlsConfig(tc.item)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err %v, want it to mention %q", err, tc.want)
			}
		})
	}

	if cfg, err := tlsConfig(ConnectionItem{MqttServerUrl: "mqtt://h"}); cfg != nil || err != nil {
		t.Fatalf("plain url: cfg %v err %v", cfg, err)
	}
	if cfg, err := tlsConfig(ConnectionItem{MqttServerUrl: "ssl://h"}); cfg == nil || err != nil {
		t.Fatalf("ssl url without settings: cfg %v err %v, want a default config", cfg, err)
	}
}
