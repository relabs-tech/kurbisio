package broker

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/DrmagicE/gmqtt"
	"github.com/DrmagicE/gmqtt/pkg/packets"

	"github.com/google/uuid"
	_ "github.com/lib/pq" // for the postgres database
)

// Broker is a MQTT broker for IoT.
type Broker struct {
	p *plugin
}

// plugin is the plugin for GMQTT
type plugin struct {
	tlsln          net.Listener
	deviceIdsRwmux sync.RWMutex
	deviceIds      map[net.Conn]uuid.UUID

	service gmqtt.Server
	schema  string

	db *sql.DB
}

// MustNewBroker returns a new broker
func MustNewBroker(db *sql.DB, schema, certfile, keyfile string) *Broker {
	crt, err := tls.LoadX509KeyPair(certfile, keyfile)
	if err != nil {
		panic(err)
	}

	caCert, _ := ioutil.ReadFile("ca.crt")
	caCertPool := x509.NewCertPool()
	ok := caCertPool.AppendCertsFromPEM(caCert)
	log.Println("certs OK = ", ok)

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{crt},
		ClientCAs:    caCertPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}
	tlsln, err := tls.Listen("tcp", ":8883", tlsConfig)

	if err != nil {
		panic(err)
	}

	b := &Broker{
		p: &plugin{
			tlsln:     tlsln,
			deviceIds: make(map[net.Conn]uuid.UUID),
			db:        db,
			schema:    schema,
		},
	}

	// var err error
	// stanConn, err := stan.Connect("test-cluster", "kurbisio-4", stan.NatsURL("nats://localhost:4223"))
	// if err != nil {
	// 	panic(err)
	// }
	// k.stanConn = stanConn

	// poor man's database migrations
	_, err = b.p.db.Query(
		`CREATE extension IF NOT EXISTS "uuid-ossp";
CREATE table IF NOT EXISTS ` + b.p.schema + `.twin 
(device_id uuid references ` + b.p.schema + `.device(device_id) ON DELETE CASCADE, 
key varchar NOT NULL, 
request json NOT NULL, 
report json NOT NULL, 
requested_at timestamp NOT NULL, 
reported_at timestamp NOT NULL,
PRIMARY KEY(device_id, key)
);`)

	if err != nil {
		panic(err)
	}

	return b
}

// Run is blocking and runs the server
func (b *Broker) Run() {

	s := gmqtt.NewServer(
		gmqtt.WithTCPListener(b.p.tlsln),
		gmqtt.WithPlugin(b.p),
		// gmqtt.WithLogger(l),
	)
	s.Run()

	fmt.Println("started...")
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
	<-signalCh
	s.Stop(context.Background())
	fmt.Println("stopped")

}

// PublishMessageQ1 publishes an MQTT messsage with quality level 1
func (b *Broker) PublishMessageQ1(topic string, payload []byte) {
	msg := gmqtt.NewMessage(topic, payload, packets.QOS_1)
	b.p.service.PublishService().Publish(msg)
}

// Load implements plugin interface
func (p *plugin) Load(service gmqtt.Server) error {
	log.Println("load kurbisio")
	p.service = service
	return nil
}

// Unload implements plugin interface
func (p *plugin) Unload() error {
	return nil
}

// Name implements plugin interface
func (p *plugin) Name() string { return "kurbisio broker" }

// HookWrapper implements plugin interface
func (p *plugin) HookWrapper() gmqtt.HookWrapper {
	return gmqtt.HookWrapper{
		OnAcceptWrapper:     p.OnAcceptWrapper,
		OnConnectWrapper:    p.OnConnectWrapper,
		OnSubscribeWrapper:  p.OnSubscribeWrapper,
		OnSubscribedWrapper: p.OnSubscribedWrapper,
		OnMsgArrivedWrapper: p.OnMsgArrivedWrapper,
	}
}

func (p *plugin) deviceIDFromConnection(conn net.Conn) uuid.UUID {
	p.deviceIdsRwmux.RLock()
	defer p.deviceIdsRwmux.RUnlock()
	deviceID, _ := p.deviceIds[conn]
	return deviceID
}

// OnConnectWrapper enforces that the MQTT client ID matches the certificate common name
func (p *plugin) OnConnectWrapper(connect gmqtt.OnConnect) gmqtt.OnConnect {
	return func(ctx context.Context, client gmqtt.Client) (code uint8) {
		deviceID := p.deviceIDFromConnection(client.Connection())
		if client.OptionsReader().ClientID() != deviceID.String() {
			log.Println("connect denied,", client.OptionsReader().ClientID(), "not authorized")
			return packets.CodeNotAuthorized
		}
		log.Println("connect", deviceID)
		return connect(ctx, client)
	}
}

// OnAcceptWrapper authorizes clients via TLS certificates
func (p *plugin) OnAcceptWrapper(accept gmqtt.OnAccept) gmqtt.OnAccept {
	return func(ctx context.Context, conn net.Conn) bool {
		tlsConn, ok := conn.(*tls.Conn)
		if ok {
			err := tlsConn.Handshake()
			if err != nil {
				return false
			}
			state := tlsConn.ConnectionState()
			cert := state.VerifiedChains[0][0]
			commonName := cert.Subject.CommonName

			commonNameAsUUID, err := uuid.Parse(commonName)
			if err != nil {
				log.Println("invalid device ID in certificate:", commonName)
				return false
			}

			// TODO check that device ID is in database

			p.deviceIdsRwmux.Lock()
			defer p.deviceIdsRwmux.Unlock()
			p.deviceIds[conn] = commonNameAsUUID
			log.Println("accept", commonName)
		}
		return accept(ctx, conn)
	}
}

// OnMsgArrivedWrapper intercepts messages
func (p *plugin) OnMsgArrivedWrapper(arrived gmqtt.OnMsgArrived) gmqtt.OnMsgArrived {
	return func(ctx context.Context, client gmqtt.Client, msg packets.Message) (valid bool) {
		deviceID := client.OptionsReader().ClientID()
		topic := msg.Topic()
		log.Println("OnMsgArrived", deviceID, topic)
		if strings.HasPrefix(topic, "kurbisio/") {
			if strings.HasPrefix(topic, "kurbisio/twin/"+deviceID+"/reports/") {
				key := strings.TrimPrefix(topic, "kurbisio/twin/"+deviceID+"/reports/")
				if strings.Contains(key, "/") {
					log.Println("invalid twin key")
					return false
				}
				body := msg.Payload()
				if !json.Valid(body) {
					log.Println("invalid json")
					return false
				}
				log.Println("write twin report for", deviceID, key)
				now := time.Now().UTC()
				never := time.Time{}
				_, err := p.db.Query(
					`INSERT INTO `+p.schema+`.twin(device_id,key,request,report,requested_at,reported_at)
					VALUES($1,$2,$3,$4,$5,$6)
					ON CONFLICT (device_id, key) DO UPDATE SET report=$4,reported_at=$6;
					`, deviceID, key, "{}", string(body), never, now)
				if err != nil {
					log.Println(err)
				}
			} else if strings.HasPrefix(topic, "kurbisio/twin/"+deviceID+"/get") {
				body := msg.Payload()
				keys := []string{}
				err := json.Unmarshal(body, &keys)
				if err != nil {
					log.Println("invalid json")
					return false
				}
				for _, key := range keys {
					fmt.Println("sending twin request for key=", key)
					payload := []byte{}
					err = p.db.QueryRow(
						`SELECT request FROM `+p.schema+`.twin WHERE device_id=$1 AND key=$2;`,
						deviceID, key).Scan(&payload)
					if err != nil {
						log.Println(err)
					} else {
						msg := gmqtt.NewMessage("kurbisio/twin/"+deviceID+"/requests/"+key, payload, packets.QOS_0)
						p.service.PublishService().Publish(msg)
					}
				}
			}
		}

		// TODO for telemetry do something like this:
		// err := k.stanConn.Publish("devices."+deviceID+"."+topic, body)
		// if err != nil {
		// 	log.Println("publish to stan", err)
		// }

		return arrived(ctx, client, msg)
	}
}

// OnSubscribeWrapper enforces topic policy
func (p *plugin) OnSubscribeWrapper(subscribe gmqtt.OnSubscribe) gmqtt.OnSubscribe {
	return func(ctx context.Context, client gmqtt.Client, topic packets.Topic) (qos uint8) {
		deviceID := client.OptionsReader().ClientID()
		if !strings.HasPrefix(topic.Name, "kurbisio/twin/"+deviceID+"/requests/") {
			log.Println("OnSubscribe", deviceID, topic.Name, "denied!")
			return packets.SUBSCRIBE_FAILURE
		}
		return subscribe(ctx, client, topic)
	}

}

// OnSubscribedWrapper store the subscription
func (p *plugin) OnSubscribedWrapper(subscribed gmqtt.OnSubscribed) gmqtt.OnSubscribed {
	return func(ctx context.Context, client gmqtt.Client, topic packets.Topic) {
		deviceID, _ := uuid.Parse(client.OptionsReader().ClientID())
		log.Println("OnSubscribed", deviceID, topic.Name)
		subscribed(ctx, client, topic)
	}
}
