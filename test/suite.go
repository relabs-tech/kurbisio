package test

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/relabs-tech/kurbisio/core/backend"
	"github.com/relabs-tech/kurbisio/core/client"
	"github.com/relabs-tech/kurbisio/core/csql"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// use POSTGRES="host=localhost port=5432 user=postgres dbname=postgres sslmode=disable"
// and POSTGRES_PASSWORD="docker"
type TestService struct {
	Postgres         string `env:"POSTGRES,required" description:"the connection string for the Postgres DB without password"`
	PostgresPassword string `env:"POSTGRES_PASSWORD,optional" description:"password to the Postgres DB"`
	KafkaBrokers     string `env:"KAFKA_BROKERS,required" description:"the connection string for the Kafka brokers"`
	backend          *backend.Backend
	client           client.Client
	clientNoAuth     client.Client
	Db               *csql.DB
	Router           *mux.Router
}

type IntegrationTestSuite struct {
	*backend.Backend
	srv *http.Server

	dbConn *csql.DB
	router *mux.Router
	suite.Suite
	network           testcontainers.Network
	kafkaContainer    testcontainers.Container
	postgresContainer testcontainers.Container
	reaperContainer   testcontainers.Container
	kafkaConn         *kafka.Conn
	kafkaAddr         string
	postgresAddr      string
	postgresUser      string
	postgresPassword  string
	postgresDB        string
	// Optionally, add your service or API client here
}

func (s *IntegrationTestSuite) createTopic(topic string, numPartitions int) error {
	if s.kafkaConn == nil {
		return fmt.Errorf("kafka connection is not established")
	}

	err := s.kafkaConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     numPartitions,
		ReplicationFactor: 1,
	})
	if err != nil {
		return fmt.Errorf("failed to create topic %s: %w", topic, err)
	}
	return nil
}

func (s *IntegrationTestSuite) deleteTopic(topic string) error {
	if s.kafkaConn == nil {
		return fmt.Errorf("kafka connection is not established")
	}

	err := s.kafkaConn.DeleteTopics(topic)
	if err != nil {
		return fmt.Errorf("failed to delete topic %s: %w", topic, err)
	}
	return nil
}

func (s *IntegrationTestSuite) SetupSuite() {
	ctx := context.Background()

	// Create a shared Docker network for Kafka and Zookeeper
	networkName := "test-kafka-network_" + fmt.Sprintf("%d", time.Now().Unix())
	network, err := testcontainers.GenericNetwork(ctx, testcontainers.GenericNetworkRequest{
		NetworkRequest: testcontainers.NetworkRequest{
			Name:           networkName,
			CheckDuplicate: true,
		},
	})
	s.Require().NoError(err)
	s.network = network

	// Start PostgreSQL container
	postgresUser := "testuser"
	postgresPassword := "testpass"
	postgresDB := "testdb"

	pgReq := testcontainers.ContainerRequest{
		Image:        "postgres:15",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     postgresUser,
			"POSTGRES_PASSWORD": postgresPassword,
			"POSTGRES_DB":       postgresDB,
		},
		Networks:       []string{networkName},
		NetworkAliases: map[string][]string{networkName: {"postgres"}},
		WaitingFor:     wait.ForListeningPort("5432/tcp"),
	}
	pgC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: pgReq,
		Started:          true,
	})
	s.Require().NoError(err)
	s.postgresContainer = pgC

	pgHost, err := pgC.Host(ctx)
	s.Require().NoError(err)
	pgPort, err := pgC.MappedPort(ctx, "5432")
	s.Require().NoError(err)
	s.postgresAddr = fmt.Sprintf("%s:%s", pgHost, pgPort.Port())
	s.postgresUser = postgresUser
	s.postgresPassword = postgresPassword
	s.postgresDB = postgresDB

	zooReq := testcontainers.ContainerRequest{
		Image:        "confluentinc/cp-zookeeper:7.5.0",
		ExposedPorts: []string{"2181/tcp"},
		Env: map[string]string{
			"ZOOKEEPER_CLIENT_PORT": "2181",
			"ZOOKEEPER_TICK_TIME":   "2000",
		},
		WaitingFor:     wait.ForListeningPort("2181/tcp"),
		Networks:       []string{networkName},
		NetworkAliases: map[string][]string{networkName: {"zookeeper"}},
	}
	_, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: zooReq,
		Started:          true,
	})
	s.Require().NoError(err)

	kafkaReq := testcontainers.ContainerRequest{
		Image:        "confluentinc/cp-kafka:7.5.0",
		ExposedPorts: []string{"9092:9092/tcp", "29092:29092/tcp"},
		Env: map[string]string{
			"KAFKA_BROKER_ID":                        "1",
			"KAFKA_ZOOKEEPER_CONNECT":                "zookeeper:2181",
			"KAFKA_LISTENERS":                        "PLAINTEXT://0.0.0.0:9092,PLAINTEXT_HOST://0.0.0.0:29092,EXTERNAL://0.0.0.0:9093",
			"KAFKA_ADVERTISED_LISTENERS":             "PLAINTEXT://localhost:9092,PLAINTEXT_HOST://localhost:29092,EXTERNAL://kafka:9093",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":   "PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT,EXTERNAL:PLAINTEXT",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
			"ALLOW_PLAINTEXT_LISTENER":               "yes",
		},
		WaitingFor:     wait.ForLog("started (kafka.server.KafkaServer)"),
		Networks:       []string{networkName},
		NetworkAliases: map[string][]string{networkName: {"kafka"}},
	}
	kafkaC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: kafkaReq,
		Started:          true,
	})
	s.Require().NoError(err)
	s.kafkaContainer = kafkaC

	kafkaHost, err := kafkaC.Host(ctx)
	s.Require().NoError(err)
	kafkaPort, err := kafkaC.MappedPort(ctx, "9092")
	s.Require().NoError(err)
	s.kafkaAddr = fmt.Sprintf("%s:%s", kafkaHost, kafkaPort.Port())

	s.kafkaConn, err = kafka.Dial("tcp", s.kafkaAddr)
	s.Require().NoError(err)

	s.router = mux.NewRouter()
	s.dbConn = csql.OpenWithSchema(fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=disable",
		pgHost, pgPort.Port(), s.postgresUser, s.postgresDB), s.postgresPassword, "public")

	configurationJSON := `{
		"collections": [			

	    	{
			"resource": "a",
			"permits": [
				{
					"role": "role1",
					"operations": [
						"create",
						"update"
					]
				}
			]	
		  },
		  {
			"resource": "b",
			"permits": [
				{
					"role": "role1",
					"operations": [
						"create",
						"update"
					]
				},
				{
					"role": "role2",
					"operations": [
						"read"
					]
				}
			]
		  }
		],
		"relations": [
			{
				"resource": "myrelation",
				"left": "a",
				"right": "b",
				"left_permits": [
					{
						"role": "role2",
						"operations": [
							"read",
							"create",							
							"list",
							"delete"
						]
					}
				],
				"right_permits": [
					{
						"role": "role2",
						"operations": [
							"read",				
							"list"
						]
					},
					{
						"role": "role1",
						"operations": [										
							"list"
						]
					}

				]
			}
		]
	  }
	`
	bb := backend.Builder{
		DB:              s.dbConn,
		KafkaBrokers:    []string{s.kafkaAddr},
		Router:          s.router,
		OutboxTableName: "_resource_notification_outbox_",
		Config:          configurationJSON,
	}
	s.Backend = backend.New(&bb)

	err = s.createTopic("resource_notification", 1)
	s.Require().NoError(err, "Failed to create resource_notification topic")

	reaperReq := testcontainers.ContainerRequest{
		Image:          "reaper:latest",
		Networks:       []string{networkName},
		NetworkAliases: map[string][]string{networkName: {"reaper"}},
		WaitingFor:     wait.ForLog("Starting background delivery handler"),
	}

	reaperC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: reaperReq,
		Started:          true,
	})
	s.Require().NoError(err)
	s.reaperContainer = reaperC

	s.srv = &http.Server{
		Addr:    ":8080",
		Handler: s.router,
	}
	go func() {
		err := s.srv.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			s.T().Errorf("Failed to start HTTP server: %v", err)
		}
	}()
}

func (s *IntegrationTestSuite) TearDownSuite() {
	ctx := context.Background()
	// Stop the HTTP server
	if s.srv != nil {
		err := s.srv.Shutdown(ctx)
		s.Require().NoError(err)
	}

	s.Close()

	if s.kafkaContainer != nil {
		err := s.kafkaContainer.Terminate(ctx)
		s.Require().NoError(err)
	}
	if s.postgresContainer != nil {
		err := s.postgresContainer.Terminate(ctx)
		s.Require().NoError(err)
	}
	// Stop your service if it was started
	// s.stopService()
}
