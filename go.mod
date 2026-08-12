module statusengine-worker

go 1.26.5

require github.com/gorilla/websocket v1.5.3

require (
	github.com/DATA-DOG/go-sqlmock v1.5.2
	github.com/go-sql-driver/mysql v1.10.0
	github.com/mikespook/gearman-go v0.0.0-20220520031403-2a518e866145
	github.com/prometheus/client_golang v1.24.1
	github.com/rabbitmq/amqp091-go v1.13.0
	golang.org/x/text v0.41.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.70.1 // indirect
	github.com/prometheus/procfs v0.21.1 // indirect
	golang.org/x/sys v0.47.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace github.com/mikespook/gearman-go => /home/nook24/git/gearman-go
