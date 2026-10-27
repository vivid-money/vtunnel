module github.com/vivid-money/vtunnel/e2e

go 1.26.10

require (
	github.com/gorilla/websocket v1.5.3
	github.com/vivid-money/vtunnel v0.0.0
	golang.org/x/net v0.61.0
	google.golang.org/grpc v1.85.0
)

require (
	github.com/cenkalti/backoff/v4 v4.3.0 // indirect
	github.com/hashicorp/yamux v0.1.2 // indirect
	golang.org/x/crypto v0.58.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	golang.org/x/text v0.43.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20261026165753-2ffab32bce7e // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)

replace github.com/vivid-money/vtunnel => ../
