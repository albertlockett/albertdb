
.PHONY: start-replica-1
start-replica-1:
	RUST_LOG=debug cargo run -- --config ./config-replica1.yaml

.PHONY: start-replica-2
start-replica-2:
	RUST_LOG=debug cargo run -- --config ./config-replica2.yaml

.PHONY: rep2-join-ring
rep2-join-ring:
	grpcurl \
	 	-plaintext \
	 	-import-path ./src/ring/proto \
	 	-proto ./src/ring/proto/ring.proto \
	 	-d '{ "node": {"node_id": "replica1","hostname": "localhost", "port": "51472" }}' \
		127.0.0.1:51471 \
		ring.Ring/JoinRing

.PHONY: rep1-get-top
rep1-get-topo:
	grpcurl \
		-plaintext \
		-import-path ./src/ring/proto \
		-proto ./src/ring/proto/ring.proto \
		-d '{ }' \
		127.0.0.1:51471 \
		ring.Ring/GetTopology