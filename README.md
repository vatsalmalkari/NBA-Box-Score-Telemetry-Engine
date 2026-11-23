# Distributed In-Memory Live NBA Box-Score & Telemetry Engine

A strongly consistent, fault-tolerant distributed telemetry engine built from scratch in Java 21. This system ingests rapid streams of live NBA play-by-play events, replicates state updates across a multi-node cluster using a custom Raft consensus protocol over gRPC, stores hot player statistics in off-heap memory to eliminate JVM Garbage Collection (GC) pauses, and asynchronously persists finalized game box scores to Azure Database for PostgreSQL.

The entire cloud infrastructure and deployment lifecycle is automated using Docker, Terraform, and GitHub Actions, engineered specifically to operate within zero-cost free-tier cloud thresholds.

Real-time sports telemetry platforms face a strict engineering trade-off: incoming play-by-play events arrive in high-frequency bursts that demand sub-millisecond in-memory processing, while downstream analytics and historical records require durable relational storage and zero data loss during server failures.

This project bridges two disciplines into a single platform:

**Distributed Systems & Low-Latency Computing:** Demonstrating how stateful services achieve strong consistency, leader election, deterministic off-heap memory layout, and non-blocking concurrency without relying on heavy external message brokers or caching middleware.

**Cloud Infrastructure & DevOps Automation:** Defining the complete cloud environment compute, networking, managed database, and container orchestration—declaratively as code so the stack can be tested, deployed, and torn down automatically with zero manual portal configuration and zero monthly cloud spend.

---

## Core Architectural Principles

### Hot Path vs. Cold Path Separation

 **The Hot Path (In-Memory Consensus):** Live game telemetry (shots, rebounds, assists, fouls) enters the cluster via gRPC and is replicated across a 3-node Raft cluster. Committed events mutate pre-allocated off-heap byte buffers directly in RAM, allowing live box-score queries to execute with deterministic latency and zero heap allocation overhead.

 **The Cold Path (Asynchronous Relational Persistence):** Rather than implementing complex disk-based log compaction and binary snapshotting inside the custom Raft engine, finalized game states are flushed asynchronously to PostgreSQL upon end game events. This keeps the distributed consensus layer lightweight and stateless across full cluster restarts while preserving durable historical data.

### Ephemeral, FinOps-Driven Cloud Design

Every cloud resource is provisioned via Terraform and designed around free-tier consumption limits (single standard B1s compute instance, single standard_B1ms PostgreSQL Flexible Server, and GitHub Container Registry instead of paid container registries).

Infrastructure is treated as disposable: environments can be spun up on demand for integration testing and destroyed via a single CI/CD trigger when idle.

---

## Deep Dive: Engine Internals

### 1. Concurrency Model (Java 21 Virtual Threads)

Traditional thread-per-request servers exhaust operating system threads under high network I/O concurrency, while reactive callback frameworks complicate debugging and stack traces. This engine uses Project Loom Virtual Threads (Executors.newVirtualThreadPerTaskExecutor()):

Every incoming gRPC ingestion call, read query, outbound peer heartbeat, and asynchronous PostgreSQL flush runs on its own lightweight Virtual Thread.

When a Virtual Thread blocks waiting on a follower gRPC acknowledgment or a JDBC network socket, the JVM unmounts it from the underlying carrier OS thread, allowing thousands of concurrent telemetry streams to run on minimal CPU cores.

### 2. Custom Raft Consensus over gRPC
The cluster consists of 3 peer nodes communicating over HTTP/2 using Protocol Buffers (telemetry.proto):

**Leader Election:** Nodes boot as follower  with randomized election timeouts (400ms–700ms) to prevent split votes. If a follower receives no heartbeat before its timer expires, it increments its currentTerm, transitions to candidate, votes for itself, and broadcasts RequestVote RPCs in parallel. Winning a majority quorum (2 of 3 nodes) promotes the node to leader.

 **Log Replication & Quorum Commit:** Only the leader accepts write proposals (IngestPlayEvent). Follower nodes return a leader_hint so clients can redirect requests. Upon receiving an event, the leader appends a LogEntry to its local log and broadcasts AppendEntries RPCs to peers. Once a majority quorum acknowledges the entry within the RPC deadline, the leader advances its commitIndex, applies the mutation to its state machine, and propagates the updated commitIndex to followers on subsequent heartbeats.

### 3. Zero-GC Off-Heap State Management (OffHeapStore)

High-frequency stat updates on standard Java heap collections (HashMap or boxed Integer objects) generate continuous garbage that triggers Stop-The-World GC pauses.
OffHeapStore allocates a contiguous block of native OS memory via ByteBuffer.allocateDirect().

Each player ID maps deterministically to a fixed 16-byte record (offset = playerId * 16), packing four 32-bit integers (playerId, points, rebounds, assists) at exact byte offsets.

Concurrency is coordinated using StampedLock: writers acquire an exclusive write stamp for microsecond-scale buffer mutations, while live box-score readers use lock-free optimistic reads (tryOptimisticRead()) to avoid thread contention.

### 4. Asynchronous Cold-Storage Flusher (`PostgresFlusher`)

On startup, nodes verify the existence of the box_scores relational table (game_id, player_id, points, rebounds, assists, updated_at) with a composite primary key (game_id, player_id).

When the Raft leader commits an end game event, it spawns a detached Virtual Thread that executes an idempotent INSERT ... ON CONFLICT DO UPDATE upsert against PostgreSQL, ensuring database latency never stalls the Raft consensus loop.

---

## Cloud Infrastructure & DevOps Automation

**Containerization (Dockerfile & docker-compose.yml):** Uses a multi-stage Docker build—compiling .proto definitions and shading a single executable JAR with Maven, then copying only the artifact into an Alpine-based Java 21 JRE image. Heap and direct memory are explicitly bounded (-Xmx160m -XX:MaxDirectMemorySize=64m) so all three Raft containers coexist safely within 1 GB of system RAM.

**Infrastructure as Code (infra/):** Terraform provisions the Azure Resource Group, Virtual Network, Network Security Groups (restricting gRPC and SSH ingress), Linux VM with cloud-init Docker bootstrapping, and Azure Database for PostgreSQL Flexible Server. Remote state is stored in Azure Blob Storage.

**CI/CD Pipeline (.github/workflows/pipeline.yml):** Automates JUnit consensus tests, builds and tags container images to GitHub Container Registry (ghcr.io), and exposes manual workflow triggers to either deploy updates (terraform apply) or tear down all cloud resources (`terraform destroy`).

---

## Repository Structure

```text
nba-telemetry-raft-engine/
├── .github/workflows/
│   └── pipeline.yml               # Automated test, container build, deploy, and destroy workflows
├── src/
│   ├── main/proto/
│   │   └── telemetry.proto        # gRPC service and Protobuf message definitions
│   └── main/java/com/nba/engine/
│       ├── RaftNode.java          # Raft state machine, leader election, and log replication
│       ├── OffHeapStore.java      # DirectByteBuffer memory layout and StampedLock access
│       ├── TelemetryServer.java   # Virtual Thread gRPC server entrypoint
│       └── PostgresFlusher.java   # Asynchronous JDBC cold-storage persistence
├── infra/
│   ├── main.tf                    # Azure VNet, B1s VM, and PostgreSQL Flexible Server definitions
│   └── variables.tf               # Configurable infrastructure parameters and credentials
├── pom.xml                        # Maven build configuration (Java 21, gRPC, Protobuf, PostgreSQL)
├── Dockerfile                     # Multi-stage slim Java 21 runtime image
├── docker-compose.yml             # Local 3-node cluster + PostgreSQL orchestration
└── README.md                      # Project architecture and operational documentation
```

---

## Configuration & Environment Variables

Each node is configured entirely via environment variables to maintain parity between local Docker Compose runs and cloud deployments:

| Variable | Description | Example Value |
| :--- | :--- | :--- |
| `NODE_ID` | Unique identifier for the Raft peer | `node-1` |
| `PORT` | TCP port the gRPC server binds to | `50051` |
| `PEERS` | Comma-separated `host:port` list of peer nodes | `node-2:50051,node-3:50051` |
| `DB_URL` | JDBC connection string for PostgreSQL cold storage | `jdbc:postgresql://postgres:5432/nba_telemetry` |
| `DB_USER` | PostgreSQL database username | `nba_admin` |
| `DB_PASS` | PostgreSQL database password | `localdevpass` |

---

## Local Quickstart & Fault-Tolerance Testing

### Prerequisites

Docker Engine and Docker Compose v2+
(Optional) `grpcurl` CLI for manual RPC inspection

### 3-Node Cluster

Java 21 artifact and start all three Raft nodes alongside PostgreSQL:
bash docker compose up --build 
Monitor the console logs to observe the cluster initialize, trigger a randomized election timeout, and elect a LEADER .

Leader Failover

Which node logged ELECTED LEADER (for example, raft-node-1) and terminate its container mid-operation:
bash docker stop raft-node-1 
Within ~500ms, the remaining two nodes will detect the missed heartbeat, increment the Raft term, hold a new election, 
and elect a replacement leader without dropping cluster availability. 
Restarting the stopped container (docker start raft-node-1) causes it to rejoin cleanly as a FOLLOWER.

Tear Down Local Environment

bash docker compose down -v 