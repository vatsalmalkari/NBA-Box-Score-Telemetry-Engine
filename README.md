# Distributed Live NBA Box-Score & Telemetry Engine

A distributed NBA telemetry engine built from scratch using Java 21, gRPC, Protocol Buffers, and a custom Raft consensus implementation. The system runs a three-node cluster that coordinates leadership, handles node failures, and maintains persistent consensus metadata across restarts.

The next stage extends this foundation with replicated live play-by-play events, majority-based log commitment, and an in-memory state machine for updating player statistics. Finalized game box scores will be persisted to PostgreSQL, with Docker providing local multi-node orchestration.

The project focuses on understanding and implementing the fundamentals of distributed systems rather than relying on external consensus frameworks or message brokers.

---

## Core Architectural Principles

### Distributed Consensus & Fault Tolerance

**The Consensus Layer (Raft):** Three independent Java nodes communicate over gRPC using Protocol Buffers. A custom Raft implementation coordinates leader election, Pre-Vote, periodic heartbeats, and leader failover. Each node persists its current term and voting state so that it can recover its consensus metadata after restarting.

**The Replication Layer (Planned):** The elected leader will accept incoming play-by-play events and replicate them to followers through Raft AppendEntries RPCs. An event will become committed only after a majority of nodes have acknowledged storing it.

### In-Memory Processing & Relational Persistence

**The Hot Path (Planned):** Committed play-by-play events will be applied to an in-memory state machine backed by Java collections. The state machine will update player statistics such as points, rebounds, and assists, supporting live box-score queries without requiring a database round trip for every update.

**The Cold Path (Planned):** When a game ends, the finalized player statistics will be flushed to PostgreSQL using idempotent database operations. PostgreSQL will provide persistent storage for completed game box scores.

### Containerized Infrastructure

Docker Compose orchestrates the three Raft nodes and PostgreSQL locally, allowing the distributed system to be tested in a reproducible environment.

Future infrastructure work includes Terraform-managed Azure deployment and GitHub Actions automation for testing, container builds, and deployment.

---

## Deep Dive: Engine Internals

### 1. Concurrency Model (Java 21 Virtual Threads)

Traditional thread-per-request servers can exhaust operating system threads under high network I/O concurrency. Java 21 Virtual Threads provide a lightweight execution model for handling blocking operations.

This project uses Java's virtual-thread executor for RPC-related asynchronous tasks:

- Peer communication and outbound Raft RPCs execute asynchronously.
- Nodes can communicate with multiple peers without dedicating a platform thread to every blocking RPC.
- Election and heartbeat scheduling remain coordinated independently of the peer RPC execution tasks.

The implementation prioritizes straightforward concurrency management while allowing the consensus layer to communicate with multiple nodes concurrently.

### 2. Custom Raft Consensus over gRPC

The cluster consists of three independent peer nodes communicating over HTTP/2 using Protocol Buffers defined in `telemetry.proto`.

**Leader Election:** Each node begins as a follower and uses a randomized election timeout between 500ms and 800ms. If a follower does not receive valid leader communication before its timeout expires, it can initiate an election.

The election process includes:

- Incrementing the current term and transitioning to candidate.
- Voting for itself and requesting votes from other peers.
- Electing a leader after receiving a majority quorum (2 of 3 nodes).
- Persisting term and voting information through `RaftStateStore`.

**Pre-Vote:** Before incrementing its term, a candidate performs a preliminary vote round to determine whether it could realistically win an election. This helps prevent an isolated node from repeatedly increasing the cluster term when it reconnects.

**Leader Lease:** A 400ms leader lease helps prevent nodes that have recently received communication from a leader from granting votes during that period. This reduces unnecessary elections while a leader is still active.

**Heartbeats:** The leader sends periodic AppendEntries heartbeat RPCs every 75ms to maintain leadership and prevent followers from starting unnecessary elections.

**Election Safety:** Vote responses are tracked by peer identity using a set, preventing duplicate responses from inflating the vote count. Election round identifiers are also used to ignore stale Pre-Vote and vote responses.

**Failure Recovery:** When a leader becomes unavailable, the remaining nodes can elect a replacement using a majority quorum. A restarted node recovers its persisted term and voting state and rejoins the cluster as a follower.

**Current Limitation:** The current AppendEntries implementation sends empty heartbeats. Replicated log entries, `nextIndex`, `matchIndex`, and majority-based commitment are the next implementation milestone.

### 3. Persistent Raft State (`RaftStateStore`)

Each node maintains a small persistent state file containing its current term and voting information.

The `RaftStateStore` is responsible for:

- Recovering the current term and previously recorded vote during startup.
- Persisting term and vote changes.
- Writing updated state through a temporary file before replacing the previous state file.
- Using file synchronization and atomic file replacement where supported.

Each Docker node uses its own persistent volume so that its consensus metadata survives container restarts.

This is currently limited to Raft election metadata. Durable replicated event logs and complete application-state recovery have not yet been implemented.

### 4. NBA Event Processing & State Machine (Planned)

The application layer will process live NBA play-by-play events after the Raft replication layer is implemented.

**Event Ingestion:** Clients will submit events to the current leader through a gRPC ingestion endpoint. Followers will return a leader hint so clients can redirect their requests.

**Log Replication:** The leader will append each event to its local log and replicate it to followers using AppendEntries RPCs. Replication progress will be tracked through `nextIndex` and `matchIndex`.

**Majority Commitment:** Once a majority of nodes have durably stored an entry, the leader can advance its `commitIndex` according to Raft's commitment rules and apply the committed event to the state machine.

**Statistics State Machine:** A simple in-memory map will maintain player statistics keyed by game and player identifiers. Committed events will update the corresponding box-score values.

**Failure Recovery:** Integration tests will verify that an event committed before a leader failure remains available after a replacement leader is elected.

### 5. PostgreSQL Persistence (Planned)

PostgreSQL will store finalized game box scores after the game has ended.

The planned relational schema includes:

| Column | Description |
|---|---|
| `game_id` | NBA game identifier |
| `player_id` | Player identifier |
| `points` | Total points |
| `rebounds` | Total rebounds |
| `assists` | Total assists |
| `updated_at` | Last update timestamp |

A composite primary key on `(game_id, player_id)` will allow idempotent upserts.

The persistence layer will flush finalized statistics at game completion rather than writing every individual play-by-play event directly to PostgreSQL.

---

## Cloud Infrastructure & DevOps Automation

**Containerization (`Dockerfile` & `docker-compose.yml`):** Docker Compose runs three independent Java nodes alongside PostgreSQL. Each Raft node has its own identity, peer configuration, and persistent data volume.

**Infrastructure as Code (`infra/` - Planned):** Terraform will define the Azure infrastructure required to deploy the containerized application, including compute resources, networking, and managed PostgreSQL.

**CI/CD Pipeline (`.github/workflows/` - Planned):** GitHub Actions will automate JUnit tests, Docker image builds, and infrastructure deployment workflows.

Cloud deployment and CI/CD automation are future milestones; the current focus is completing and validating the distributed consensus and event replication layers locally.

---

## Repository Structure

```text
nba-telemetry-raft-engine/
├── src/
│   ├── main/
│   │   ├── proto/
│   │   │   └── telemetry.proto        # gRPC service and Protobuf definitions
│   │   └── java/com/nba/engine/
│   │       ├── RaftNode.java          # Raft election and heartbeat coordination
│   │       ├── RaftStateStore.java    # Persistent term and vote metadata
│   │       ├── RaftRole.java          # Follower, candidate, and leader roles
│   │       ├── PeerClient.java        # Inter-node gRPC communication
│   │       ├── NodeConfig.java        # Node identity and peer configuration
│   │       ├── TelemetryServer.java  # gRPC server entrypoint
│   │       └── TelemetryServiceImpl.java # Raft RPC service implementation
│   └── test/
│       └── java/
│           └── OneLeaderPerTermTest.java # Raft election and failover tests
├── pom.xml                            # Maven, Java 21, gRPC, and Protobuf
├── Dockerfile                         # Java application container image
├── docker-compose.yml                 # Three-node cluster and PostgreSQL
└── README.md                           # Project documentation
```

Additional components such as the replicated log, NBA state machine, and PostgreSQL persistence layer will be added as implementation progresses.

---

## Configuration & Environment Variables

Each node is configured through environment variables to maintain consistent configuration across the local Docker deployment.

| Variable | Description | Example Value |
|---|---|---|
| `NODE_ID` | Unique identifier for the Raft peer | `node-1` |
| `PORT` | TCP port the gRPC server binds to | `50051` |
| `PEERS` | Comma-separated peer addresses | `node-2:50051,node-3:50051` |
| `RAFT_STATE_FILE` | Path to the persistent Raft state file | `/data/raft-state.properties` |

PostgreSQL connection variables will be used by the application persistence layer when that component is implemented.

---

## Local Quickstart & Fault-Tolerance Testing

### Prerequisites

- Java 21
- Maven
- Docker Engine
- Docker Compose v2+

### 1. Build the Project

Compile and package the Java application:

```bash
mvn clean package -DskipTests
```

### 2. Start the Three-Node Cluster

Build the Docker images and start all three Raft nodes alongside PostgreSQL:

```bash
docker compose build
docker compose up -d
```

Check container health and status:

```bash
docker compose ps
```

Monitor the Raft nodes:

```bash
docker compose logs -f node-1 node-2 node-3
```

The nodes initialize, exchange peer RPCs, and begin randomized leader elections. One node should become leader after obtaining a majority of votes.

### 3. Leader Failover & Recovery

Identify the elected leader from the logs, then stop its container:

```bash
docker compose stop node-1
```

Replace `node-1` with whichever node is currently the leader.

The remaining two nodes should detect the missing heartbeats, initiate an election, and elect a replacement leader in a higher term.

Restart the stopped node:

```bash
docker compose start node-1
```

The restarted node should recover its persisted term and vote information and rejoin the cluster without disrupting the current leader.

### 4. Run Raft Tests

Execute the JUnit test suite:

```bash
mvn test
```

`OneLeaderPerTermTest` currently covers two scenarios:

- **Single Leader Per Term:** Runs a three-node cluster for 10 seconds and verifies that no term has more than one elected leader.
- **Leader Failure and Rejoin:** Stops the elected leader, verifies that another node becomes leader in a higher term, then restarts the original leader and verifies that it rejoins without disturbing the new leader.

Both tests have passed across five consecutive runs.

### 5. Tear Down the Local Environment

Stop and remove the containers:

```bash
docker compose down
```

To also remove persistent Docker volumes:

```bash
docker compose down -v
```

**Warning:** The `-v` option deletes persisted node state and PostgreSQL data.

---

## Planned Development Milestones

- [x] Three-node Java 21 Raft cluster
- [x] gRPC-based inter-node communication
- [x] Persistent term and vote metadata
- [x] Randomized leader election and heartbeats
- [x] Pre-Vote and leader lease mechanisms
- [x] Duplicate-vote protection and stale response filtering
- [x] JUnit leader safety and failover/rejoin tests
- [x] Final clean Docker build and repeated failover validation
- [ ] Leader-only play-by-play event ingestion
- [ ] Persistent Raft write-ahead log
- [ ] AppendEntries log replication
- [ ] `nextIndex` and `matchIndex` tracking
- [ ] Majority-based log commitment
- [ ] In-memory NBA statistics state machine
- [ ] Committed-event survival after leader failure
- [ ] PostgreSQL persistence of finalized game statistics
- [ ] Azure deployment using Terraform
- [ ] GitHub Actions CI/CD pipeline
