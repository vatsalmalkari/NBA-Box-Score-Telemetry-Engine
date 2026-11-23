# NBA-Box-Score-Telemetry-Engine
A strongly consistent, fault-tolerant distributed telemetry engine built from scratch in Java 21+. 
Designed to ingest rapid play-by-play NBA game streams, replicate state across a multi-node cluster via a 
custom Raft consensus protocol over gRPC, store hot player stats in off-heap memory to eliminate 
JVM Garbage Collection (GC) pauses, and asynchronously flush finalized box scores to 
Azure PostgreSQL. Provisioned and deployed automatically via Terraform, Docker, and GitHub Actions.

                      NBA Play-by-Play Event Producer 
                                     │
                             (gRPC / HTTP Stream)
                                     ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    Azure Virtual Network (10.0.0.0/16)                  │
│                                                                         │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │           Compute Subnet (Azure B1s Linux VM - Docker)            │  │
│  │                                                                   │  │
│  │   ┌───────────────────────┐                                       │  │
│  │   │  Raft Leader (Node 1) │ ◄── Virtual Threads (Project Loom)    │  │
│  │   │  Off-Heap ByteBuffer  │ ◄── Zero-GC Live Box-Score State      │  │
│  │   └─────┬───────────┬─────┘                                       │  │
│  │         │ (gRPC)    │ (gRPC)                                      │  │
│  │         ▼           ▼                                             │  │
│  │   ┌───────────┐   ┌───────────┐                                   │  │
│  │   │ Follower  │   │ Follower  │                                   │  │
│  │   │ (Node 2)  │   │ (Node 3)  │                                   │  │
│  │   └───────────┘   └───────────┘                                   │  │
│  └─────────┬─────────────────────────────────────────────────────────┘  │
│            | (Async Batch Flush on Quarter/Game End)                    │
│            ▼                                                            │
│  ┌───────────────────────────────────┐    ┌──────────────────────────┐  │
│  │  Database Subnet (Private)        │    │  Security & Telemetry    │  │
│  │  Azure Database for PostgreSQL    │    │  • Azure Key Vault       │  │
│  │  (Flexible Server - B1ms)         │    │  • Azure Monitor Logs    │  │
│  └───────────────────────────────────┘    └──────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘


nba-box-score-telemetry-engine/
├── .github/workflows/
│   └── pipeline.yml               # Single workflow: Test -> Build Docker -> Deploy (or Destroy)
├── src/                           # Java 21 project directly at root (no nested engine/ folder)
│   ├── main/proto/
│   │   └── telemetry.proto        # Single .proto file for both Raft & NBA events
│   └── main/java/com/nba/engine/
│       ├── RaftNode.java          # Consensus state, election, & replication
│       ├── OffHeapStore.java      # DirectByteBuffer player stat layout
│       ├── TelemetryServer.java   # Virtual Thread gRPC server & replay producer
│       └── PostgresFlusher.java   # Async cold-storage persistence
├── infra/
│   ├── main.tf                    # All Azure resources (RG, VNet, VM, Postgres) in one file
│   └── variables.tf               # Credentials & sizing variables
├── pom.xml                        # Maven dependencies (gRPC, Postgres JDBC, JUnit)
├── Dockerfile                     # Slim Java 21 runtime build
├── docker-compose.yml             # Local 3-node cluster + Postgres
└── README.md


Pipeline.yml: Uses a workflow_dispatch dropdown input (action: deploy | destroy) so you only maintain one YAML file.

Promoted Java to the root (src/ + pom.xml): Eliminates the nested engine/ folder so IDEs like IntelliJ IDEA or VS Code recognize the Maven project immediately upon opening.

Consolidated Terraform into main.tf: For a 4-resource stack (Resource Group, VNet, VM, PostgreSQL), keeping everything in one main.tf makes reading resource dependencies effortless—you can split them into modules later once it works.

4 focused Java classes: Starts with clear, single-file boundaries (RaftNode, OffHeapStore, TelemetryServer, PostgresFlusher) so you don't get lost in package hierarchies while prototyping.