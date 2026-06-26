# Erebus (NimbusFS) - Cloud Storage Platform Specification

## 1. Project Overview
Erebus (internally referred to as NimbusFS) is a self-hostable, polyglot (Go + Rust) cloud storage platform designed for personal and organizational use. It provides S3-class storage intelligence—smart tiering, chunk-level deduplication, and content-addressed storage—behind a first-class native interface: a React web app and a Rust desktop client. It functions as both a consumer product with a polished UI and a developer primitive offering an S3-compatible API endpoint.

### 1.1 Key Principles
- **Global Content-Addressed Storage (CAS):** Files are chunked and deduplicated globally across all tenants.
- **Microservices Architecture:** The backend consists of 6-8 Go services communicating via gRPC, using NATS JetStream as the event bus.
- **No Binary Delta Storage:** Versioning relies on CAS and chunk manifests (saving storage on unmodified chunks) rather than complex delta algorithms.
- **Bandwidth-Efficient Uploads:** Clients hash chunks locally and negotiate with the server to upload only missing data.

---

## 2. Architecture Layers

### 2.1 Storage Engine & Tiering (Layer 1)
The physical layer responsible for how data is chunked, hashed, deduplicated, compressed, and stored across multiple tiers.

* **Content-Addressed Storage (CAS):**
  * **Hash Algorithm:** BLAKE3 (fast, cryptographic, parallelizable).
  * **Namespace:** Global CAS across all tenants. Chunks are shared, maximizing deduplication.
  * **Data Integrity:** BLAKE3 validation on write and during periodic background scrubs. Optional CRC32C checks on the hot read path.

* **Chunking & Deduplication:**
  * **Algorithm:** FastCDC (Content-Defined Chunking).
  * **Chunk Sizes:** 512 KB min, 2 MB average, 8 MB max.
  * **Deduplication Index:** BadgerDB (single-node) tracking chunk existence and reference counts. 
  * **Manifests:** File versions are represented by lightweight JSON manifests (ordered lists of Chunk IDs).

* **Tiering & Compression Strategy:**
  * **Hot Tier (Local NVMe/SSD):** No compression. Immediate access.
  * **Warm Tier (Object Store like MinIO):** Compressed using zstd level 1 on transition.
  * **Cold Tier (Deep Archive):** Compressed using zstd level 9 on transition.
  * **Compressibility Probe:** MIME sniff + 64 KB probe to skip compressing already-compressed formats (JPEG, MP4, etc.).
  * **Policy Engine:** "Tiered bucket" model based on access recency. A background scheduler runs periodic scans to queue migration jobs.

* **Versioning & Retention:**
  * Immutable versions using new chunk manifests.
  * Configurable retention policies (e.g., keep 5 versions, or per-path rules like 10 versions for `.xlsx`).

### 2.2 Service Architecture & Communication (Layer 2)
The backend logic layer, detailing microservices, event flows, and API handling.

* **API Gateway:**
  * The only internet-facing service.
  * Terminates TLS, handles JWT validation, and translates REST/SSE to gRPC.
  * Streams file uploads in 64KB frames (zero in-memory buffering).
  * Enforces rate limiting per user ID.

* **Auth / IAM Service (Includes SMTP Support):**
  * Manages users, roles, and the ACL table.
  * **SMTP Integration:** Handles local SMTP for **User Creation** (invitation emails, verifications), **Password Resets** (recovery links), and notifications related to **RBAC** (e.g., alerting users when their permission levels change).
  * **RBAC:** Cumulative bitmask permission levels: 0 (None), 1 (Read), 2 (Write), 3 (Read+Write), 4 (Download), 5 (Share).
  * Admin role bypasses the ACL table entirely.

* **Metadata Service:**
  * Backed by PostgreSQL.
  * Authoritative source for the file tree, file metadata, chunk manifests, version history, and Share Tokens. Does not hold actual file bytes.

* **Internal Services:**
  * **Chunk Engine:** Stores and retrieves chunk bytes.
  * **Tier Manager:** Executes the tier policy and chunk migrations.
  * **Compression Worker:** Stateless worker acting on NATS events.
  * **Sync Service:** Maintains device sync cursors and conflict records.
  * **Search Service:** Read-only projection of metadata for fast lookups.

* **Event Bus (NATS JetStream):**
  * Replaces Kafka. Handles async messaging (`file.created`, `chunk.stored`).
  * At-least-once delivery with consumer groups for background workers.

* **Observability & Deployment:**
  * OpenTelemetry (Traces, Metrics, Logs) pushed to Jaeger.
  * Deployed primarily as a single-binary mode for self-hosting, or via Docker Compose for medium-scale team setups.

### 2.3 Client Layer & External APIs (Layer 3)
The interfaces through which users and third-party tools interact with the platform.

* **Web Application:**
  * React + TypeScript.
  * Handles chunked multipart uploads from the browser, streaming downloads, and Server-Sent Events (SSE) for real-time UI updates without websockets.

* **Desktop Application:**
  * Rust using the `iced` GUI framework and `tokio`.
  * Two-pane file manager layout.
  * Maintains a local SQLite manifest to ensure the UI feels instant (zero network round trips for navigation).
  * Uses the `notify` crate for background folder watching and sync.

* **S3-Compatible API:**
  * Go endpoint translating S3 SDK calls (PutObject, GetObject, ListBuckets, multipart upload) to internal gRPC calls.
  * Uses AWS Signature V4 authentication, enabling compatibility with tools like `rclone` and `boto3`.

* **Share Links:**
  * Ephemeral or permanent presigned URLs managed via a `ShareToken` table in the Metadata service.
  * Resolves tokens to synthetic anonymous identities with strictly scoped read/download permissions.

---

## 3. Development Strategies & Phases

### Phase 1: Core Foundation & Storage Engine
- **Goal:** Get the core backend and storage mechanisms running.
- **Tasks:**
  - Setup Go monorepo, `buf` for protobuf generation, and Docker Compose environment.
  - Implement the API Gateway and Auth Service (including local SMTP integration for user creation/reset).
  - Implement CAS, FastCDC chunking, and the local Chunk Engine.
  - Implement PostgreSQL-backed Metadata Service (file trees, manifests).

### Phase 2: Asynchronous Workflows & Tiering
- **Goal:** Enable background processing, optimization, and storage tiering.
- **Tasks:**
  - Integrate NATS JetStream for event propagation (`chunk.stored`, etc.).
  - Implement Compression Worker (zstd/sniffing).
  - Implement Tier Manager (Scheduler, heat records, migration logic).
  - Implement pluggable backend interfaces (Local NVMe + MinIO/S3).

### Phase 3: Sync Protocol & Client Interfaces
- **Goal:** Build user-facing clients and establish robust synchronization.
- **Tasks:**
  - Build Rust desktop client UI (`iced`), local SQLite state, and folder watcher.
  - Implement the Bandwidth-Efficient Upload Protocol (client sends chunk hashes, server requests missing chunks).
  - Develop the React Web UI for browser access.
  - Implement Sync Service (cursors, conflict fork-and-surface).

### Phase 4: External APIs & Polish
- **Goal:** Add interoperability, sharing, and observability.
- **Tasks:**
  - Implement the S3-Compatible API wrapper.
  - Add Share Link generation and resolution (unauthenticated gateway routes).
  - Integrate OpenTelemetry and Jaeger tracing across all Go services.
  - Implement background Scrubber for continuous data integrity validation.

---

## 4. User Stories

### 4.1 End Users
- **Upload Resilience:** As a user, I want my large file uploads to be chunked and resumable so that network interruptions don't force me to start over.
- **Instant Desktop UI:** As a user, I want the desktop app to navigate my folders instantly using a local cache, syncing in the background without locking the UI.
- **Bandwidth Efficiency:** As a user editing a large document, I want the client to only upload the chunks that changed, saving bandwidth and time.
- **Seamless Tiering:** As a user, I want old files to automatically move to cheaper storage, but remain visible in my directory tree so I can access them seamlessly (even if it takes a moment to restore).
- **Secure Sharing:** As a user, I want to generate a shareable link with an expiration date so I can safely send a file to someone without an account.

### 4.2 System Administrators
- **Easy Deployment:** As an admin, I want to deploy the entire Erebus platform using a single binary or a simple Docker Compose file.
- **Storage Efficiency:** As an admin, I want global deduplication across all users to minimize the storage footprint of widely shared files.
- **Configurable Retention:** As an admin, I want to set retention policies (e.g., keep 5 versions of `.docx`, but only 1 version of `.mp4`) to control storage costs.
- **Auditable Integrity:** As an admin, I want a dashboard showing the status of background storage scrubs to ensure no data is suffering from bit rot.
- **Self-Contained Auth:** As an admin, I want user management and password resets to be handled entirely by the internal Auth service using a configurable SMTP server, without relying on third-party identity providers.
