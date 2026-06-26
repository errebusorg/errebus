# NimbusFS — Complete Project Specification

> **Version:** 1.0  
> **Date:** 2026-06-25  
> **Status:** Authoritative — this document is the single source of truth for all development.  
> **Scope:** Backend (Go), Desktop Client (Rust/iced), Storage Engine, API Layer, Authentication, Email System, Deployment.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Technology Stack](#2-technology-stack)
3. [Service Architecture Overview](#3-service-architecture-overview)
4. [Proto File Structure & Rules](#4-proto-file-structure--rules)
5. [Service 1: API Gateway](#5-service-1-api-gateway)
6. [Service 2: Auth / IAM Service](#6-service-2-auth--iam-service)
7. [Service 3: Metadata Service](#7-service-3-metadata-service)
8. [Service 4: Chunk Engine](#8-service-4-chunk-engine)
9. [Service 5: Tier Manager](#9-service-5-tier-manager)
10. [Service 6: Compression Worker](#10-service-6-compression-worker)
11. [Service 7: Sync Service](#11-service-7-sync-service)
12. [Service 8: Search Service](#12-service-8-search-service)
13. [Service 9: Email Service (SMTP)](#13-service-9-email-service-smtp)
14. [NATS JetStream Event Bus](#14-nats-jetstream-event-bus)
15. [RBAC & Permission System](#15-rbac--permission-system)
16. [Shareable Link System](#16-shareable-link-system)
17. [Storage Engine — Content-Addressed Storage](#17-storage-engine--content-addressed-storage)
18. [Chunking & Deduplication](#18-chunking--deduplication)
19. [Compression Strategy Per Tier](#19-compression-strategy-per-tier)
20. [Tier Policy Engine](#20-tier-policy-engine)
21. [Pluggable Storage Backends](#21-pluggable-storage-backends)
22. [Data Integrity](#22-data-integrity)
23. [Version Management & Retention](#23-version-management--retention)
24. [Upload Flow (End to End)](#24-upload-flow-end-to-end)
25. [Download Flow (End to End)](#25-download-flow-end-to-end)
26. [Sync Protocol](#26-sync-protocol)
27. [Desktop Client (Rust / iced)](#27-desktop-client-rust--iced)
28. [REST API Surface (Gateway)](#28-rest-api-surface-gateway)
29. [Observability](#29-observability)
30. [Deployment](#30-deployment)
31. [Docker Compose (Local Development)](#31-docker-compose-local-development)
32. [Resolved Design Decisions](#32-resolved-design-decisions)

---

## 1. Project Overview

**NimbusFS** is a self-hostable, polyglot (Go + Rust) cloud storage platform that provides S3-class storage intelligence — smart tiering, chunk-level deduplication, content-addressed storage — behind a first-class native desktop interface built in Rust with iced.

It is simultaneously:
- **A consumer product** — personal/organizational cloud storage with a polished native UI, background sync, and OS notifications.
- **A developer primitive** — exposes a clean REST/JSON API that any tool can integrate with.

### What Is In Scope (v1)

- Go across the entire backend (gateway, metadata, chunk engine, tier manager, auth, search, compression, sync, email).
- Rust for the desktop client with the `iced` GUI framework.
- NATS JetStream as the sole event bus.
- Three storage tiers (hot / warm / cold) with automatic policy-driven movement.
- A native desktop app as the primary client — no web frontend in v1.
- REST/JSON for client ↔ gateway communication.
- gRPC for all internal service-to-service communication.
- SMTP-based email for user account creation and password reset.
- No delta storage — CAS + CDC chunking provides inherent versioning.

### What Is Explicitly Out of Scope (v1)

- Web frontend (React + TypeScript) — deferred.
- S3-compatible API endpoint — deferred.
- WebDAV bridge, FUSE/kernel-level mount, mobile apps.
- ML-based features, analytics, anything requiring Python.
- Code signing for the desktop binary.
- System tray, autostart, or OS-level mount for the desktop client.

---

## 2. Technology Stack

| Concern | Technology | Version/Notes |
|---|---|---|
| Backend language | **Go** | All 9 services, all tooling |
| Desktop client language | **Rust** | `iced` with `tokio` feature flag |
| Internal RPC | **gRPC + Protobuf** | All service-to-service calls |
| Async messaging | **NATS JetStream** | Sole message broker; replaces Kafka entirely |
| HTTP layer | **net/http + chi router** | API Gateway only |
| gRPC translation | **grpc-gateway** (CRUD) + **hand-rolled** (streaming) | Hybrid approach |
| Proto tooling | **buf** | Lint, breaking-change detection, registry |
| Metadata database | **PostgreSQL 16** | Metadata + Auth/IAM service |
| Chunk index (single-node) | **BadgerDB v4** | Chunk Engine local index |
| Desktop local state | **SQLite** | Client-side manifest, sync state, chunk progress |
| Rate limiting | **golang.org/x/time/rate** (single gateway) / **go-redis-rate** (multi-replica) | Token bucket per user |
| Email sending | **SMTP** (net/smtp or go-mail/mail) | User creation confirmation, password reset |
| Observability | **OpenTelemetry** | Traces + metrics + logs |
| Trace backend | **Jaeger** | all-in-one image for local dev |
| Metrics backend | **Prometheus** | Scraping OTEL exporter |
| Logs backend | **Loki** (optional) | Structured JSON logs with trace correlation |
| Local dev orchestration | **Docker Compose** | All services + infra |
| Hash algorithm (CAS) | **BLAKE3** | `zeebo/blake3` Go package |
| Chunking algorithm | **FastCDC** | `jotfs/fastcdc-go` Go package |
| Compression | **zstd** | `klauspost/compress/zstd` Go package |
| File type detection | **gabriel-vasile/mimetype** | Magic byte sniffing for compressibility |
| File watching (client) | **notify** crate | Rust; abstracts inotify/FSEvents/ReadDirectoryChanges |
| HTTP client (desktop) | **reqwest** | Rust; async HTTP |
| Notifications (desktop) | **notify-rust** crate | Cross-platform OS notifications |

---

## 3. Service Architecture Overview

NimbusFS consists of **9 Go microservices** (8 original + 1 Email Service):

```
┌──────────────┐
│  Desktop     │◄──── REST/JSON + SSE ────►┌──────────────┐
│  Client      │                           │  API Gateway │
│  (Rust/iced) │                           │  (Go)        │
└──────────────┘                           └──────┬───────┘
                                                  │ gRPC
                   ┌──────────────────────────────┼───────────────────────────────┐
                   │                              │                               │
           ┌───────▼───────┐  ┌──────────────┐  ┌─▼─────────────┐  ┌────────────┐
           │ Auth/IAM      │  │ Metadata     │  │ Chunk Engine  │  │ Sync       │
           │ Service       │  │ Service      │  │               │  │ Service    │
           │ (Go)          │  │ (Go + PG)    │  │ (Go + Badger) │  │ (Go)       │
           └───────────────┘  └──────────────┘  └───────────────┘  └────────────┘
                   │                │                   │                │
                   │          ┌─────▼──────┐     ┌─────▼──────┐        │
                   │          │ Search     │     │ Compression│        │
                   │          │ Service    │     │ Worker     │        │
                   │          │ (Go)       │     │ (Go)       │        │
                   │          └────────────┘     └────────────┘        │
                   │                                                   │
           ┌───────▼───────┐                    ┌─────────────┐       │
           │ Email Service │                    │ Tier Manager│       │
           │ (Go + SMTP)   │                    │ (Go)        │       │
           └───────────────┘                    └─────────────┘       │
                                                                      │
                              ┌────────────────────────────────────────┘
                              │
                       ┌──────▼───────┐
                       │  NATS        │
                       │  JetStream   │
                       └──────────────┘
```

### Service Dependency Rules

- **API Gateway** is the only service exposed to the public internet.
- **Auth/IAM** is a leaf node — it never calls other services.
- **Email Service** is a leaf node — called by Auth/IAM; it never calls other services.
- No service's `internal/` package imports another service's `internal/` package.
- Services communicate only through gRPC clients (via generated code in `gen/`) or NATS events.
- Downstream services trust the gateway's JWT validation — they do not re-validate.

---

## 4. Proto File Structure & Rules

```
nimbus/
  proto/
    common/
      v1/
        types.proto          # FileID, ChunkID, UserID, DeviceID, Timestamp — shared types
        errors.proto         # Domain error codes (NimbusErrorCode enum)
    metadata/
      v1/
        metadata.proto       # File CRUD, upload session, share tokens, versions
    chunk/
      v1/
        chunk.proto          # StoreChunk, FetchChunk, GetChunkInfo, NegotiateUpload
    auth/
      v1/
        auth.proto           # Register, Login, ValidateToken, CheckPermission, GrantPermission, etc.
    tier/
      v1/
        tier.proto           # GetTierInfo, InitiateRestore, RestoreStatus
    sync/
      v1/
        sync.proto           # SyncSession (bidi stream), GetChanges, RegisterDevice
    email/
      v1/
        email.proto          # SendEmail (internal only — never exposed to clients)
    search/
      v1/
        search.proto         # SearchFiles, IndexFile
  gen/
    go/
      common/v1/
      metadata/v1/
      chunk/v1/
      auth/v1/
      tier/v1/
      sync/v1/
      email/v1/
      search/v1/
  internal/
    metadata/
      service.go             # imports gen/go/metadata/v1 and gen/go/common/v1 ONLY
      store.go
    chunk/
      service.go
    auth/
      service.go
    email/
      service.go
    tier/
      service.go
    sync/
      service.go
    search/
      service.go
    compression/
      worker.go
    gateway/
      handler.go
  cmd/
    metadata-svc/
      main.go
    chunk-svc/
      main.go
    auth-svc/
      main.go
    gateway/
      main.go
    tier-svc/
      main.go
    sync-svc/
      main.go
    search-svc/
      main.go
    email-svc/
      main.go
    compression-worker/
      main.go
```

### Hard Rules

1. `common/v1` is the **only** shared proto dependency. No service proto imports another service proto sideways.
2. Generated code lives in `gen/` — business logic never imports gen code transitively across service boundaries.
3. Each `cmd/` binary imports only its own `internal/` package.
4. No service's `internal/` package imports another service's `internal/` package.
5. Use `buf` (not raw `protoc`) for linting, breaking-change detection, and code generation.
6. Every proto message includes a comment describing its purpose.

---

## 5. Service 1: API Gateway

### Purpose

The only service exposed to the public internet. Terminates TLS, validates JWTs, translates REST/SSE to gRPC, enforces rate limiting. Has **no business logic** and **owns no persistent state**.

### Responsibilities

- Terminate TLS (via reverse proxy or directly)
- Validate JWT on every request (except `/share/{token}` routes and public auth routes)
- Extract `user_id` from JWT claims and propagate as gRPC metadata header `x-user-id`
- Translate incoming REST/JSON requests to gRPC calls on downstream services
- Handle streaming file uploads by piping HTTP request body to gRPC client-streaming in 64 KB frames (zero buffering in memory)
- Handle streaming file downloads by piping gRPC server-streaming response back to HTTP response
- Enforce per-user rate limiting (token bucket)
- Serve SSE connections for real-time client updates (subscribe to NATS subjects filtered by user)
- Set request-scoped deadlines on every incoming HTTP request — deadline cascades through all downstream gRPC calls

### Routing Strategy

| Route Type | Strategy |
|---|---|
| CRUD metadata endpoints | `grpc-gateway` auto-generated from proto annotations |
| File upload / download | Hand-rolled translation (streaming needs precise control) |
| Auth routes (`/auth/*`) | Hand-rolled; some routes skip JWT validation |
| Share routes (`/share/*`) | Hand-rolled; skip JWT validation entirely |
| SSE endpoint (`/events`) | Hand-rolled; holds open HTTP connection, forwards NATS events |

### Rate Limiting

- **Unit:** Per `user_id` (after auth), NOT per IP
- **Algorithm:** Token bucket via `golang.org/x/time/rate`
- **Default:** 100 requests/sec burst, 50 sustained (configurable via env)
- **Multi-replica:** Switch to `go-redis-rate` with a shared Redis instance
- **Unauthenticated routes:** Rate-limit by IP (for `/auth/login`, `/auth/register`, `/share/*`)

### Configuration (Environment Variables)

| Variable | Type | Default | Description |
|---|---|---|---|
| `GATEWAY_PORT` | int | `8080` | HTTP listen port |
| `GATEWAY_TLS_CERT` | string | `""` | Path to TLS certificate file |
| `GATEWAY_TLS_KEY` | string | `""` | Path to TLS key file |
| `GATEWAY_JWT_SECRET` | string | *required* | HMAC-SHA256 secret for JWT validation |
| `GATEWAY_RATE_LIMIT_BURST` | int | `100` | Max burst requests per user |
| `GATEWAY_RATE_LIMIT_SUSTAINED` | float | `50.0` | Sustained requests/sec per user |
| `AUTH_SERVICE_ADDR` | string | `auth-svc:9001` | gRPC address of Auth/IAM service |
| `METADATA_SERVICE_ADDR` | string | `metadata-svc:9002` | gRPC address of Metadata service |
| `CHUNK_SERVICE_ADDR` | string | `chunk-svc:9003` | gRPC address of Chunk Engine |
| `SYNC_SERVICE_ADDR` | string | `sync-svc:9004` | gRPC address of Sync service |
| `SEARCH_SERVICE_ADDR` | string | `search-svc:9005` | gRPC address of Search service |
| `TIER_SERVICE_ADDR` | string | `tier-svc:9006` | gRPC address of Tier Manager |
| `NATS_URL` | string | `nats://nats:4222` | NATS server URL (for SSE event forwarding) |
| `OTEL_EXPORTER_ENDPOINT` | string | `otel-collector:4318` | OTEL collector address |

### JWT Token Structure

```json
{
  "sub": "user_id (UUID)",
  "email": "user@example.com",
  "role": "user | admin",
  "iat": 1719340000,
  "exp": 1719426400,
  "jti": "unique-token-id (UUID)"
}
```

- **Algorithm:** HMAC-SHA256 (`HS256`)
- **Access token TTL:** 24 hours
- **Refresh token TTL:** 30 days
- Refresh tokens are stored hashed in the Auth/IAM database

---

## 6. Service 2: Auth / IAM Service

### Purpose

Owns all user identity, authentication, role management, and permission/ACL logic. This is a **leaf node** — it never calls other services (except Email Service for sending verification/reset emails).

### Database: PostgreSQL

#### Table: `users`

| Column | Type | Constraints | Description |
|---|---|---|---|
| `id` | `UUID` | `PRIMARY KEY, DEFAULT gen_random_uuid()` | Unique user identifier |
| `email` | `VARCHAR(255)` | `UNIQUE, NOT NULL` | User's email address (login identifier) |
| `username` | `VARCHAR(100)` | `UNIQUE, NOT NULL` | Display name / handle |
| `password_hash` | `VARCHAR(255)` | `NOT NULL` | bcrypt hash of password (cost factor 12) |
| `role` | `VARCHAR(20)` | `NOT NULL, DEFAULT 'user'` | `admin` or `user` |
| `email_verified` | `BOOLEAN` | `NOT NULL, DEFAULT false` | Whether email has been verified |
| `status` | `VARCHAR(20)` | `NOT NULL, DEFAULT 'pending_verification'` | `pending_verification`, `active`, `suspended`, `deleted` |
| `storage_quota_bytes` | `BIGINT` | `NOT NULL, DEFAULT 10737418240` | Storage quota in bytes (default 10 GB) |
| `storage_used_bytes` | `BIGINT` | `NOT NULL, DEFAULT 0` | Current storage consumed |
| `created_at` | `TIMESTAMPTZ` | `NOT NULL, DEFAULT NOW()` | Account creation timestamp |
| `updated_at` | `TIMESTAMPTZ` | `NOT NULL, DEFAULT NOW()` | Last profile update |
| `last_login_at` | `TIMESTAMPTZ` | `NULL` | Last successful login |
| `password_changed_at` | `TIMESTAMPTZ` | `NOT NULL, DEFAULT NOW()` | Last password change |

#### Table: `roles`

| Column | Type | Constraints | Description |
|---|---|---|---|
| `name` | `VARCHAR(20)` | `PRIMARY KEY` | Role name: `admin` or `user` |
| `description` | `TEXT` | `NOT NULL` | Human-readable role description |

**Seed data:** Two rows — `admin` ("Full system access, bypasses all ACL checks") and `user` ("Standard user, subject to per-file ACL").

#### Table: `acl`

| Column | Type | Constraints | Description |
|---|---|---|---|
| `id` | `UUID` | `PRIMARY KEY, DEFAULT gen_random_uuid()` | ACL entry identifier |
| `file_id` | `UUID` | `NOT NULL` | The file this permission applies to |
| `grantee_user_id` | `UUID` | `NOT NULL, FK → users.id` | The user receiving the permission |
| `permission_level` | `INT` | `NOT NULL, CHECK (0 <= val <= 5)` | Cumulative permission level (see §15) |
| `granted_by` | `UUID` | `NOT NULL, FK → users.id` | The user who granted this permission |
| `granted_at` | `TIMESTAMPTZ` | `NOT NULL, DEFAULT NOW()` | When permission was granted |

**Unique constraint:** `UNIQUE(file_id, grantee_user_id)` — one permission entry per user per file.

#### Table: `refresh_tokens`

| Column | Type | Constraints | Description |
|---|---|---|---|
| `id` | `UUID` | `PRIMARY KEY, DEFAULT gen_random_uuid()` | Token record identifier |
| `user_id` | `UUID` | `NOT NULL, FK → users.id ON DELETE CASCADE` | Owner of this refresh token |
| `token_hash` | `VARCHAR(255)` | `NOT NULL, UNIQUE` | SHA-256 hash of the refresh token value |
| `device_name` | `VARCHAR(255)` | `NULL` | Human-readable device label |
| `expires_at` | `TIMESTAMPTZ` | `NOT NULL` | Expiry timestamp |
| `created_at` | `TIMESTAMPTZ` | `NOT NULL, DEFAULT NOW()` | When this token was issued |
| `revoked_at` | `TIMESTAMPTZ` | `NULL` | Set on revocation; token rejected if non-null |

#### Table: `email_verification_tokens`

| Column | Type | Constraints | Description |
|---|---|---|---|
| `id` | `UUID` | `PRIMARY KEY, DEFAULT gen_random_uuid()` | Token record identifier |
| `user_id` | `UUID` | `NOT NULL, FK → users.id ON DELETE CASCADE` | User being verified |
| `token` | `VARCHAR(255)` | `NOT NULL, UNIQUE` | Cryptographically random token (32 bytes hex) |
| `expires_at` | `TIMESTAMPTZ` | `NOT NULL` | Expiry: 24 hours from creation |
| `used_at` | `TIMESTAMPTZ` | `NULL` | Set when the token is consumed |
| `created_at` | `TIMESTAMPTZ` | `NOT NULL, DEFAULT NOW()` | Creation timestamp |

#### Table: `password_reset_tokens`

| Column | Type | Constraints | Description |
|---|---|---|---|
| `id` | `UUID` | `PRIMARY KEY, DEFAULT gen_random_uuid()` | Token record identifier |
| `user_id` | `UUID` | `NOT NULL, FK → users.id ON DELETE CASCADE` | User requesting reset |
| `token` | `VARCHAR(255)` | `NOT NULL, UNIQUE` | Cryptographically random token (32 bytes hex) |
| `expires_at` | `TIMESTAMPTZ` | `NOT NULL` | Expiry: 1 hour from creation |
| `used_at` | `TIMESTAMPTZ` | `NULL` | Set when the token is consumed |
| `created_at` | `TIMESTAMPTZ` | `NOT NULL, DEFAULT NOW()` | Creation timestamp |

### gRPC RPCs

```protobuf
service AuthService {
  // --- Authentication ---
  rpc Register(RegisterRequest) returns (RegisterResponse);
  rpc VerifyEmail(VerifyEmailRequest) returns (VerifyEmailResponse);
  rpc Login(LoginRequest) returns (LoginResponse);
  rpc RefreshToken(RefreshTokenRequest) returns (RefreshTokenResponse);
  rpc Logout(LogoutRequest) returns (LogoutResponse);
  rpc ValidateToken(ValidateTokenRequest) returns (ValidateTokenResponse);

  // --- Password Management ---
  rpc RequestPasswordReset(RequestPasswordResetRequest) returns (RequestPasswordResetResponse);
  rpc ResetPassword(ResetPasswordRequest) returns (ResetPasswordResponse);
  rpc ChangePassword(ChangePasswordRequest) returns (ChangePasswordResponse);

  // --- User Management ---
  rpc GetUser(GetUserRequest) returns (GetUserResponse);
  rpc UpdateUser(UpdateUserRequest) returns (UpdateUserResponse);
  rpc ListUsers(ListUsersRequest) returns (ListUsersResponse);         // admin only
  rpc SuspendUser(SuspendUserRequest) returns (SuspendUserResponse);   // admin only
  rpc DeleteUser(DeleteUserRequest) returns (DeleteUserResponse);      // admin only
  rpc UpdateQuota(UpdateQuotaRequest) returns (UpdateQuotaResponse);   // admin only

  // --- Permission / ACL ---
  rpc CheckPermission(CheckPermissionRequest) returns (CheckPermissionResponse);
  rpc GrantPermission(GrantPermissionRequest) returns (GrantPermissionResponse);
  rpc RevokePermission(RevokePermissionRequest) returns (RevokePermissionResponse);
  rpc ListPermissions(ListPermissionsRequest) returns (ListPermissionsResponse);
}
```

### RPC Message Definitions

#### Register

```protobuf
message RegisterRequest {
  string email = 1;              // must be valid email format
  string username = 2;           // 3-100 chars, alphanumeric + underscores
  string password = 3;           // min 8 chars, must contain uppercase, lowercase, digit
}

message RegisterResponse {
  string user_id = 1;
  string message = 2;            // "Verification email sent to <email>"
}
```

**Flow:**
1. Validate email format, username uniqueness, password strength.
2. Hash password with bcrypt (cost 12).
3. Insert user row with `status = 'pending_verification'`, `email_verified = false`.
4. Generate a cryptographically random 32-byte token, insert into `email_verification_tokens` with 24h expiry.
5. Call **Email Service** via gRPC: `SendEmail(to: email, template: "email_verification", data: {username, verification_link})`.
6. Publish `user.created` to NATS.
7. Return `user_id` and confirmation message. **Do NOT return tokens here.**

#### VerifyEmail

```protobuf
message VerifyEmailRequest {
  string token = 1;              // the token from the verification email link
}

message VerifyEmailResponse {
  string message = 1;            // "Email verified successfully"
}
```

**Flow:**
1. Look up token in `email_verification_tokens`.
2. Check not expired (`expires_at > NOW()`), not already used (`used_at IS NULL`).
3. Set `used_at = NOW()` on the token row.
4. Update user: `email_verified = true`, `status = 'active'`.
5. Publish `user.verified` to NATS.

#### Login

```protobuf
message LoginRequest {
  string email = 1;
  string password = 2;
  string device_name = 3;        // optional — e.g., "MacBook Pro", "Desktop-Home"
}

message LoginResponse {
  string access_token = 1;       // JWT, 24h TTL
  string refresh_token = 2;      // opaque token, 30d TTL
  User user = 3;                 // user profile data
}
```

**Flow:**
1. Look up user by email.
2. If `status != 'active'` → return `PERMISSION_DENIED` with appropriate message:
   - `pending_verification` → "Please verify your email first"
   - `suspended` → "Account suspended, contact administrator"
   - `deleted` → "Account not found"
3. Compare password against `password_hash` using bcrypt.
4. If mismatch → return `UNAUTHENTICATED`.
5. Generate JWT access token (24h TTL) and opaque refresh token (30d TTL).
6. Hash refresh token with SHA-256, insert into `refresh_tokens` table.
7. Update `last_login_at = NOW()`.
8. Return both tokens and user profile.

#### RequestPasswordReset

```protobuf
message RequestPasswordResetRequest {
  string email = 1;
}

message RequestPasswordResetResponse {
  string message = 1;            // Always "If an account with that email exists, a reset link has been sent."
}
```

**Flow:**
1. Look up user by email.
2. **If user does not exist → still return success message** (prevent email enumeration).
3. If user exists:
   a. Invalidate all existing unused password reset tokens for this user (`UPDATE ... SET used_at = NOW()`).
   b. Generate cryptographically random 32-byte token, insert into `password_reset_tokens` with **1 hour** expiry.
   c. Call **Email Service** via gRPC: `SendEmail(to: email, template: "password_reset", data: {username, reset_link})`.
4. Return generic success message regardless of whether user exists.

#### ResetPassword

```protobuf
message ResetPasswordRequest {
  string token = 1;              // from the password reset email link
  string new_password = 2;       // min 8 chars, same validation as Register
}

message ResetPasswordResponse {
  string message = 1;            // "Password reset successfully"
}
```

**Flow:**
1. Look up token in `password_reset_tokens`.
2. Validate: not expired, not already used.
3. Mark token as used.
4. Hash `new_password` with bcrypt (cost 12).
5. Update user: `password_hash`, `password_changed_at = NOW()`.
6. **Revoke ALL existing refresh tokens** for this user (security: force re-login on all devices).
7. Publish `user.password_changed` to NATS.

#### ChangePassword (authenticated)

```protobuf
message ChangePasswordRequest {
  string current_password = 1;
  string new_password = 2;
}

message ChangePasswordResponse {
  string message = 1;
}
```

**Flow:**
1. Extract `user_id` from gRPC metadata (`x-user-id`).
2. Verify `current_password` against stored hash.
3. If mismatch → return `UNAUTHENTICATED`.
4. Hash `new_password`, update `password_hash` and `password_changed_at`.
5. Revoke all other refresh tokens for this user (except the current session).
6. Call **Email Service**: `SendEmail(template: "password_changed_notification", data: {username})`.
7. Publish `user.password_changed` to NATS.

#### CheckPermission

```protobuf
message CheckPermissionRequest {
  string user_id = 1;
  string file_id = 2;
  int32 required_level = 3;      // the minimum permission level needed
}

message CheckPermissionResponse {
  bool allowed = 1;
  int32 actual_level = 2;        // the user's actual permission level on this file
}
```

**Flow:**
1. Look up user's role.
2. If role = `admin` → return `{allowed: true, actual_level: 5}` (bypass ACL).
3. Query ACL table: `SELECT permission_level FROM acl WHERE file_id = ? AND grantee_user_id = ?`.
4. If no row → return `{allowed: false, actual_level: 0}`.
5. If `stored_level >= required_level` → return `{allowed: true, actual_level: stored_level}`.
6. Else → return `{allowed: false, actual_level: stored_level}`.

#### GrantPermission

```protobuf
message GrantPermissionRequest {
  string granter_user_id = 1;    // who is granting
  string grantee_user_id = 2;    // who is receiving
  string file_id = 3;
  int32 permission_level = 4;    // 1-5
}

message GrantPermissionResponse {
  string acl_id = 1;
}
```

**Flow:**
1. Check granter has level 5 (Share) on the file. If not → `PERMISSION_DENIED`.
2. Check `permission_level <= granter's own level`. Granter cannot escalate beyond their own access.
3. UPSERT into `acl` table.
4. Publish `acl.granted` to NATS: `{file_id, grantee_user_id, permission_level, granted_by}`.

### Configuration (Environment Variables)

| Variable | Type | Default | Description |
|---|---|---|---|
| `AUTH_GRPC_PORT` | int | `9001` | gRPC listen port |
| `AUTH_DB_DSN` | string | *required* | PostgreSQL connection string |
| `AUTH_JWT_SECRET` | string | *required* | HMAC-SHA256 signing secret (shared with gateway) |
| `AUTH_BCRYPT_COST` | int | `12` | bcrypt cost factor |
| `AUTH_ACCESS_TOKEN_TTL` | duration | `24h` | Access token lifetime |
| `AUTH_REFRESH_TOKEN_TTL` | duration | `720h` | Refresh token lifetime (30 days) |
| `AUTH_EMAIL_VERIFY_TTL` | duration | `24h` | Email verification token lifetime |
| `AUTH_PASSWORD_RESET_TTL` | duration | `1h` | Password reset token lifetime |
| `AUTH_DEFAULT_QUOTA_BYTES` | int64 | `10737418240` | Default storage quota (10 GB) |
| `EMAIL_SERVICE_ADDR` | string | `email-svc:9007` | gRPC address of Email service |
| `NATS_URL` | string | `nats://nats:4222` | NATS server URL |
| `OTEL_EXPORTER_ENDPOINT` | string | `otel-collector:4318` | OTEL collector address |

### NATS Events Published

| Subject | Payload Fields | Triggered By |
|---|---|---|
| `user.created` | `user_id, email, username, created_at` | Register |
| `user.verified` | `user_id, verified_at` | VerifyEmail |
| `user.password_changed` | `user_id, changed_at` | ResetPassword, ChangePassword |
| `user.suspended` | `user_id, suspended_by, suspended_at` | SuspendUser |
| `user.deleted` | `user_id, deleted_by, deleted_at` | DeleteUser |
| `acl.granted` | `file_id, grantee_user_id, permission_level, granted_by, granted_at` | GrantPermission |
| `acl.revoked` | `file_id, grantee_user_id, revoked_by, revoked_at` | RevokePermission |

---

## 7. Service 3: Metadata Service

### Purpose

Authoritative source for the **file namespace**. Owns the file tree, file metadata, chunk manifests, version history, and share tokens. Does NOT store chunk bytes — treats `ChunkID` as opaque identifiers.

### Database: PostgreSQL

#### Table: `files`

| Column | Type | Constraints | Description |
|---|---|---|---|
| `id` | `UUID` | `PRIMARY KEY, DEFAULT gen_random_uuid()` | Unique file identifier |
| `owner_id` | `UUID` | `NOT NULL, FK → users.id` | File owner |
| `parent_id` | `UUID` | `NULL, FK → files.id` | Parent directory (NULL = root) |
| `name` | `VARCHAR(255)` | `NOT NULL` | File or directory name |
| `path` | `TEXT` | `NOT NULL` | Full path (e.g., `/documents/report.pdf`) |
| `is_directory` | `BOOLEAN` | `NOT NULL, DEFAULT false` | Whether this is a directory |
| `size_bytes` | `BIGINT` | `NOT NULL, DEFAULT 0` | File size in bytes (0 for directories) |
| `mime_type` | `VARCHAR(255)` | `NULL` | Detected MIME type |
| `status` | `VARCHAR(20)` | `NOT NULL, DEFAULT 'ACTIVE'` | `UPLOADING`, `ACTIVE`, `DELETED`, `RESTORING` |
| `current_version` | `INT` | `NOT NULL, DEFAULT 1` | Current version number |
| `checksum` | `VARCHAR(64)` | `NULL` | BLAKE3 hash of the complete file content |
| `created_at` | `TIMESTAMPTZ` | `NOT NULL, DEFAULT NOW()` | Creation timestamp |
| `updated_at` | `TIMESTAMPTZ` | `NOT NULL, DEFAULT NOW()` | Last update |
| `deleted_at` | `TIMESTAMPTZ` | `NULL` | Soft-delete timestamp |

**Unique constraint:** `UNIQUE(parent_id, name, owner_id)` — no duplicate names in the same directory for the same owner.

**Index:** `CREATE INDEX idx_files_owner_path ON files(owner_id, path);`

#### Table: `file_versions`

| Column | Type | Constraints | Description |
|---|---|---|---|
| `id` | `UUID` | `PRIMARY KEY, DEFAULT gen_random_uuid()` | Version record ID |
| `file_id` | `UUID` | `NOT NULL, FK → files.id ON DELETE CASCADE` | Parent file |
| `version_number` | `INT` | `NOT NULL` | Sequential version (1, 2, 3, ...) |
| `size_bytes` | `BIGINT` | `NOT NULL` | File size at this version |
| `checksum` | `VARCHAR(64)` | `NOT NULL` | BLAKE3 hash of full file content at this version |
| `chunk_count` | `INT` | `NOT NULL` | Number of chunks in this version's manifest |
| `new_chunk_bytes` | `BIGINT` | `NOT NULL, DEFAULT 0` | Bytes stored that are unique to this version (for dedup stats) |
| `created_at` | `TIMESTAMPTZ` | `NOT NULL, DEFAULT NOW()` | When this version was created |
| `created_by` | `UUID` | `NOT NULL, FK → users.id` | Who uploaded this version |

**Unique constraint:** `UNIQUE(file_id, version_number)`

#### Table: `chunk_manifests`

| Column | Type | Constraints | Description |
|---|---|---|---|
| `id` | `UUID` | `PRIMARY KEY, DEFAULT gen_random_uuid()` | Manifest entry ID |
| `file_version_id` | `UUID` | `NOT NULL, FK → file_versions.id ON DELETE CASCADE` | The file version this chunk belongs to |
| `chunk_index` | `INT` | `NOT NULL` | Ordered position (0, 1, 2, ...) |
| `chunk_id` | `VARCHAR(64)` | `NOT NULL` | BLAKE3 hash of the chunk content (the CAS key) |
| `offset` | `BIGINT` | `NOT NULL` | Byte offset of this chunk within the file |
| `size_bytes` | `INT` | `NOT NULL` | Size of this chunk in bytes |

**Unique constraint:** `UNIQUE(file_version_id, chunk_index)`

**Index:** `CREATE INDEX idx_chunk_manifests_chunk_id ON chunk_manifests(chunk_id);`

#### Table: `upload_sessions`

| Column | Type | Constraints | Description |
|---|---|---|---|
| `id` | `UUID` | `PRIMARY KEY, DEFAULT gen_random_uuid()` | Session identifier |
| `file_id` | `UUID` | `NOT NULL, FK → files.id` | The file being uploaded |
| `user_id` | `UUID` | `NOT NULL, FK → users.id` | Uploader |
| `expected_size` | `BIGINT` | `NOT NULL` | Total expected file size |
| `expected_chunks` | `INT` | `NOT NULL` | Number of expected chunks |
| `received_chunks` | `INT` | `NOT NULL, DEFAULT 0` | Chunks received so far |
| `status` | `VARCHAR(20)` | `NOT NULL, DEFAULT 'IN_PROGRESS'` | `IN_PROGRESS`, `FINALIZED`, `EXPIRED`, `FAILED` |
| `created_at` | `TIMESTAMPTZ` | `NOT NULL, DEFAULT NOW()` | Session start |
| `expires_at` | `TIMESTAMPTZ` | `NOT NULL` | Session expiry (24h from creation) |
| `finalized_at` | `TIMESTAMPTZ` | `NULL` | When finalized |

#### Table: `share_tokens`

| Column | Type | Constraints | Description |
|---|---|---|---|
| `token_id` | `UUID` | `PRIMARY KEY, DEFAULT gen_random_uuid()` | Public-facing token identifier |
| `file_id` | `UUID` | `NOT NULL, FK → files.id` | The file being shared |
| `permission_level` | `INT` | `NOT NULL, CHECK (1 <= val <= 4)` | Capped: 1 (Read/View) or 4 (Download) are typical |
| `expiry` | `TIMESTAMPTZ` | `NULL` | Null = permanent until revoked |
| `created_by_user_id` | `UUID` | `NOT NULL, FK → users.id` | Must have level 5 on the file to create |
| `created_at` | `TIMESTAMPTZ` | `NOT NULL, DEFAULT NOW()` | Creation timestamp |
| `revoked_at` | `TIMESTAMPTZ` | `NULL` | Set on revocation; token rejected if non-null |
| `access_count` | `INT` | `NOT NULL, DEFAULT 0` | Number of times this link has been accessed |

### gRPC RPCs

```protobuf
service MetadataService {
  // --- File Operations ---
  rpc CreateFile(CreateFileRequest) returns (CreateFileResponse);
  rpc CreateDirectory(CreateDirectoryRequest) returns (CreateDirectoryResponse);
  rpc GetFile(GetFileRequest) returns (GetFileResponse);
  rpc ListDirectory(ListDirectoryRequest) returns (ListDirectoryResponse);
  rpc MoveFile(MoveFileRequest) returns (MoveFileResponse);
  rpc RenameFile(RenameFileRequest) returns (RenameFileResponse);
  rpc DeleteFile(DeleteFileRequest) returns (DeleteFileResponse);
  rpc GetFileStats(GetFileStatsRequest) returns (GetFileStatsResponse);

  // --- Upload Session ---
  rpc InitiateUpload(InitiateUploadRequest) returns (InitiateUploadResponse);
  rpc FinalizeUpload(FinalizeUploadRequest) returns (FinalizeUploadResponse);
  rpc AbortUpload(AbortUploadRequest) returns (AbortUploadResponse);

  // --- Version Management ---
  rpc ListVersions(ListVersionsRequest) returns (ListVersionsResponse);
  rpc GetVersion(GetVersionRequest) returns (GetVersionResponse);
  rpc RestoreVersion(RestoreVersionRequest) returns (RestoreVersionResponse);

  // --- Share Tokens ---
  rpc CreateShareToken(CreateShareTokenRequest) returns (CreateShareTokenResponse);
  rpc ResolveShareToken(ResolveShareTokenRequest) returns (ResolveShareTokenResponse);
  rpc RevokeShareToken(RevokeShareTokenRequest) returns (RevokeShareTokenResponse);
  rpc ListShareTokens(ListShareTokensRequest) returns (ListShareTokensResponse);

  // --- Search Support ---
  rpc GetChunkManifest(GetChunkManifestRequest) returns (GetChunkManifestResponse);
}
```

### Key RPC Message Definitions

#### InitiateUpload

```protobuf
message InitiateUploadRequest {
  string parent_path = 1;        // directory path (e.g., "/documents")
  string file_name = 2;          // e.g., "report.pdf"
  int64 file_size = 3;           // total file size in bytes
  string mime_type = 4;          // detected MIME type
  string user_id = 5;            // from gateway metadata
}

message InitiateUploadResponse {
  string upload_session_id = 1;
  string file_id = 2;
  int32 expected_chunk_count = 3;
  int64 chunk_size_avg = 4;      // suggested average chunk size (2MB)
}
```

#### FinalizeUpload

```protobuf
message FinalizeUploadRequest {
  string upload_session_id = 1;
  repeated ChunkRef chunks = 2;  // ordered list of chunk IDs and their metadata
  string file_checksum = 3;      // BLAKE3 of entire file content
}

message FinalizeUploadResponse {
  string file_id = 1;
  int32 version_number = 2;
  int64 new_storage_bytes = 3;   // only new bytes stored (after dedup)
  int64 total_file_size = 4;
}
```

### NATS Events Published

| Subject | Payload Fields | Triggered By |
|---|---|---|
| `file.created` | `file_id, owner_id, path, name, size_bytes, mime_type, version, created_at` | FinalizeUpload (new file) |
| `file.updated` | `file_id, owner_id, path, name, size_bytes, new_version, old_version, updated_at` | FinalizeUpload (existing file) |
| `file.deleted` | `file_id, owner_id, path, deleted_by, deleted_at` | DeleteFile |
| `file.moved` | `file_id, owner_id, old_path, new_path, moved_at` | MoveFile |
| `file.renamed` | `file_id, owner_id, old_name, new_name, renamed_at` | RenameFile |
| `share.created` | `file_id, token_id, permission_level, expiry, created_by` | CreateShareToken |
| `share.revoked` | `file_id, token_id, revoked_by` | RevokeShareToken |

### Configuration (Environment Variables)

| Variable | Type | Default | Description |
|---|---|---|---|
| `METADATA_GRPC_PORT` | int | `9002` | gRPC listen port |
| `METADATA_DB_DSN` | string | *required* | PostgreSQL connection string |
| `METADATA_UPLOAD_SESSION_TTL` | duration | `24h` | Upload session expiry |
| `METADATA_MAX_VERSIONS` | int | `5` | Default max versions to retain per file |
| `AUTH_SERVICE_ADDR` | string | `auth-svc:9001` | For permission checks |
| `NATS_URL` | string | `nats://nats:4222` | NATS server URL |
| `OTEL_EXPORTER_ENDPOINT` | string | `otel-collector:4318` | OTEL collector address |

---

## 8. Service 4: Chunk Engine

### Purpose

Owns **chunk bytes** and the **hash→location index**. Stores, retrieves, and deduplicates content-addressed chunks. Manages chunk reference counts for garbage collection.

### Local State: BadgerDB v4

#### Key-Value Schema (BadgerDB)

**Key:** `chunk_id` (32 bytes, BLAKE3 hash)

**Value:** JSON-encoded `ChunkMeta`:

```go
type ChunkMeta struct {
    Size              int               // chunk size in bytes
    Tier              string            // "hot", "warm", "cold"
    CompressionAlgo   string            // "none", "zstd1", "zstd9"
    RefCount          int32             // number of manifests referencing this chunk
    StoragePath       string            // relative path within the backend (e.g., "a1/b2c3d4...")
    CRC32C            uint32            // fast read-path integrity check
    CreatedAt         int64             // unix timestamp
    LastAccessedAt    int64             // unix timestamp, updated on read
    AccessCount       int64             // total lifetime reads
}
```

### gRPC RPCs

```protobuf
service ChunkService {
  // --- Upload ---
  rpc StoreChunk(stream StoreChunkRequest) returns (StoreChunkResponse);
  rpc NegotiateUpload(NegotiateUploadRequest) returns (NegotiateUploadResponse);

  // --- Download ---
  rpc FetchChunk(FetchChunkRequest) returns (stream FetchChunkResponse);

  // --- Index ---
  rpc GetChunkInfo(GetChunkInfoRequest) returns (GetChunkInfoResponse);
  rpc BatchGetChunkInfo(BatchGetChunkInfoRequest) returns (BatchGetChunkInfoResponse);

  // --- Reference Counting ---
  rpc IncrementRef(IncrementRefRequest) returns (IncrementRefResponse);
  rpc DecrementRef(DecrementRefRequest) returns (DecrementRefResponse);

  // --- Garbage Collection ---
  rpc RunGC(RunGCRequest) returns (RunGCResponse);                    // admin only

  // --- Tier Migration (called by Tier Manager) ---
  rpc MigrateChunk(MigrateChunkRequest) returns (MigrateChunkResponse);
}
```

### Key RPC Message Definitions

#### StoreChunk (client-streaming)

```protobuf
message StoreChunkRequest {
  oneof payload {
    StoreChunkHeader header = 1;
    bytes data = 2;               // 64KB frames
  }
}

message StoreChunkHeader {
  string chunk_id = 1;            // expected BLAKE3 hash
  int64 size = 2;                 // expected chunk size
  string mime_type = 3;           // for compressibility detection
}

message StoreChunkResponse {
  string chunk_id = 1;
  bool already_existed = 2;       // true if dedup — chunk was not re-stored
  string etag = 3;                // backend-returned integrity tag
}
```

**Flow:**
1. Receive header with expected `chunk_id` and size.
2. Check if `chunk_id` exists in BadgerDB. If yes → return `{already_existed: true}`, skip data frames.
3. Receive data frames (64KB each), stream to hot storage backend.
4. After all data received: recompute BLAKE3 hash.
5. If computed hash ≠ expected `chunk_id` → return `DATA_LOSS` error, delete temp file.
6. Store CRC32C alongside in BadgerDB for fast read-path verification.
7. Insert ChunkMeta into BadgerDB with `RefCount=0` (caller is responsible for incrementing).
8. Publish `chunk.stored` to NATS.

#### NegotiateUpload (bandwidth-efficient upload)

```protobuf
message NegotiateUploadRequest {
  repeated string chunk_ids = 1;  // client-computed BLAKE3 hashes
}

message NegotiateUploadResponse {
  repeated string missing = 1;    // chunks the server needs
  repeated string existing = 2;   // chunks the server already has
}
```

#### FetchChunk (server-streaming)

```protobuf
message FetchChunkRequest {
  string chunk_id = 1;
}

message FetchChunkResponse {
  oneof payload {
    FetchChunkHeader header = 1;
    bytes data = 2;               // 64KB frames
  }
}

message FetchChunkHeader {
  string chunk_id = 1;
  int64 size = 2;
  string tier = 3;                // current tier
  string compression = 4;        // "none", "zstd1", "zstd9" — client may need to decompress
}
```

**Flow:**
1. Look up `chunk_id` in BadgerDB → get `ChunkMeta`.
2. If tier = `cold` → return `UNAVAILABLE` with detail "chunk is on cold tier, initiate restore".
3. If tier = `warm` → decompress (zstd) before streaming.
4. Open reader from appropriate storage backend.
5. Optionally verify CRC32C on read path.
6. Stream back in 64KB frames.
7. Update `LastAccessedAt` and `AccessCount` in BadgerDB.

### NATS Events Published

| Subject | Payload Fields |
|---|---|
| `chunk.stored` | `chunk_id, size, tier, created_at` |
| `chunk.deleted` | `chunk_id, deleted_at, reason` |

### Configuration (Environment Variables)

| Variable | Type | Default | Description |
|---|---|---|---|
| `CHUNK_GRPC_PORT` | int | `9003` | gRPC listen port |
| `CHUNK_HOT_STORAGE_PATH` | string | `/data/hot` | Local filesystem path for hot tier |
| `CHUNK_WARM_STORAGE_ENDPOINT` | string | `minio:9000` | MinIO/S3 endpoint for warm tier |
| `CHUNK_WARM_STORAGE_BUCKET` | string | `nimbus-warm` | Warm tier bucket name |
| `CHUNK_WARM_ACCESS_KEY` | string | *required* | MinIO access key |
| `CHUNK_WARM_SECRET_KEY` | string | *required* | MinIO secret key |
| `CHUNK_COLD_STORAGE_ENDPOINT` | string | `""` | Cold tier endpoint (empty = disabled) |
| `CHUNK_COLD_STORAGE_BUCKET` | string | `nimbus-cold` | Cold tier bucket |
| `CHUNK_BADGER_PATH` | string | `/data/chunk-index` | BadgerDB data directory |
| `CHUNK_FRAME_SIZE` | int | `65536` | Streaming frame size (64KB) |
| `CHUNK_VERIFY_ON_READ` | bool | `true` | Whether to verify CRC32C on every read |
| `NATS_URL` | string | `nats://nats:4222` | NATS server URL |
| `OTEL_EXPORTER_ENDPOINT` | string | `otel-collector:4318` | OTEL collector address |

---

## 9. Service 5: Tier Manager

### Purpose

Owns **tier policy**, **tier assignment**, and **migration job scheduling**. Determines which tier each chunk should live on based on access patterns, and orchestrates chunk movement by delegating to the Chunk Engine.

### Database: PostgreSQL (shared instance, separate schema)

#### Table: `heat_records`

| Column | Type | Constraints | Description |
|---|---|---|---|
| `chunk_id` | `VARCHAR(64)` | `PRIMARY KEY` | The chunk being tracked |
| `current_tier` | `VARCHAR(10)` | `NOT NULL, DEFAULT 'hot'` | Current storage tier: `hot`, `warm`, `cold` |
| `last_access_at` | `TIMESTAMPTZ` | `NOT NULL, DEFAULT NOW()` | Last time this chunk was read |
| `access_count` | `BIGINT` | `NOT NULL, DEFAULT 0` | Total lifetime read count |
| `size_bytes` | `INT` | `NOT NULL` | Chunk size for migration cost estimation |
| `created_at` | `TIMESTAMPTZ` | `NOT NULL, DEFAULT NOW()` | When chunk was first registered |

#### Table: `migration_jobs`

| Column | Type | Constraints | Description |
|---|---|---|---|
| `id` | `UUID` | `PRIMARY KEY, DEFAULT gen_random_uuid()` | Job identifier |
| `chunk_id` | `VARCHAR(64)` | `NOT NULL` | Chunk to migrate |
| `source_tier` | `VARCHAR(10)` | `NOT NULL` | Current tier |
| `target_tier` | `VARCHAR(10)` | `NOT NULL` | Desired tier |
| `status` | `VARCHAR(20)` | `NOT NULL, DEFAULT 'PENDING'` | `PENDING`, `IN_PROGRESS`, `COMPLETED`, `FAILED` |
| `priority` | `INT` | `NOT NULL, DEFAULT 0` | Higher = more urgent (restore jobs get priority 10) |
| `error_message` | `TEXT` | `NULL` | Error details on failure |
| `created_at` | `TIMESTAMPTZ` | `NOT NULL, DEFAULT NOW()` | Job creation time |
| `started_at` | `TIMESTAMPTZ` | `NULL` | Processing start time |
| `completed_at` | `TIMESTAMPTZ` | `NULL` | Processing completion time |
| `retry_count` | `INT` | `NOT NULL, DEFAULT 0` | Number of retries attempted |

#### Table: `restore_jobs`

| Column | Type | Constraints | Description |
|---|---|---|---|
| `id` | `UUID` | `PRIMARY KEY, DEFAULT gen_random_uuid()` | Job identifier |
| `file_id` | `UUID` | `NOT NULL` | File being restored (maps to multiple chunks) |
| `requested_by` | `UUID` | `NOT NULL` | User who requested the restore |
| `status` | `VARCHAR(20)` | `NOT NULL, DEFAULT 'PENDING'` | `PENDING`, `IN_PROGRESS`, `READY`, `FAILED` |
| `total_chunks` | `INT` | `NOT NULL` | Total chunks to restore |
| `completed_chunks` | `INT` | `NOT NULL, DEFAULT 0` | Chunks restored so far |
| `created_at` | `TIMESTAMPTZ` | `NOT NULL, DEFAULT NOW()` | Request time |
| `ready_at` | `TIMESTAMPTZ` | `NULL` | When all chunks are ready |

### Tier Policy Configuration

```yaml
tier_policy:
  hot_to_warm_after: "168h"        # 7 days without access
  warm_to_cold_after: "720h"       # 30 days without access
  min_file_size_for_tiering: 1048576  # 1 MB — don't bother tiering tiny files
  scan_interval: "1h"             # how often the scheduler scans
  max_concurrent_migrations: 10    # semaphore limit
  restore_priority: 10            # priority for cold→hot restore jobs
```

### gRPC RPCs

```protobuf
service TierService {
  rpc GetTierInfo(GetTierInfoRequest) returns (GetTierInfoResponse);
  rpc InitiateRestore(InitiateRestoreRequest) returns (InitiateRestoreResponse);
  rpc GetRestoreStatus(GetRestoreStatusRequest) returns (GetRestoreStatusResponse);
  rpc UpdatePolicy(UpdatePolicyRequest) returns (UpdatePolicyResponse);         // admin only
  rpc GetPolicy(GetPolicyRequest) returns (GetPolicyResponse);
  rpc GetTierStats(GetTierStatsRequest) returns (GetTierStatsResponse);        // admin only
}
```

#### InitiateRestore

```protobuf
message InitiateRestoreRequest {
  string file_id = 1;
  string user_id = 2;
}

message InitiateRestoreResponse {
  string restore_job_id = 1;
  int32 estimated_seconds = 2;    // estimated time to restore
  string status = 3;              // "PENDING"
}
```

**Flow:**
1. Look up file's chunk manifest from Metadata Service.
2. Identify which chunks are on cold tier.
3. Create `restore_job` row.
4. For each cold chunk: create `migration_job` with `priority = 10` (high) and `target_tier = 'hot'`.
5. Return job ID with estimated restore time.
6. **Client polls** `GetRestoreStatus` until `status = 'READY'`.
7. Once all chunks migrated: update `restore_job.status = 'READY'`, publish `file.restored` to NATS.

### NATS Events Published

| Subject | Payload Fields |
|---|---|
| `tier.changed` | `chunk_id, old_tier, new_tier, changed_at` |
| `chunk.tiered` | `chunk_id, tier, migrated_at` |
| `file.restored` | `file_id, restore_job_id, restored_at` |

### NATS Events Consumed

| Subject | Action |
|---|---|
| `chunk.stored` | Register chunk in `heat_records` with tier=hot |
| `file.created`, `file.updated` | Update access timestamps for all chunks in manifest |

---

## 10. Service 6: Compression Worker

### Purpose

Stateless processor that compresses chunks during tier transitions. Consumes NATS events, compresses data, and writes compressed output back. Owns **no persistent state**.

### Behavior

1. **Consumes:** `chunk.tiered` events from NATS (when a chunk is scheduled for warm/cold migration).
2. **For each event:**
   a. Read chunk data from source tier via Chunk Engine.
   b. Detect compressibility:
      - **MIME sniff:** Check magic bytes using `gabriel-vasile/mimetype`. Skip if JPEG, PNG, MP4, ZIP, GZIP, etc.
      - **Compressibility probe:** Compress first 64KB with zstd-fastest. If ratio > 90% → skip.
   c. Apply compression algorithm based on target tier:
      - Warm → zstd level 1 (~500 MB/s, ~2.5:1 ratio)
      - Cold → zstd level 9 (~80 MB/s, ~3.5:1 ratio)
   d. Write compressed chunk to target tier via Chunk Engine.
   e. Update ChunkMeta in Chunk Engine with `CompressionAlgo` field.
3. **Publishes:** `chunk.compressed` to NATS.

### NATS Events

| Direction | Subject | Details |
|---|---|---|
| Consume | `chunk.tiered` | Trigger compression for the migrated chunk |
| Publish | `chunk.compressed` | `chunk_id, algorithm, original_size, compressed_size, ratio, compressed_at` |

### Compression Decision Matrix

| Input MIME Type | Target Tier | Action | Algorithm |
|---|---|---|---|
| `image/jpeg`, `image/png`, `image/webp`, `image/gif` | Any | Skip — already compressed | `none` |
| `video/mp4`, `video/webm`, `audio/mpeg` | Any | Skip | `none` |
| `application/zip`, `application/gzip`, `application/zstd` | Any | Skip | `none` |
| Any other, compressibility ratio < 90% | Warm | Compress | `zstd1` |
| Any other, compressibility ratio < 90% | Cold | Compress | `zstd9` |
| Any other, compressibility ratio >= 90% | Any | Skip | `none` |

### Configuration

| Variable | Type | Default | Description |
|---|---|---|---|
| `COMPRESSION_WORKER_CONCURRENCY` | int | `4` | Number of parallel compression goroutines |
| `COMPRESSION_PROBE_SIZE` | int | `65536` | Bytes to sample for compressibility (64KB) |
| `COMPRESSION_SKIP_RATIO` | float | `0.90` | Skip if compressed/original >= this ratio |
| `CHUNK_SERVICE_ADDR` | string | `chunk-svc:9003` | gRPC address of Chunk Engine |
| `NATS_URL` | string | `nats://nats:4222` | NATS server URL |

---

## 11. Service 7: Sync Service

### Purpose

Manages the synchronization protocol between desktop clients and the server. Owns device registrations, sync cursors, and conflict records. Does NOT own file state — reads from Metadata Service.

### Database: PostgreSQL (shared instance, separate schema)

#### Table: `devices`

| Column | Type | Constraints | Description |
|---|---|---|---|
| `id` | `UUID` | `PRIMARY KEY, DEFAULT gen_random_uuid()` | Device identifier |
| `user_id` | `UUID` | `NOT NULL, FK → users.id` | Owner |
| `device_name` | `VARCHAR(255)` | `NOT NULL` | Human-readable name (e.g., "MacBook Pro") |
| `os` | `VARCHAR(50)` | `NOT NULL` | Operating system (macOS, Windows, Linux) |
| `sync_cursor` | `BIGINT` | `NOT NULL, DEFAULT 0` | Monotonic sequence: "I have seen all events up to N" |
| `last_sync_at` | `TIMESTAMPTZ` | `NULL` | Last successful sync timestamp |
| `registered_at` | `TIMESTAMPTZ` | `NOT NULL, DEFAULT NOW()` | Registration time |
| `status` | `VARCHAR(20)` | `NOT NULL, DEFAULT 'active'` | `active`, `revoked` |

#### Table: `sync_conflicts`

| Column | Type | Constraints | Description |
|---|---|---|---|
| `id` | `UUID` | `PRIMARY KEY, DEFAULT gen_random_uuid()` | Conflict record ID |
| `file_id` | `UUID` | `NOT NULL` | The file with the conflict |
| `device_id` | `UUID` | `NOT NULL, FK → devices.id` | Device that detected the conflict |
| `server_version` | `INT` | `NOT NULL` | Server's version at conflict time |
| `client_version` | `INT` | `NOT NULL` | Client's base version when editing |
| `conflict_copy_path` | `TEXT` | `NOT NULL` | Path of the conflict copy created |
| `resolution` | `VARCHAR(20)` | `NOT NULL, DEFAULT 'UNRESOLVED'` | `UNRESOLVED`, `KEEP_SERVER`, `KEEP_CLIENT`, `KEEP_BOTH` |
| `resolved_at` | `TIMESTAMPTZ` | `NULL` | When resolved |
| `created_at` | `TIMESTAMPTZ` | `NOT NULL, DEFAULT NOW()` | When conflict was detected |

### gRPC RPCs

```protobuf
service SyncService {
  // --- Device Management ---
  rpc RegisterDevice(RegisterDeviceRequest) returns (RegisterDeviceResponse);
  rpc ListDevices(ListDevicesRequest) returns (ListDevicesResponse);
  rpc RevokeDevice(RevokeDeviceRequest) returns (RevokeDeviceResponse);

  // --- Sync Protocol ---
  rpc GetChanges(GetChangesRequest) returns (GetChangesResponse);
  rpc SyncSession(stream SyncMessage) returns (stream SyncMessage);   // bidirectional

  // --- Conflict Management ---
  rpc ListConflicts(ListConflictsRequest) returns (ListConflictsResponse);
  rpc ResolveConflict(ResolveConflictRequest) returns (ResolveConflictResponse);
}
```

#### GetChanges

```protobuf
message GetChangesRequest {
  string device_id = 1;
  int64 since_cursor = 2;         // "give me everything since this cursor"
}

message GetChangesResponse {
  repeated ChangeEvent changes = 1;
  int64 new_cursor = 2;           // advance to this after processing
}

message ChangeEvent {
  string event_type = 1;          // "file.created", "file.updated", "file.deleted", "file.moved", "file.renamed"
  string file_id = 2;
  string path = 3;
  string name = 4;
  int64 size_bytes = 5;
  int32 version = 6;
  int64 sequence = 7;             // monotonic NATS sequence number
  string timestamp = 8;           // RFC3339
}
```

### Conflict Resolution Strategy (v1)

**Fork-and-surface approach:**
1. Conflict detected when: client's base version < server's current version AND client has local modifications.
2. Create a conflict copy: `file.txt (conflict copy from DeviceName, YYYY-MM-DD).txt`.
3. Upload the conflict copy as a new file.
4. Insert `sync_conflicts` record with `resolution = 'UNRESOLVED'`.
5. Publish `sync.conflict` to NATS.
6. Desktop client surfaces the conflict in UI with resolution options.

### NATS Events

| Direction | Subject | Details |
|---|---|---|
| Consume | `file.created`, `file.updated`, `file.deleted`, `file.moved`, `file.renamed` | Build sync cursor event log |
| Publish | `sync.conflict` | `file_id, device_id, server_version, client_version, conflict_copy_path` |

---

## 12. Service 8: Search Service

### Purpose

Maintains a search index over file metadata. Consumes file events from NATS and indexes file names, paths, MIME types, and metadata. Provides **filename and metadata search** only (not full-text content search in v1).

### Index Store

- **v1:** In-memory inverted index backed by a persistent file (rebuilt on startup from Metadata Service if lost).
- **Future:** Migrate to Bleve or Meilisearch for full-text search.

### Search Index Entry

```go
type SearchEntry struct {
    FileID    string
    OwnerID   string
    Path      string
    Name      string
    MIMEType  string
    SizeBytes int64
    Tier      string
    Tags      []string    // user-defined tags (future)
    UpdatedAt time.Time
}
```

### gRPC RPCs

```protobuf
service SearchService {
  rpc SearchFiles(SearchFilesRequest) returns (SearchFilesResponse);
  rpc IndexFile(IndexFileRequest) returns (IndexFileResponse);        // internal, called from NATS consumer
  rpc RemoveFromIndex(RemoveFromIndexRequest) returns (RemoveFromIndexResponse);
}
```

#### SearchFiles

```protobuf
message SearchFilesRequest {
  string user_id = 1;             // only return files the user can access
  string query = 2;               // search query string
  string mime_type_filter = 3;    // optional: filter by MIME type
  int32 page = 4;                 // pagination page (1-indexed)
  int32 page_size = 5;            // items per page (default 20, max 100)
  string sort_by = 6;             // "name", "size", "updated_at" (default: relevance)
  string sort_order = 7;          // "asc" or "desc"
}

message SearchFilesResponse {
  repeated SearchResult results = 1;
  int32 total_count = 2;
  int32 page = 3;
  int32 page_size = 4;
}

message SearchResult {
  string file_id = 1;
  string name = 2;
  string path = 3;
  string mime_type = 4;
  int64 size_bytes = 5;
  string tier = 6;
  float relevance_score = 7;
  string updated_at = 8;
}
```

### NATS Events Consumed

| Subject | Action |
|---|---|
| `file.created` | Add to search index |
| `file.updated` | Update search index entry |
| `file.deleted` | Remove from search index |
| `file.renamed` | Update name in index |
| `file.moved` | Update path in index |

---

## 13. Service 9: Email Service (SMTP)

### Purpose

Handles all outbound email via SMTP. Called internally by Auth/IAM Service. This is a **leaf node** — it never calls other services and does not store persistent state (beyond SMTP connection pooling).

### gRPC RPCs

```protobuf
service EmailService {
  rpc SendEmail(SendEmailRequest) returns (SendEmailResponse);
  rpc SendBatchEmail(SendBatchEmailRequest) returns (SendBatchEmailResponse);  // admin notifications
}
```

#### SendEmail

```protobuf
message SendEmailRequest {
  string to_email = 1;            // recipient email address
  string to_name = 2;             // recipient display name (optional)
  string template = 3;            // template identifier
  map<string, string> data = 4;   // template variables
}

message SendEmailResponse {
  bool success = 1;
  string message_id = 2;          // SMTP message ID for tracking
  string error = 3;               // error message if failed
}
```

### Email Templates

#### Template: `email_verification`

- **Subject:** "NimbusFS — Verify your email address"
- **Variables:** `username`, `verification_link`, `expiry_hours`
- **Body (plain text + HTML):**
  ```
  Hi {{username}},

  Welcome to NimbusFS! Please verify your email address by clicking the link below:

  {{verification_link}}

  This link expires in {{expiry_hours}} hours.

  If you didn't create an account, you can safely ignore this email.
  ```

#### Template: `password_reset`

- **Subject:** "NimbusFS — Reset your password"
- **Variables:** `username`, `reset_link`, `expiry_minutes`
- **Body:**
  ```
  Hi {{username}},

  We received a request to reset your password. Click the link below:

  {{reset_link}}

  This link expires in {{expiry_minutes}} minutes.

  If you didn't request this, you can safely ignore this email.
  Your password will not be changed unless you click the link and create a new one.
  ```

#### Template: `password_changed_notification`

- **Subject:** "NimbusFS — Your password was changed"
- **Variables:** `username`, `changed_at`, `ip_address`
- **Body:**
  ```
  Hi {{username}},

  Your NimbusFS password was changed on {{changed_at}}.

  If you did not make this change, please contact your system administrator immediately.
  ```

#### Template: `storage_quota_warning`

- **Subject:** "NimbusFS — Storage quota warning"
- **Variables:** `username`, `used_percent`, `used_gb`, `total_gb`
- **Body:**
  ```
  Hi {{username}},

  You have used {{used_percent}}% of your storage quota ({{used_gb}} GB of {{total_gb}} GB).

  Consider removing unused files or contact your administrator to increase your quota.
  ```

### SMTP Configuration

| Variable | Type | Default | Description |
|---|---|---|---|
| `EMAIL_GRPC_PORT` | int | `9007` | gRPC listen port |
| `SMTP_HOST` | string | *required* | SMTP server hostname (e.g., `smtp.gmail.com`) |
| `SMTP_PORT` | int | `587` | SMTP server port (587 for STARTTLS, 465 for implicit TLS) |
| `SMTP_USERNAME` | string | *required* | SMTP authentication username |
| `SMTP_PASSWORD` | string | *required* | SMTP authentication password |
| `SMTP_FROM_EMAIL` | string | *required* | Sender email address (e.g., `noreply@nimbus.example.com`) |
| `SMTP_FROM_NAME` | string | `NimbusFS` | Sender display name |
| `SMTP_TLS_MODE` | string | `starttls` | `starttls` (port 587) or `tls` (port 465) or `none` (port 25, dev only) |
| `SMTP_CONNECTION_POOL_SIZE` | int | `5` | Number of persistent SMTP connections |
| `SMTP_RETRY_COUNT` | int | `3` | Retries on transient SMTP errors |
| `SMTP_RETRY_DELAY` | duration | `5s` | Delay between retries |
| `EMAIL_BASE_URL` | string | *required* | Base URL for verification/reset links (e.g., `https://nimbus.example.com`) |
| `OTEL_EXPORTER_ENDPOINT` | string | `otel-collector:4318` | OTEL collector address |

### Implementation Notes

- Use Go's `net/smtp` package or `go-mail/mail` for SMTP operations.
- Templates are compiled at startup using Go's `text/template` and `html/template` packages.
- Both plain-text and HTML versions of each email are sent (multipart/alternative).
- Connection pooling: maintain a pool of authenticated SMTP connections to avoid per-email handshake overhead.
- **Rate limiting:** Max 10 emails/minute per user to prevent abuse of password reset and verification endpoints.
- **Verification/Reset link format:**
  - Verification: `{{EMAIL_BASE_URL}}/auth/verify?token={{token}}`
  - Password reset: `{{EMAIL_BASE_URL}}/auth/reset-password?token={{token}}`

---

## 14. NATS JetStream Event Bus

### Stream Configuration

Single JetStream stream capturing all subjects:

```
Stream Name:    NIMBUS_EVENTS
Subjects:       nimbus.>
Retention:      WorkQueue (messages removed after ack)
Max Age:        7 days
Max Bytes:      10 GB
Replicas:       1 (single node), 3 (clustered)
Discard:        Old (discard oldest when full)
Dedup Window:   2 minutes
Storage:        File
```

### Complete Subject Registry

| Subject | Publisher | Payload Fields |
|---|---|---|
| `file.created` | Metadata Service | `event_id, event_type, timestamp, correlation_id, file_id, owner_id, path, name, size_bytes, mime_type, version, created_at` |
| `file.updated` | Metadata Service | `event_id, event_type, timestamp, correlation_id, file_id, owner_id, path, size_bytes, new_version, old_version, updated_at` |
| `file.deleted` | Metadata Service | `event_id, event_type, timestamp, correlation_id, file_id, owner_id, path, deleted_by, deleted_at` |
| `file.moved` | Metadata Service | `event_id, event_type, timestamp, correlation_id, file_id, owner_id, old_path, new_path, moved_at` |
| `file.renamed` | Metadata Service | `event_id, event_type, timestamp, correlation_id, file_id, owner_id, old_name, new_name, renamed_at` |
| `file.restored` | Tier Manager | `event_id, event_type, timestamp, correlation_id, file_id, restore_job_id, restored_at` |
| `chunk.stored` | Chunk Engine | `event_id, event_type, timestamp, correlation_id, chunk_id, size, tier, created_at` |
| `chunk.compressed` | Compression Worker | `event_id, event_type, timestamp, correlation_id, chunk_id, algorithm, original_size, compressed_size, ratio` |
| `chunk.tiered` | Tier Manager | `event_id, event_type, timestamp, correlation_id, chunk_id, old_tier, new_tier` |
| `chunk.deleted` | Chunk Engine | `event_id, event_type, timestamp, correlation_id, chunk_id, reason` |
| `tier.changed` | Tier Manager | `event_id, event_type, timestamp, correlation_id, chunk_id, old_tier, new_tier` |
| `sync.conflict` | Sync Service | `event_id, event_type, timestamp, correlation_id, file_id, device_id, server_version, client_version, conflict_copy_path` |
| `user.created` | Auth/IAM Service | `event_id, event_type, timestamp, correlation_id, user_id, email, username` |
| `user.verified` | Auth/IAM Service | `event_id, event_type, timestamp, correlation_id, user_id` |
| `user.password_changed` | Auth/IAM Service | `event_id, event_type, timestamp, correlation_id, user_id` |
| `user.suspended` | Auth/IAM Service | `event_id, event_type, timestamp, correlation_id, user_id, suspended_by` |
| `user.deleted` | Auth/IAM Service | `event_id, event_type, timestamp, correlation_id, user_id, deleted_by` |
| `acl.granted` | Auth/IAM Service | `event_id, event_type, timestamp, correlation_id, file_id, grantee_user_id, permission_level, granted_by` |
| `acl.revoked` | Auth/IAM Service | `event_id, event_type, timestamp, correlation_id, file_id, grantee_user_id, revoked_by` |
| `share.created` | Metadata Service | `event_id, event_type, timestamp, correlation_id, file_id, token_id, permission_level, expiry, created_by` |
| `share.revoked` | Metadata Service | `event_id, event_type, timestamp, correlation_id, file_id, token_id, revoked_by` |

### Event Envelope (All Events)

Every event published to NATS includes these mandatory fields:

```json
{
  "event_id": "UUID — unique event identifier",
  "event_type": "string — matches NATS subject (e.g., file.created)",
  "timestamp": "RFC3339 — when the event was produced",
  "correlation_id": "UUID — links to OTel trace for distributed tracing",
  "payload": { /* domain-specific fields */ }
}
```

### Consumer Configuration

| Consumer Name | Subscribed Subjects | Type | Purpose |
|---|---|---|---|
| `search-indexer` | `file.created`, `file.updated`, `file.deleted`, `file.renamed`, `file.moved` | Pull, durable | Index files for search |
| `compression-worker` | `chunk.stored` | Pull, durable | Compress newly stored chunks based on tier |
| `tier-updater` | `chunk.stored` | Pull, durable | Register chunks in heat records |
| `sync-event-log` | `file.*` | Pull, durable | Build event log for sync cursors |
| `gateway-sse` | `file.*`, `sync.conflict`, `file.restored` | Push | Forward to SSE clients in real-time |
| `audit-logger` | `acl.*`, `share.*`, `user.*` | Pull, durable | Audit trail logging |

### Delivery Guarantees

- **At-least-once delivery:** Every consumer must be idempotent (upsert, not blind insert).
- **Pull consumers:** All background workers use pull consumers for backpressure control.
- **Durable consumers:** Survive service restarts.
- **AckWait:** 30 seconds. If a pod dies without acking, NATS redelivers.
- **Publish-side dedup:** Pass `Nats-Msg-Id` header using the domain entity ID (e.g., `chunk_id` for `chunk.stored`).

---

## 15. RBAC & Permission System

### Permission Levels (Cumulative)

| Level | Name | Capabilities | Includes |
|---|---|---|---|
| 0 | None | No access | — |
| 1 | Read | View file content, see metadata | — |
| 2 | Write | Modify file content (upload new version) | — |
| 3 | Read + Write | View and modify | Level 1 + 2 |
| 4 | Download | Download file to local machine | Level 1 + 2 + 3 |
| 5 | Share | Create share links, grant access to others | Level 1 + 2 + 3 + 4 |
| Admin | System role | All permissions on all files, bypasses ACL entirely | — |

### Permission Check Algorithm

```
function CheckPermission(user_id, file_id, required_level):
    user = GetUser(user_id)
    if user.role == "admin":
        return ALLOWED     // admins bypass ACL entirely

    acl_entry = QueryACL(file_id, user_id)
    if acl_entry is NULL:
        return DENIED       // no ACL entry = no access

    if acl_entry.permission_level >= required_level:
        return ALLOWED
    else:
        return DENIED
```

### Grant / Revoke Rules

1. Only users with level 5 (Share) on a file can grant access to others.
2. A user can only grant **up to their own permission level** — cannot escalate.
3. Admins can grant and revoke any permission on any file.
4. On revocation, the ACL row is deleted.
5. **File owner** automatically has level 5 on all files they own (enforced by Metadata Service, not ACL table).

### Required Permission Levels Per Operation

| Operation | Required Level |
|---|---|
| View file metadata | 1 (Read) |
| View file content | 1 (Read) |
| List directory | 1 (Read) |
| Upload new version | 2 (Write) |
| Rename file | 2 (Write) |
| Move file | 2 (Write) |
| Download file | 4 (Download) |
| Create share link | 5 (Share) |
| Grant permission to another user | 5 (Share) |
| Revoke permission | 5 (Share) or admin |
| Delete file | Owner or admin only |

---

## 16. Shareable Link System

### Share Token Flow (End to End)

1. **Create:** Authenticated user with level 5 sends `POST /files/{file_id}/share`. Gateway calls `Metadata.CreateShareToken(file_id, permission_level, expiry)`.
2. **Response:** Returns `{token_id, share_url}` where `share_url = https://nimbus.example.com/share/{token_id}`.
3. **Access:** External user hits `GET /share/{token_id}`.
4. Gateway detects `/share/` prefix → **skips JWT validation**.
5. Gateway calls `Metadata.ResolveShareToken(token_id)` → returns `{file_id, permission_level, expiry, revoked_at}`.
6. Gateway checks: not expired AND not revoked.
7. Gateway creates **synthetic identity:** `{user_id: "anonymous", permission_level: token.permission_level}`.
8. Gateway calls `Metadata.GetFile(file_id)` with synthetic identity.
9. If `permission_level >= 4` (Download): serve with `Content-Disposition: attachment`.
10. If `permission_level == 1` (Read only): serve minimal HTML page with file preview.
11. Increment `access_count` on the share token.

### API Routes

| Method | Path | Auth | Description |
|---|---|---|---|
| `POST` | `/files/{file_id}/share` | Required (level 5) | Create a share link |
| `GET` | `/share/{token_id}` | **None** | Access shared file |
| `DELETE` | `/files/{file_id}/share/{token_id}` | Required (level 5) | Revoke a share link |
| `GET` | `/files/{file_id}/shares` | Required (level 5) | List all share links for a file |

---

## 17. Storage Engine — Content-Addressed Storage

### Hash Function: BLAKE3

- **Package:** `github.com/zeebo/blake3`
- **Output:** 256-bit (32 bytes)
- **Speed:** ~10 GB/s on modern CPU with SIMD
- **Why not SHA-256:** BLAKE3 is 3–4× faster on large files with no security downside.

### Object ID

```go
type ObjectID [32]byte

func (id ObjectID) String() string {
    return hex.EncodeToString(id[:])
}

// StoragePath maps hash to filesystem path: "a1/b2c3d4..."
func (id ObjectID) StoragePath() string {
    s := id.String()
    return s[:2] + "/" + s[2:]
}
```

### CAS Namespace

- **Scope:** Global CAS — single dedup pool across all users.
- **Rationale:** NimbusFS is self-hosted for personal/organizational use; cross-user dedup is acceptable.
- **Security note:** With global CAS, a user could theoretically detect whether another user stored the same file (instant upload = chunk exists). For single-organization deployments, this is acceptable.
- **Future consideration:** If multi-tenant isolation is needed, add per-tenant encryption keys before hashing (breaks cross-tenant dedup but eliminates information leak).

### Immutability Rule

CAS objects are **strictly immutable**. Once a chunk is written with a given ObjectID, it is never overwritten. Deletion happens through reference counting and garbage collection.

---

## 18. Chunking & Deduplication

### Algorithm: FastCDC (Content-Defined Chunking)

- **Package:** `github.com/jotfs/fastcdc-go`
- **Min chunk size:** 512 KB
- **Average chunk size:** 2 MB
- **Max chunk size:** 8 MB

### Why FastCDC Over Fixed-Size Chunking

Fixed-size chunking has the **boundary shift problem**: inserting 1 byte at the start of a file shifts every chunk boundary, producing completely different hashes and zero dedup. FastCDC cuts on content-defined boundaries (rolling hash), so insertions only affect nearby chunks.

### Deduplication Flow

1. Client splits file into chunks locally using FastCDC.
2. Client computes BLAKE3 hash of each chunk.
3. Client sends chunk hashes to server: `ChunkEngine.NegotiateUpload(chunk_ids)`.
4. Server responds with which chunks are `missing` vs `existing`.
5. Client uploads only `missing` chunks — 60–90% bandwidth savings on modified files.
6. Server verifies each uploaded chunk's hash matches the expected chunk_id.

### Chunk Index (BadgerDB)

Per chunk entry stored in BadgerDB:

```go
Key:   chunk_id (32 bytes)
Value: ChunkMeta {
    Size, Tier, CompressionAlgo, RefCount,
    StoragePath, CRC32C, CreatedAt, LastAccessedAt, AccessCount
}
```

### Garbage Collection

- Each chunk has a `RefCount` tracking how many file version manifests reference it.
- When a file version is deleted (by retention policy or user action), `RefCount` is decremented for all its chunks.
- When `RefCount` reaches 0, the chunk is eligible for GC.
- GC runs as a background sweep (triggered by admin or on a schedule).
- **Safety:** The GC sweep double-checks ref counts before deletion to prevent race conditions.

---

## 19. Compression Strategy Per Tier

| Tier | Algorithm | Compression Level | When Applied | Typical Ratio |
|---|---|---|---|---|
| Hot | None | — | — | 1:1 |
| Warm | zstd | Level 1 (~500 MB/s) | On tier transition (hot → warm) | 2.5–3:1 |
| Cold | zstd | Level 9 (~80 MB/s) | On tier transition (warm → cold) | 3.5–4:1 |

### Compressibility Detection (Two-Stage)

1. **MIME sniff:** Check magic bytes. Skip compression for: JPEG, PNG, WebP, GIF, MP4, WebM, MPEG, ZIP, GZIP, ZSTD.
2. **Probe:** Compress first 64KB with zstd-fastest. If `compressed_size / original_size >= 0.90`, skip.

### Metadata

Each chunk's `ChunkMeta` records which compression algorithm was applied. On read, the system knows how to decompress.

---

## 20. Tier Policy Engine

### Tier Model

| Tier | Storage Backend | Access Latency | Compression | Use Case |
|---|---|---|---|---|
| **Hot** | Local NVMe/SSD | Immediate (< 1ms) | None | Actively used files |
| **Warm** | MinIO / S3-compatible | Seconds (~100ms–2s) | zstd level 1 | Infrequently accessed |
| **Cold** | Deep archive (MinIO/B2) | Minutes to hours | zstd level 9 | Rarely accessed / archive |

### Transition Policy (Tiered Bucket Model)

```yaml
hot_to_warm:   168h (7 days) without access
warm_to_cold:  720h (30 days) without access
min_size:      1 MB (don't tier tiny files)
```

### Cold File Restore Flow

1. User requests a cold file → Gateway calls `TierManager.InitiateRestore(file_id)`.
2. Tier Manager creates restore job, enqueues high-priority migration jobs for all cold chunks.
3. Gateway returns `HTTP 202 Accepted` with `Retry-After` header and `restore_job_id`.
4. Client polls `GET /files/{file_id}/restore-status`.
5. Once all chunks migrated to hot → Tier Manager publishes `file.restored` to NATS.
6. Client receives SSE event → re-requests the file.

### Scheduler

- Runs as a background goroutine in the Tier Manager service.
- Scans `heat_records` table every 1 hour.
- Evaluates each chunk against the tier policy.
- Enqueues `migration_jobs` for chunks that need to move.
- Concurrency limited by semaphore (default: 10 parallel migrations).

---

## 21. Pluggable Storage Backends

### Backend Interface

```go
type Backend interface {
    Put(ctx context.Context, key string, r io.Reader, size int64) (etag string, err error)
    Get(ctx context.Context, key string) (io.ReadCloser, ObjectMeta, error)
    GetRange(ctx context.Context, key string, start, end int64) (io.ReadCloser, error)
    Stat(ctx context.Context, key string) (ObjectMeta, error)
    Delete(ctx context.Context, key string) error
    Exists(ctx context.Context, key string) (bool, error)
    List(ctx context.Context, prefix string) ([]string, error)
}
```

### ObjectMeta

```go
type ObjectMeta struct {
    Size         int64
    ETag         string
    LastModified time.Time
    ContentType  string
}
```

### Implementations

| Backend | Tier | Implementation |
|---|---|---|
| `LocalBackend` | Hot | Local filesystem with atomic writes (temp file + rename) |
| `MinIOBackend` | Warm, Cold | MinIO Go SDK (`minio/minio-go/v7`) |
| `ArchiveBackend` | Cold (cloud) | Extends Backend with `InitiateRestore`/`RestoreStatus` for async retrieval |

### Optional Extension: MultipartUploader

```go
type MultipartUploader interface {
    InitiateMultipart(ctx context.Context, key string) (uploadID string, err error)
    UploadPart(ctx context.Context, key, uploadID string, partNum int, r io.Reader, size int64) (etag string, err error)
    CompleteMultipart(ctx context.Context, key, uploadID string, parts []CompletedPart) error
    AbortMultipart(ctx context.Context, key, uploadID string) error
}
```

Checked via type assertion: `if mp, ok := backend.(MultipartUploader); ok { ... }`

---

## 22. Data Integrity

### Verification Matrix

| Stage | Check | Algorithm | When |
|---|---|---|---|
| After chunk write | Recompute hash, compare to ObjectID | BLAKE3 | Every write |
| Read path (fast) | Verify stored CRC32C | CRC32C (~20 GB/s with HW) | Every read (configurable) |
| Periodic scrub | Recompute full hash of all chunks | BLAKE3 | Weekly background job |
| After tier migration | Verify hash at destination before deleting source | BLAKE3 | Every migration |
| Upload finalization | Verify full file hash matches client-provided checksum | BLAKE3 | Every upload |

### Scrubber

- Background goroutine that walks all chunks and re-verifies BLAKE3 hashes.
- Catches silent corruption (bit rot, cosmic rays).
- Configurable schedule (default: weekly).
- On corruption detected: log ERROR, attempt repair from another tier if chunk exists on multiple tiers.
- Exposes stats via the Integrity Dashboard API.

### Integrity Dashboard API

```
GET /api/v1/admin/integrity

{
  "last_scrub": "2026-06-20T03:00:00Z",
  "scrub_coverage_pct": 100,
  "chunks_verified": 2847291,
  "corruptions_detected": 0,
  "corruptions_repaired": 0,
  "next_scrub": "2026-06-27T03:00:00Z"
}
```

---

## 23. Version Management & Retention

### Snapshot Versioning

NimbusFS stores **manifest snapshots** for each file version. Because chunks are deduplicated, each version only consumes storage for chunks that actually changed.

- Default: keep **5 versions** per file.
- Configurable per-path and per-file-type.

### Version Stats API

```
GET /api/v1/files/{id}/versions

{
  "versions": [
    { "v": 5, "created": "...", "size_mb": 5.2, "new_storage_mb": 0.8, "chunk_change_pct": 15 },
    { "v": 4, "created": "...", "size_mb": 5.1, "new_storage_mb": 0.3, "chunk_change_pct": 6 },
    ...
  ],
  "total_unique_storage_mb": 6.1,
  "savings_vs_full_copies_mb": 19.9
}
```

### Retention Policy Configuration

```yaml
retention:
  default:
    keep_versions: 5
    keep_daily_snapshots: 7        # one per day for 7 days
    keep_weekly_snapshots: 4       # one per week for 4 weeks
    keep_monthly_snapshots: 12     # one per month for 12 months
  per_path:
    - pattern: "**.xlsx"
      keep_versions: 10            # finance files keep more versions
    - pattern: "**.mp4"
      keep_versions: 1             # videos — only latest
```

### GC on Version Deletion

When a version is deleted by retention policy:
1. Decrement `RefCount` for all chunks in that version's manifest.
2. Chunks with `RefCount = 0` are eligible for GC.
3. GC is batched and runs as a background sweep.

---

## 24. Upload Flow (End to End)

Trace a 500 MB file upload:

### Step 1 — Initiate

1. Client sends `POST /files` with metadata: `{parent_path, file_name, file_size, mime_type}`.
2. Gateway validates JWT → calls `Auth.ValidateToken`.
3. Gateway calls `Metadata.InitiateUpload(...)`.
4. Metadata creates `files` row with `status = 'UPLOADING'`.
5. Metadata creates `upload_sessions` row.
6. Returns `{upload_session_id, file_id, expected_chunk_count}`.

### Step 2 — Chunk Negotiation

1. Client splits file locally into chunks using FastCDC (512KB min / 2MB avg / 8MB max).
2. Client computes BLAKE3 hash of each chunk.
3. Client sends all chunk IDs to `POST /chunks/negotiate` → `ChunkEngine.NegotiateUpload(chunk_ids)`.
4. Server responds: `{missing: [...], existing: [...]}`.

### Step 3 — Chunk Upload (per missing chunk)

1. For each missing chunk: client opens client-streaming RPC `ChunkEngine.StoreChunk`.
2. First message: `StoreChunkHeader {chunk_id, size, mime_type}`.
3. Subsequent messages: 64KB data frames.
4. Server writes to hot storage, verifies BLAKE3 on completion.
5. Server publishes `chunk.stored` to NATS.

### Step 4 — Finalize

1. Client sends `POST /files/{upload_session_id}/finalize` with ordered chunk list and file checksum.
2. Gateway calls `Metadata.FinalizeUpload(session_id, chunks, checksum)`.
3. Metadata writes `chunk_manifests` rows (ordered), creates `file_versions` row.
4. Metadata increments `RefCount` for each chunk via `ChunkEngine.IncrementRef`.
5. Metadata sets file `status = 'ACTIVE'`, `current_version` incremented.
6. Metadata updates user's `storage_used_bytes` in Auth/IAM.
7. Metadata publishes `file.created` (or `file.updated`) to NATS.
8. Search Service consumes event, indexes the file.

---

## 25. Download Flow (End to End)

### Hot File Download

1. Client sends `GET /files/{file_id}/download`.
2. Gateway validates JWT, checks permission (level 4 = Download).
3. Gateway calls `Metadata.GetFile(file_id)` → gets current version's chunk manifest.
4. Gateway calls `Metadata.GetChunkManifest(file_version_id)` → gets ordered list of `{chunk_id, offset, size}`.
5. For each chunk in order: Gateway opens `ChunkEngine.FetchChunk(chunk_id)` server-streaming RPC.
6. Gateway pipes each chunk's data frames directly to the HTTP response body.
7. Response headers: `Content-Disposition: attachment; filename="<name>"`, `Content-Length: <size>`.

### Cold File Download

1. Same as above, but `ChunkEngine.FetchChunk` returns `UNAVAILABLE` for cold chunks.
2. Gateway calls `TierManager.InitiateRestore(file_id)`.
3. Gateway returns `HTTP 202 Accepted` with body: `{restore_job_id, estimated_seconds}`.
4. Client polls `GET /files/{file_id}/restore-status` until `status = 'READY'`.
5. Client retries download.

---

## 26. Sync Protocol

### Client-Side State (SQLite)

Desktop client maintains a local SQLite database:

```sql
CREATE TABLE local_files (
    path        TEXT PRIMARY KEY,
    content_hash VARCHAR(64),      -- BLAKE3 of file content
    size        INTEGER,
    mtime       INTEGER,           -- last modified time (unix)
    inode       INTEGER,           -- filesystem inode
    sync_state  TEXT DEFAULT 'synced',  -- 'synced', 'modified', 'new', 'deleted', 'conflict'
    tier        TEXT DEFAULT 'hot',
    version     INTEGER DEFAULT 1,
    last_synced_at INTEGER
);

CREATE TABLE chunk_upload_progress (
    file_path   TEXT,
    chunk_index INTEGER,
    chunk_id    VARCHAR(64),
    uploaded    BOOLEAN DEFAULT 0,
    PRIMARY KEY (file_path, chunk_index)
);
```

### Change Detection (Client)

On each sync cycle:
1. Walk filesystem under the sync directory.
2. For each file: compare `mtime` and `size` against `local_files` table.
3. If both unchanged → file unchanged (no re-hash needed).
4. If changed → re-chunk, recompute hashes, mark as `modified`.
5. New files not in `local_files` → mark as `new`.
6. Files in `local_files` but missing from filesystem → mark as `deleted`.

### Sync Cycle

1. **Pull:** Call `SyncService.GetChanges(device_id, since_cursor)`.
2. Apply remote changes to local manifest and filesystem.
3. **Push:** Upload local changes to server.
4. Advance `sync_cursor` to `new_cursor`.

### Real-Time Updates: SSE

- Client holds an open SSE connection to `GET /events`.
- Gateway filters NATS events by `owner_id` and forwards to the SSE stream.
- Event types: `file.created`, `file.updated`, `file.deleted`, `file.moved`, `file.renamed`, `file.restored`, `sync.conflict`.
- On receiving an SSE event, client applies the change to the local manifest without a full re-fetch.

### Conflict Detection & Resolution (v1)

**Conflict condition:** Client's base version < server's current version AND client has local modifications made while offline.

**Resolution: Fork-and-surface**
1. Keep the server version as canonical.
2. Save client's version as: `filename (conflict copy from DeviceName, YYYY-MM-DD).extension`.
3. Upload conflict copy as a new file.
4. Create `sync_conflicts` record.
5. Surface conflict in desktop client UI with options:
   - **Keep server version** (delete conflict copy)
   - **Keep my version** (replace server version with conflict copy)
   - **Keep both** (do nothing, both files remain)

---

## 27. Desktop Client (Rust / iced)

### Technology

- **Framework:** `iced` with `tokio` feature flag — no separate runtime bridging needed.
- **HTTP:** `reqwest` crate (async HTTP client, communicates with API Gateway via REST/JSON).
- **File watching:** `notify` crate (cross-platform: inotify/FSEvents/ReadDirectoryChanges).
- **Notifications:** `notify-rust` crate (cross-platform OS notifications).
- **Local state:** SQLite via `rusqlite` crate.
- **Builds:** Cross-platform from day one (macOS, Windows, Linux) via CI matrix.

### UI Layout

- **Two-pane file manager** using iced's `pane_grid` widget.
  - **Left pane:** Directory tree navigation.
  - **Right pane:** File list (grid or list view) for the selected directory.
- Navigation state reads from **local SQLite manifest** — instant startup, no network round-trip.

### Features (v1)

| Feature | Description |
|---|---|
| **File browser** | Navigate directory tree, view files in grid/list mode |
| **Upload** | Chunked parallel upload with resume support; progress persisted in SQLite |
| **Download** | Stream response body directly to disk via reqwest — no RAM ceiling |
| **Background sync** | Watch a configured sync folder; debounce filesystem events before syncing |
| **Conflict resolution** | Display unresolved conflicts with Keep Server / Keep Mine / Keep Both options |
| **Tier display** | Show which tier each file is on (hot/warm/cold icon) |
| **Cold file restore** | Show restore progress indicator for cold files being retrieved |
| **Upload progress** | Per-file and per-chunk upload progress bars |
| **OS notifications** | Notify on: sync complete, conflict detected, restore ready, upload failed |
| **Search** | Search files by name/path via Search Service API |
| **Share links** | Create and manage share links for files (level 5 required) |
| **Version history** | View version list with dedup statistics |

### Features NOT in v1

- System tray
- Autostart on OS boot
- OS-level file system mount (FUSE/kernel)
- Code signing

### Upload Implementation (Rust Client)

```
1. User selects file(s) for upload.
2. Client chunks file using fixed-size splitting (negotiated chunk size from server).
3. Client computes BLAKE3 hash of each chunk.
4. Client calls POST /chunks/negotiate → gets list of missing chunks.
5. For each missing chunk:
   a. Record chunk in `chunk_upload_progress` SQLite table (uploaded=0).
   b. Upload chunk via POST /chunks (multipart/streaming).
   c. On success: update SQLite (uploaded=1).
6. On app crash/restart: read `chunk_upload_progress`, resume from unfinished chunks.
7. Call POST /files/{session_id}/finalize with ordered chunk list.
8. Clear `chunk_upload_progress` for this file.
```

### Background Sync Implementation

```
1. On startup: register device via POST /devices (if not already registered).
2. Start filesystem watcher (notify crate) on configured sync folder.
3. Debounce events: wait for quiet period (500ms–2s) before processing.
4. On change detected:
   a. Re-chunk changed files.
   b. Compute new hashes.
   c. Negotiate upload → upload missing chunks → finalize.
5. Periodically poll GET /sync/changes?since_cursor=N for remote changes.
6. Apply remote changes to local filesystem and SQLite manifest.
7. On SSE event received: apply immediately without waiting for poll.
```

### Client ↔ Server Communication

| Protocol | Direction | Use |
|---|---|---|
| REST/JSON over HTTPS | Client → Server | All CRUD operations, uploads, downloads |
| SSE | Server → Client | Real-time event notifications |

> **Note:** gRPC was considered for client↔server but REST is simpler; gRPC adds proto maintenance with no clear benefit at this scale.

### Configuration (Client-Side)

Stored in platform-appropriate config directory:
- macOS: `~/Library/Application Support/NimbusFS/config.toml`
- Linux: `~/.config/nimbusfs/config.toml`
- Windows: `%APPDATA%\NimbusFS\config.toml`

```toml
[server]
url = "https://nimbus.example.com"

[sync]
enabled = true
folder = "~/NimbusFS"
poll_interval_seconds = 30
debounce_ms = 1000

[auth]
# Tokens stored in OS keychain / credential manager
# Stored via keyring crate on Linux, Security.framework on macOS, WinCred on Windows

[ui]
theme = "dark"
view_mode = "list"    # "list" or "grid"
```

---

## 28. REST API Surface (Gateway)

### Authentication Routes (No JWT required)

| Method | Path | Description | Request Body | Response |
|---|---|---|---|---|
| `POST` | `/auth/register` | Register new user | `{email, username, password}` | `{user_id, message}` |
| `GET` | `/auth/verify` | Verify email | Query param: `token` | `{message}` |
| `POST` | `/auth/login` | Login | `{email, password, device_name?}` | `{access_token, refresh_token, user}` |
| `POST` | `/auth/refresh` | Refresh access token | `{refresh_token}` | `{access_token, refresh_token}` |
| `POST` | `/auth/forgot-password` | Request password reset | `{email}` | `{message}` |
| `POST` | `/auth/reset-password` | Reset password with token | `{token, new_password}` | `{message}` |

### Authenticated Routes (JWT required)

#### User Management

| Method | Path | Description | Request Body | Response |
|---|---|---|---|---|
| `GET` | `/auth/me` | Get current user profile | — | `User` |
| `PUT` | `/auth/me` | Update profile | `{username?}` | `User` |
| `POST` | `/auth/change-password` | Change password | `{current_password, new_password}` | `{message}` |
| `POST` | `/auth/logout` | Logout (revoke refresh token) | `{refresh_token}` | `{message}` |

#### File Operations

| Method | Path | Description | Request Body | Response |
|---|---|---|---|---|
| `POST` | `/files` | Initiate file upload | `{parent_path, file_name, file_size, mime_type}` | `{upload_session_id, file_id, expected_chunk_count}` |
| `POST` | `/files/{session_id}/finalize` | Finalize upload | `{chunks: [{chunk_id, index, size}], checksum}` | `{file_id, version, new_storage_bytes}` |
| `POST` | `/files/{session_id}/abort` | Abort upload | — | `{message}` |
| `GET` | `/files/{file_id}` | Get file metadata | — | `File` |
| `GET` | `/files/{file_id}/download` | Download file | — | Binary stream |
| `DELETE` | `/files/{file_id}` | Delete file | — | `{message}` |
| `PUT` | `/files/{file_id}/rename` | Rename file | `{new_name}` | `File` |
| `PUT` | `/files/{file_id}/move` | Move file | `{new_parent_path}` | `File` |
| `GET` | `/files/{file_id}/restore-status` | Check cold file restore status | — | `{status, progress_pct, estimated_seconds}` |

#### Directory Operations

| Method | Path | Description | Request Body | Response |
|---|---|---|---|---|
| `POST` | `/directories` | Create directory | `{parent_path, name}` | `File` |
| `GET` | `/directories` | List directory contents | Query: `path`, `page`, `page_size`, `sort_by`, `sort_order` | `{files: [File], total_count, page, page_size}` |

#### Chunk Operations

| Method | Path | Description | Request Body | Response |
|---|---|---|---|---|
| `POST` | `/chunks/negotiate` | Check which chunks exist | `{chunk_ids: [string]}` | `{missing: [string], existing: [string]}` |
| `POST` | `/chunks` | Upload a chunk (streaming) | Binary stream with headers | `{chunk_id, already_existed}` |
| `GET` | `/chunks/{chunk_id}` | Get chunk info | — | `{chunk_id, size, tier, exists}` |

#### Version Operations

| Method | Path | Description | Response |
|---|---|---|---|
| `GET` | `/files/{file_id}/versions` | List versions with dedup stats | `{versions: [VersionSummary], total_unique_storage_mb, savings_vs_full_copies_mb}` |
| `GET` | `/files/{file_id}/versions/{version}` | Get specific version metadata | `VersionSummary` |
| `POST` | `/files/{file_id}/versions/{version}/restore` | Restore old version as current | `{file_id, new_version}` |

#### Share Operations

| Method | Path | Description | Request Body | Response |
|---|---|---|---|---|
| `POST` | `/files/{file_id}/share` | Create share link | `{permission_level, expiry?}` | `{token_id, share_url}` |
| `GET` | `/files/{file_id}/shares` | List share links | — | `{tokens: [ShareToken]}` |
| `DELETE` | `/files/{file_id}/share/{token_id}` | Revoke share link | — | `{message}` |

#### Public Share Route (No Auth)

| Method | Path | Description |
|---|---|---|
| `GET` | `/share/{token_id}` | Access shared file (served as minimal HTML page or direct download) |

#### Search

| Method | Path | Description | Query Params | Response |
|---|---|---|---|---|
| `GET` | `/search` | Search files | `q`, `mime_type`, `page`, `page_size`, `sort_by`, `sort_order` | `{results: [SearchResult], total_count}` |

#### Sync Operations

| Method | Path | Description | Request Body | Response |
|---|---|---|---|---|
| `POST` | `/devices` | Register a device | `{device_name, os}` | `{device_id}` |
| `GET` | `/devices` | List user's devices | — | `{devices: [Device]}` |
| `DELETE` | `/devices/{device_id}` | Revoke a device | — | `{message}` |
| `GET` | `/sync/changes` | Get changes since cursor | Query: `device_id`, `since_cursor` | `{changes: [ChangeEvent], new_cursor}` |
| `GET` | `/sync/conflicts` | List unresolved conflicts | — | `{conflicts: [Conflict]}` |
| `POST` | `/sync/conflicts/{conflict_id}/resolve` | Resolve a conflict | `{resolution: "KEEP_SERVER" \| "KEEP_CLIENT" \| "KEEP_BOTH"}` | `{message}` |

#### SSE (Real-Time Events)

| Method | Path | Description |
|---|---|---|
| `GET` | `/events` | SSE stream of real-time events (filtered by user) |

**SSE Event Format:**

```
event: file.created
data: {"file_id": "...", "path": "/documents/report.pdf", "name": "report.pdf", "size_bytes": 5242880}

event: sync.conflict
data: {"file_id": "...", "device_id": "...", "conflict_copy_path": "..."}

event: file.restored
data: {"file_id": "...", "restore_job_id": "..."}
```

#### Admin Routes (Admin role required)

| Method | Path | Description |
|---|---|---|
| `GET` | `/admin/users` | List all users |
| `PUT` | `/admin/users/{user_id}/quota` | Update user storage quota |
| `POST` | `/admin/users/{user_id}/suspend` | Suspend a user |
| `DELETE` | `/admin/users/{user_id}` | Delete a user |
| `GET` | `/admin/integrity` | Get integrity scrub status |
| `POST` | `/admin/integrity/scrub` | Trigger a manual integrity scrub |
| `GET` | `/admin/tier/stats` | Get tier distribution statistics |
| `PUT` | `/admin/tier/policy` | Update tier transition policy |
| `POST` | `/admin/gc` | Trigger manual garbage collection |

### Response Data Types

#### User

```json
{
  "id": "UUID",
  "email": "string",
  "username": "string",
  "role": "user | admin",
  "email_verified": true,
  "status": "active",
  "storage_quota_bytes": 10737418240,
  "storage_used_bytes": 3221225472,
  "created_at": "RFC3339",
  "last_login_at": "RFC3339"
}
```

#### File

```json
{
  "id": "UUID",
  "owner_id": "UUID",
  "parent_id": "UUID | null",
  "name": "string",
  "path": "string",
  "is_directory": false,
  "size_bytes": 5242880,
  "mime_type": "application/pdf",
  "status": "ACTIVE",
  "current_version": 3,
  "tier": "hot",
  "checksum": "BLAKE3 hex string",
  "created_at": "RFC3339",
  "updated_at": "RFC3339"
}
```

#### VersionSummary

```json
{
  "version": 3,
  "size_bytes": 5242880,
  "new_storage_bytes": 819200,
  "chunk_change_pct": 15.0,
  "chunk_count": 5,
  "checksum": "BLAKE3 hex string",
  "created_at": "RFC3339",
  "created_by": "UUID"
}
```

#### ShareToken

```json
{
  "token_id": "UUID",
  "file_id": "UUID",
  "permission_level": 4,
  "expiry": "RFC3339 | null",
  "created_by_user_id": "UUID",
  "created_at": "RFC3339",
  "revoked_at": "RFC3339 | null",
  "access_count": 42,
  "share_url": "https://nimbus.example.com/share/{token_id}"
}
```

#### Device

```json
{
  "id": "UUID",
  "device_name": "MacBook Pro",
  "os": "macOS",
  "sync_cursor": 12847,
  "last_sync_at": "RFC3339",
  "registered_at": "RFC3339",
  "status": "active"
}
```

#### Conflict

```json
{
  "id": "UUID",
  "file_id": "UUID",
  "device_id": "UUID",
  "server_version": 5,
  "client_version": 3,
  "conflict_copy_path": "/documents/report (conflict copy from MacBook Pro, 2026-06-25).pdf",
  "resolution": "UNRESOLVED",
  "created_at": "RFC3339"
}
```

### Error Response Format

All errors follow a consistent format:

```json
{
  "error": {
    "code": "NOT_FOUND",
    "message": "File not found",
    "details": "No file with ID abc-123 exists"
  }
}
```

| HTTP Status | gRPC Code | Meaning |
|---|---|---|
| 400 | `INVALID_ARGUMENT` | Malformed request |
| 401 | `UNAUTHENTICATED` | Missing or invalid JWT |
| 403 | `PERMISSION_DENIED` | Insufficient permissions |
| 404 | `NOT_FOUND` | Resource not found |
| 409 | `ALREADY_EXISTS` | Duplicate resource |
| 429 | `RESOURCE_EXHAUSTED` | Rate limit exceeded |
| 500 | `INTERNAL` | Server error |
| 503 | `UNAVAILABLE` | Service temporarily unavailable |

---

## 29. Observability

### Three Pillars

| Pillar | Tool | What It Captures |
|---|---|---|
| **Traces** | Jaeger (via OTEL) | Full request path across all services |
| **Metrics** | Prometheus (via OTEL) | Aggregate signals: latency, throughput, error rates |
| **Logs** | Structured JSON (stdout) | Point-in-time details per service |

### Trace Context Propagation

- **gRPC:** Automatic via `otelgrpc.NewServerHandler()` / `otelgrpc.NewClientHandler()`.
- **NATS:** Inject OTel context into NATS message headers on publish. Extract on consume to create child span with `SpanKindConsumer`.
- **HTTP:** Standard W3C `traceparent` header propagation.

### Every Log Line Includes

```json
{
  "level": "INFO | WARN | ERROR",
  "msg": "human-readable message",
  "trace_id": "hex string",
  "span_id": "hex string",
  "service": "metadata-svc",
  "timestamp": "RFC3339Nano",
  "key": "value"
}
```

### Key Metrics

| Metric | Type | Labels |
|---|---|---|
| `nimbus_request_duration_seconds` | Histogram | `service`, `method`, `status_code` |
| `nimbus_request_total` | Counter | `service`, `method`, `status_code` |
| `nimbus_chunk_stored_bytes_total` | Counter | `tier` |
| `nimbus_chunk_retrieved_bytes_total` | Counter | `tier` |
| `nimbus_nats_consumer_lag` | Gauge | `consumer_name` |
| `nimbus_tier_migration_queue_depth` | Gauge | `source_tier`, `target_tier` |
| `nimbus_active_uploads` | Gauge | — |
| `nimbus_storage_used_bytes` | Gauge | `tier` |
| `nimbus_users_total` | Gauge | `status` |
| `nimbus_scrub_corruptions_total` | Counter | — |

### Dashboard Essentials

- Upload success rate
- End-to-end upload latency (p50, p95, p99)
- NATS consumer lag per consumer (if growing → workers falling behind)
- Chunk Engine disk IOPS
- Tier migration queue depth
- Storage utilization per tier

---

## 30. Deployment

### Option A — Single Binary Mode (Primary Self-Hosted Target)

- All 9 services compiled into **one binary**.
- Multiple gRPC servers on different ports, each in a separate goroutine.
- NATS calls become in-process function calls.
- **Target audience:** Personal use, single VM, NAS device.
- Zero ops overhead, no orchestration.

### Option B — Docker Compose (Team Deployments)

- Same Compose file as development, single VM with more resources.
- Handles moderate scale (tens of thousands of files, dozens of users).
- Upgrade: `docker compose pull && docker compose up -d`.

### Option C — Kubernetes (Enterprise, v2)

- For multiple storage nodes, HA, horizontal scaling.
- **Not implemented at launch.** Path documented for enterprises.

### Finalized Deployment Plan

- Ship **Option A** as primary self-hosted mode.
- Ship **Option B** for team deployments.
- Document Kubernetes path — do not implement at launch.

---

## 31. Docker Compose (Local Development)

All 9 services + infrastructure run with `docker compose up`.

### Services

| Service | Image | Port(s) | Depends On |
|---|---|---|---|
| `nats` | `nats:2.10-alpine` | `4222`, `8222` (monitoring) | — |
| `postgres` | `postgres:16-alpine` | `5432` | — |
| `otel-collector` | `otel/opentelemetry-collector-contrib` | `4317`, `4318` | — |
| `jaeger` | `jaegertracing/all-in-one` | `16686` (UI), `14268` | `otel-collector` |
| `minio` | `minio/minio` | `9000`, `9001` (console) | — |
| `auth-svc` | Built from `cmd/auth-svc` | `9001` | `postgres`, `nats` |
| `email-svc` | Built from `cmd/email-svc` | `9007` | — |
| `metadata-svc` | Built from `cmd/metadata-svc` | `9002` | `postgres`, `nats`, `auth-svc` |
| `chunk-svc` | Built from `cmd/chunk-svc` | `9003` | `nats`, `minio` |
| `tier-svc` | Built from `cmd/tier-svc` | `9006` | `postgres`, `nats`, `chunk-svc` |
| `compression-worker` | Built from `cmd/compression-worker` | — (no port) | `nats`, `chunk-svc` |
| `sync-svc` | Built from `cmd/sync-svc` | `9004` | `postgres`, `nats`, `metadata-svc` |
| `search-svc` | Built from `cmd/search-svc` | `9005` | `nats`, `metadata-svc` |
| `gateway` | Built from `cmd/gateway` | `8080` | All services |

### Key Principles

- Deterministic startup via `healthcheck` + `depends_on: condition: service_healthy`.
- Shared Docker network namespace.
- Mounted volumes for chunk data and database persistence.
- Every developer gets an identical environment.

---

## 32. Resolved Design Decisions

| Decision | Chosen | Rejected | Why |
|---|---|---|---|
| Message broker | NATS JetStream | Kafka | Simpler to self-host (single binary), at-least-once, embedded |
| Proto tooling | buf | Raw protoc | Lint, breaking-change detection, catches accidents |
| CAS hash | BLAKE3 | SHA-256 | 3–4× faster, same security properties |
| Chunk addressing | Content-addressed | Path-addressed | Enables dedup, immutability, cache-friendliness |
| Chunking algorithm | FastCDC (variable) | Fixed-size | Boundary-shift resilience, 10× faster than Rabin |
| Chunk manifest ownership | Metadata Service | Chunk Engine | Metadata needs manifests for reads — avoids cross-service hop |
| Client-side chunking | Client splits and hashes | Server-side | Client-side dedup check: server never receives existing data |
| gRPC upload streaming | Client-streaming | Single unary | Files > 4MB need backpressure and partial failure recovery |
| Sync conflict (v1) | Fork-and-surface | Last-write-wins / three-way merge | LWW silently loses data; three-way merge is complex |
| Rate limiting unit | Per user ID | Per IP | Throttles heavy users without punishing NAT users |
| Permission model | Cumulative ordinal 0–5 | Flat bitmask | Clean UX, natural hierarchy, easy `>=` comparison |
| Share token storage | Metadata Service | Auth/IAM | Share tokens are file accessibility, not user identity |
| Version strategy | CAS + CDC snapshot manifests | Binary delta storage | Delta saves only ~6% more on ~30% of file types; 11 weeks of engineering vs 4 weeks |
| Client ↔ server protocol | REST/JSON + SSE | gRPC | Simpler for Rust client; gRPC adds proto maintenance overhead |
| Real-time updates | SSE (Server-Sent Events) | WebSocket | Unidirectional is sufficient; avoids WebSocket complexity |
| Desktop framework | iced (Rust) | Tauri, Electron | Native Rust, no embedded browser overhead |
| Web frontend | None (v1) | React + TypeScript | Desktop covers full use case for self-hosted; deferred |
| S3 API | None (v1) | S3-compatible endpoint | Deferred; clean REST API first |
| Local dev | Docker Compose | Kubernetes | Compose is simpler and sufficient for dev |
| Production deployment (v1) | Single binary + Compose | Kubernetes | Matches self-hosted audience |
| Compression on write vs transition | On tier transition | On initial write | Simplifies write path; tier policy engine handles uniformly |
| Cold restore | 202 Accepted + polling | Synchronous block | Users shouldn't wait minutes; async with progress |
| CAS scope | Global (cross-user) | Per-tenant | Self-hosted for one org; cross-user dedup is acceptable |
| Dedup index (single-node) | BadgerDB v4 | Redis, etcd | Single-node; BadgerDB is embedded, no external dependency |
| Email system | SMTP via Go net/smtp | External service (SendGrid, etc.) | Self-hosted; no external dependency; SMTP is universal |
