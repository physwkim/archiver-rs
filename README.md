# EPICS Archiver Appliance (Rust)

A high-performance EPICS Channel Access archiver written in Rust, compatible with the [Java EPICS Archiver Appliance](https://github.com/slacmshankar/epicsarchiverap) data format and REST API.

## Features

- **PlainPB-compatible storage** — binary protobuf format, readable by Java archiver tools
- **3-tier storage with automatic ETL** — Short-Term (STS), Mid-Term (MTS), Long-Term (LTS) with configurable partition granularity
- **Monitor and Scan sampling modes** — subscribe to value changes or poll at fixed intervals
- **Post-processing** — `mean`, `min`, `max`, `std`, `firstSample` decimation on retrieval
- **Multi-appliance cluster mode** — transparent PV routing and proxy across peers
- **Bluesky integration** — archive scan metadata from Kafka as EPICS-style PVs
- **Management UI** — web console for PV management, search, bulk operations, and reports
- **Java-compatible REST API** — drop-in replacement for archiver viewer tools
- **Built-in security** — optional TLS, API key authentication, CORS restriction, rate limiting, security headers, request body size limits, and internal error hiding — features the Java archiver relies on reverse proxies or network segmentation for

## Prerequisites

- **Rust** 1.75+ (2021 edition)
- **CMake** — required to build the `rdkafka` Kafka client
- **EPICS base** — `EPICS_CA_ADDR_LIST` must be set for Channel Access connectivity
- **Kafka** (optional) — only needed if Bluesky integration is enabled

## Building

```bash
git clone <repo-url>
cd archiver
cargo build --release
```

The binary is produced at `target/release/archiver`.

## Quick Start

### 1. Create a configuration file

Create `archiver.toml`:

```toml
listen_addr = "0.0.0.0"
listen_port = 17665

[storage.sts]
root_folder = "/data/archiver/sts"
partition_granularity = "hour"

[storage.mts]
root_folder = "/data/archiver/mts"
partition_granularity = "day"

[storage.lts]
root_folder = "/data/archiver/lts"
partition_granularity = "year"

[engine]
write_period_secs = 10
```

### 2. Set EPICS environment variables

```bash
# List of IOC hosts or broadcast addresses for CA name resolution
export EPICS_CA_ADDR_LIST="192.168.1.255 10.0.0.255"

# Disable auto-discovery if you specify addresses manually
export EPICS_CA_AUTO_ADDR_LIST=NO
```

If your IOCs are on the same subnet and broadcast works, you can rely on auto-discovery:

```bash
export EPICS_CA_AUTO_ADDR_LIST=YES
```

### 3. Run the archiver

```bash
# Default: reads archiver.toml from current directory
./target/release/archiver

# Or specify a config file path
./target/release/archiver /etc/archiver/myconfig.toml
```

The default log level is `info`. Use the `RUST_LOG` environment variable for more detail:

```bash
# Enable debug logging for all archiver crates
RUST_LOG=debug ./target/release/archiver

# Debug only for the engine, info for everything else
RUST_LOG=info,archiver_engine=debug ./target/release/archiver

# Suppress noisy storage writes, keep engine debug
RUST_LOG=info,archiver_engine=debug,archiver_core::storage=warn ./target/release/archiver
```

### 4. Archive a PV

```bash
# Single PV
curl -X POST http://localhost:17665/mgmt/bpl/archivePV \
  -H "Content-Type: application/json" \
  -d '{"pv": "SIM:Sine"}'

# Bulk archive (newline-delimited)
curl -X POST http://localhost:17665/mgmt/bpl/archivePV \
  -H "Content-Type: text/plain" \
  -d $'SIM:Sine\nSIM:Cosine\nSIM:Ramp'
```

### 5. Retrieve data

```bash
# JSON format
curl "http://localhost:17665/retrieval/data/getData.json?pv=SIM:Sine&from=2024-03-15T00:00:00Z&to=2024-03-15T12:00:00Z"

# CSV format
curl "http://localhost:17665/retrieval/data/getData.csv?pv=SIM:Sine&from=2024-03-15T00:00:00Z"

# With post-processing (10-minute mean)
curl "http://localhost:17665/retrieval/data/getData.json?pv=mean_600(SIM:Sine)&from=2024-03-15T00:00:00Z"
```

### 6. Open the management UI

Navigate to `http://localhost:17665/mgmt/ui/` in your browser.

## Configuration Reference

### Top-Level

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `listen_addr` | string | `"0.0.0.0"` | Bind address |
| `listen_port` | u16 | `17665` | HTTP port (matches Java archiver default) |
| `storage` | object | *required* | 3-tier storage config |
| `engine` | object | defaults | EPICS engine settings |
| `bluesky` | object | *disabled* | Kafka Bluesky integration |
| `cluster` | object | *disabled* | Multi-appliance cluster mode |
| `security` | object | defaults | Security settings (CORS, rate limiting, body limits) |
| `tls` | object | *disabled* | TLS certificate configuration |
| `api_keys` | string[] | *disabled* | API keys for write-endpoint authentication |

### `[storage.sts]` / `[storage.mts]` / `[storage.lts]`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `root_folder` | path | *required* | Directory for this storage tier |
| `partition_granularity` | string | *required* | Time-based file partitioning |
| `hold` | u32 | `5` | Partitions to keep before ETL moves data to the next tier |
| `gather` | u32 | `3` | Partitions to move per ETL cycle |

**Partition granularity options:** `"5min"`, `"15min"`, `"30min"`, `"hour"`, `"day"`, `"month"`, `"year"`

Recommended setup:
- STS: `"hour"` — high-resolution recent data
- MTS: `"day"` — medium-term storage
- LTS: `"year"` — long-term archive

### `[engine]`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `write_period_secs` | u64 | `10` | How often buffered samples flush to disk |
| `policy_file` | path | *none* | Path to PV policy TOML file |

### `[bluesky]`

| Field | Type | Description |
|-------|------|-------------|
| `bootstrap_servers` | string | Kafka broker list (e.g., `"localhost:9092"`) |
| `topic` | string | Kafka topic (e.g., `"bluesky.documents"`) |
| `group_id` | string | Consumer group ID |
| `beamline` | string | Beamline name, used in PV naming (`EXP:{beamline}:...`) |

### `[cluster]`

See [Cluster Mode](#cluster-mode) below.

### `[security]`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `cors_origins` | string[] | `[]` (permissive) | Allowed CORS origins. Empty = allow all. |
| `rate_limit_rps` | u32 | `0` (disabled) | Per-IP requests per second limit |
| `rate_limit_burst` | u32 | `50` | Burst capacity for rate limiter |
| `max_body_size` | usize | `10485760` (10 MB) | Maximum request body size in bytes |

### `[tls]`

| Field | Type | Description |
|-------|------|-------------|
| `cert_path` | path | Path to PEM certificate file |
| `key_path` | path | Path to PEM private key file |

## Security

The archiver includes built-in security features that the Java Archiver Appliance typically delegates to reverse proxies or network segmentation:

- **TLS** — native HTTPS support via rustls (no need for a reverse proxy just for TLS)
- **API key authentication** — write endpoints (archive, pause, resume, delete) require an `X-API-Key` header when `api_keys` is configured; read endpoints remain open
- **Timing-safe key comparison** — API keys are compared using constant-time equality to prevent timing attacks
- **CORS restriction** — configurable allowed origins (default: permissive for development; restrict in production)
- **Rate limiting** — per-IP token bucket rate limiter to mitigate abuse
- **Security headers** — `X-Content-Type-Options`, `X-Frame-Options`, `Content-Security-Policy`, `Referrer-Policy` added to all responses
- **Request body size limits** — prevents oversized payloads from consuming memory
- **Error information hiding** — internal errors are logged server-side but only generic messages are returned to clients

### Example: Production Security Configuration

```toml
# API key authentication
api_keys = ["your-secret-key-here"]

# TLS
[tls]
cert_path = "/etc/ssl/archiver/cert.pem"
key_path = "/etc/ssl/archiver/key.pem"

# Security settings
[security]
cors_origins = ["https://your-app.example.com"]
rate_limit_rps = 100
rate_limit_burst = 200
max_body_size = 5242880  # 5 MB
```

## EPICS CA Environment Variables

The archiver uses the `epics-base-rs` library, which respects standard EPICS Channel Access environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `EPICS_CA_ADDR_LIST` | *empty* | Space-separated list of IP addresses or broadcast addresses for CA name resolution |
| `EPICS_CA_AUTO_ADDR_LIST` | `YES` | Automatically add broadcast addresses of all local network interfaces |
| `EPICS_CA_CONN_TMO` | `30.0` | CA connection timeout in seconds |
| `EPICS_CA_SERVER_PORT` | `5064` | Default CA server port |
| `EPICS_CA_REPEATER_PORT` | `5065` | CA repeater port |

**Typical configurations:**

```bash
# Lab network with IOCs on a single subnet
export EPICS_CA_AUTO_ADDR_LIST=YES

# Multiple subnets — list each broadcast address
export EPICS_CA_ADDR_LIST="10.0.1.255 10.0.2.255 192.168.1.100"
export EPICS_CA_AUTO_ADDR_LIST=NO

# Direct connection to specific IOC hosts
export EPICS_CA_ADDR_LIST="ioc-host1 ioc-host2 ioc-host3"
export EPICS_CA_AUTO_ADDR_LIST=NO
```

## PV Policy File

A policy file controls per-PV sampling behavior. Create a TOML file and reference it in the config:

```toml
# archiver.toml
[engine]
policy_file = "/etc/archiver/policies.toml"
```

Policy file format:

```toml
# Exact match — highest priority
[[policy]]
pv = "RING:Current"
sample_mode = "scan"
sampling_period = 1.0

# Glob pattern — matches all PVs starting with "SIM:"
[[policy]]
pv = "SIM:*"
sample_mode = "monitor"

# Scan mode with 5-second interval
[[policy]]
pv = "TEMP:??:Readback"
sample_mode = "scan"
sampling_period = 5.0
```

- **`sample_mode`**: `"monitor"` (event-driven, default) or `"scan"` (periodic polling)
- **`sampling_period`**: seconds between reads in scan mode (default: `1.0`)
- Glob patterns support `*` (any characters) and `?` (single character)
- Exact PV name matches take priority over glob patterns

## Post-Processing on Retrieval

Data can be decimated at query time using post-processor syntax in the `pv` parameter:

```
pv=<processor>_<interval_secs>(<pv_name>)
```

| Processor | Description |
|-----------|-------------|
| `mean_N` | Average value over N-second bins |
| `min_N` | Minimum value over N-second bins |
| `max_N` | Maximum value over N-second bins |
| `std_N` | Standard deviation over N-second bins |
| `firstSample_N` | First sample in each N-second bin |

Examples:

```bash
# 10-minute mean
curl "http://localhost:17665/retrieval/data/getData.json?pv=mean_600(RING:Current)&from=2024-03-15T00:00:00Z&to=2024-03-16T00:00:00Z"

# 1-hour max
curl "http://localhost:17665/retrieval/data/getData.json?pv=max_3600(TEMP:Sensor1)&from=2024-03-01T00:00:00Z"
```

## Cluster Mode

Cluster mode enables multiple archiver instances to work together as a single logical system, compatible with the Java archiver's multi-appliance architecture.

### How It Works

- Each appliance archives a subset of PVs
- When a data request arrives for a PV not archived locally, the appliance transparently proxies the request to the correct peer
- PV routing is cached with a configurable TTL to minimize lookup overhead
- Management endpoints can aggregate results across all peers with `?cluster=true`
- Circular proxy loops are prevented via the `X-Archiver-Proxied` header

### Cluster Configuration

Each appliance needs its own identity and a list of peers:

**Appliance 0** (`appliance0.toml`):

```toml
listen_addr = "0.0.0.0"
listen_port = 17665

[storage.sts]
root_folder = "/data/app0/sts"
partition_granularity = "hour"

[storage.mts]
root_folder = "/data/app0/mts"
partition_granularity = "day"

[storage.lts]
root_folder = "/data/app0/lts"
partition_granularity = "year"

[cluster.identity]
name = "appliance0"
mgmt_url = "http://app0-host:17665/mgmt/bpl"
retrieval_url = "http://app0-host:17665/retrieval"
engine_url = "http://app0-host:17665"
etl_url = "http://app0-host:17665"

[[cluster.peers]]
name = "appliance1"
mgmt_url = "http://app1-host:17665/mgmt/bpl"
retrieval_url = "http://app1-host:17665/retrieval"

[[cluster.peers]]
name = "appliance2"
mgmt_url = "http://app2-host:17665/mgmt/bpl"
retrieval_url = "http://app2-host:17665/retrieval"
```

**Appliance 1** (`appliance1.toml`):

```toml
listen_addr = "0.0.0.0"
listen_port = 17665

[storage.sts]
root_folder = "/data/app1/sts"
partition_granularity = "hour"

[storage.mts]
root_folder = "/data/app1/mts"
partition_granularity = "day"

[storage.lts]
root_folder = "/data/app1/lts"
partition_granularity = "year"

[cluster.identity]
name = "appliance1"
mgmt_url = "http://app1-host:17665/mgmt/bpl"
retrieval_url = "http://app1-host:17665/retrieval"
engine_url = "http://app1-host:17665"
etl_url = "http://app1-host:17665"

[[cluster.peers]]
name = "appliance0"
mgmt_url = "http://app0-host:17665/mgmt/bpl"
retrieval_url = "http://app0-host:17665/retrieval"

[[cluster.peers]]
name = "appliance2"
mgmt_url = "http://app2-host:17665/mgmt/bpl"
retrieval_url = "http://app2-host:17665/retrieval"
```

### Cluster Config Options

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `identity.name` | string | *required* | Unique name for this appliance |
| `identity.mgmt_url` | string | *required* | This appliance's management URL |
| `identity.retrieval_url` | string | *required* | This appliance's retrieval URL |
| `identity.engine_url` | string | *required* | This appliance's engine URL |
| `identity.etl_url` | string | *required* | This appliance's ETL URL |
| `cache_ttl_secs` | u64 | `300` | PV routing cache TTL in seconds |
| `peer_timeout_secs` | u64 | `30` | HTTP timeout for peer requests |
| `peers[].name` | string | *required* | Peer appliance name |
| `peers[].mgmt_url` | string | *required* | Peer management URL |
| `peers[].retrieval_url` | string | *required* | Peer retrieval URL |

### Deploying a Cluster

1. **Provision storage** on each host — each appliance has its own independent 3-tier storage.

2. **Write config files** — each appliance lists all *other* appliances as peers. The `identity` section describes itself.

3. **Set EPICS environment** — each appliance needs `EPICS_CA_ADDR_LIST` to reach the IOCs it will archive.

4. **Start each appliance:**

   ```bash
   # On app0-host
   EPICS_CA_ADDR_LIST="10.0.1.255" ./archiver appliance0.toml

   # On app1-host
   EPICS_CA_ADDR_LIST="10.0.2.255" ./archiver appliance1.toml

   # On app2-host
   EPICS_CA_ADDR_LIST="10.0.3.255" ./archiver appliance2.toml
   ```

5. **Archive PVs** on whichever appliance you choose — the PV is owned by the appliance that archives it:

   ```bash
   # Archive SIM:Sine on appliance0
   curl -X POST http://app0-host:17665/mgmt/bpl/archivePV \
     -H "Content-Type: application/json" -d '{"pv":"SIM:Sine"}'

   # Archive SIM:Cosine on appliance1
   curl -X POST http://app1-host:17665/mgmt/bpl/archivePV \
     -H "Content-Type: application/json" -d '{"pv":"SIM:Cosine"}'
   ```

6. **Query from any appliance** — requests are transparently proxied:

   ```bash
   # This works even though SIM:Cosine is on appliance1
   curl "http://app0-host:17665/retrieval/data/getData.json?pv=SIM:Cosine&from=2024-01-01T00:00:00Z"
   ```

### Cluster-Aware Management

Add `?cluster=true` to aggregate results from all peers:

```bash
# List PVs from all appliances
curl "http://app0-host:17665/mgmt/bpl/getAllPVs?cluster=true"

# Search across the cluster
curl "http://app0-host:17665/mgmt/bpl/getMatchingPVs?pv=SIM:*&cluster=true"

# Check PV status across the cluster
curl "http://app0-host:17665/mgmt/bpl/getPVStatus?pv=SIM:Cosine&cluster=true"
```

### Cluster BPL Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /mgmt/bpl/getAppliancesInCluster` | List all appliances (self + peers) |
| `GET /mgmt/bpl/getApplianceInfo?id=<name>` | Get info for a specific appliance |

The management UI includes a **Cluster** tab and **"Include cluster"** checkboxes on the PV List and Search tabs.

## Bluesky Kafka Integration

The archiver can consume [Bluesky](https://blueskyproject.io/) scan documents from Kafka and archive them as EPICS-style PVs.

```toml
[bluesky]
bootstrap_servers = "kafka-broker:9092"
topic = "bluesky.documents"
group_id = "archiver-bluesky"
beamline = "BL1"
```

Bluesky documents are mapped to PVs with the naming convention `EXP:{beamline}:{category}:{detail}`:

| Document | PVs Created |
|----------|-------------|
| RunStart | `EXP:BL1:run:active`, `EXP:BL1:run:uid`, `EXP:BL1:run:plan_name`, `EXP:BL1:run:scan_id` |
| Descriptor | `EXP:BL1:scan:num_points`, `EXP:BL1:det:{name}:*`, `EXP:BL1:motor:{name}:*` |
| Event | `EXP:BL1:det:{name}:value`, `EXP:BL1:motor:{name}:value` |
| RunStop | `EXP:BL1:run:active` set to 0 |

## REST API Reference

### Data Retrieval

| Endpoint | Description |
|----------|-------------|
| `GET /retrieval/data/getData.json` | JSON format (Java viewer compatible) |
| `GET /retrieval/data/getData.csv` | CSV format |
| `GET /retrieval/data/getData.raw` | Raw protobuf format |

**Query parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| `pv` | yes | PV name, optionally with post-processor: `mean_600(PV:Name)` |
| `from` | no | Start time in ISO 8601 (default: 1 hour ago) |
| `to` | no | End time in ISO 8601 (default: now) |
| `limit` | no | Maximum number of samples to return |

### Management

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/mgmt/bpl/getAllPVs` | GET | List all PV names (`?cluster=true` for cluster-wide) |
| `/mgmt/bpl/getMatchingPVs?pv=<pattern>` | GET | Glob-pattern search (`?cluster=true` supported) |
| `/mgmt/bpl/getPVStatus?pv=<name>` | GET | PV status and metadata (`?cluster=true` supported) |
| `/mgmt/bpl/archivePV` | POST | Start archiving (JSON or text body) |
| `/mgmt/bpl/pauseArchivingPV?pv=<name>` | GET | Pause archiving |
| `/mgmt/bpl/pauseArchivingPV` | POST | Bulk pause (text body) |
| `/mgmt/bpl/resumeArchivingPV?pv=<name>` | GET | Resume archiving |
| `/mgmt/bpl/resumeArchivingPV` | POST | Bulk resume (text body) |
| `/mgmt/bpl/deletePV?pv=<name>` | GET | Delete PV (`&deleteData=true` to remove files) |
| `/mgmt/bpl/getPVCount` | GET | Total, active, and paused counts |
| `/mgmt/bpl/changeArchivalParameters` | GET | Update sampling (`?pv=<name>&samplingmethod=scan&samplingperiod=5`) |
| `/mgmt/bpl/getPausedPVsReport` | GET | List paused PVs |
| `/mgmt/bpl/getNeverConnectedPVs` | GET | PVs that never connected |
| `/mgmt/bpl/getCurrentlyDisconnectedPVs` | GET | Currently offline PVs |
| `/mgmt/bpl/getRecentlyAddedPVs` | GET | PVs added in last 24h |
| `/mgmt/bpl/getRecentlyModifiedPVs` | GET | PVs modified in last 24h |
| `/mgmt/bpl/getSilentPVsReport` | GET | PVs with no samples in last 1h |
| `/mgmt/bpl/getAppliancesInCluster` | GET | List cluster appliances |
| `/mgmt/bpl/getApplianceInfo?id=<name>` | GET | Single appliance info |

### UI

| Endpoint | Description |
|----------|-------------|
| `/mgmt/ui/` | Management web console |

## Storage Architecture

```
Writes ──> [STS] ──ETL──> [MTS] ──ETL──> [LTS]
            hour           day            year
```

- All new samples are written to STS
- ETL automatically migrates aged partitions to the next tier
- Each tier's `hold` parameter controls how many partitions to keep before migration
- File format is PlainPB (protobuf), compatible with the Java archiver
- File path convention: `{root}/{PV/Name}:{partition}.pb` (colons in PV names become directory separators)

**ETL timing:**
- STS to MTS runs every `sts.hold * sts.partition_granularity` (e.g., 5 hours with `hold=5, granularity="hour"`)
- MTS to LTS runs every `mts.hold * mts.partition_granularity` (e.g., 150 days with `hold=5, granularity="month"`)

## Running Tests

```bash
cargo test --workspace
```

## Full Configuration Example

```toml
listen_addr = "0.0.0.0"
listen_port = 17665

[storage.sts]
root_folder = "/data/archiver/sts"
partition_granularity = "hour"
hold = 5
gather = 3

[storage.mts]
root_folder = "/data/archiver/mts"
partition_granularity = "day"
hold = 30
gather = 10

[storage.lts]
root_folder = "/data/archiver/lts"
partition_granularity = "year"
hold = 10
gather = 5

[engine]
write_period_secs = 10
policy_file = "/etc/archiver/policies.toml"

# Optional: API key authentication
api_keys = ["change-me-to-a-real-secret"]

# Optional: TLS
[tls]
cert_path = "/etc/ssl/archiver/cert.pem"
key_path = "/etc/ssl/archiver/key.pem"

# Optional: Security hardening
[security]
cors_origins = ["https://controls.example.com"]
rate_limit_rps = 100
rate_limit_burst = 200
max_body_size = 10485760

# Optional: Bluesky Kafka integration
[bluesky]
bootstrap_servers = "kafka:9092"
topic = "bluesky.documents"
group_id = "archiver"
beamline = "BL1"

# Optional: Multi-appliance cluster
[cluster.identity]
name = "appliance0"
mgmt_url = "http://archiver0:17665/mgmt/bpl"
retrieval_url = "http://archiver0:17665/retrieval"
engine_url = "http://archiver0:17665"
etl_url = "http://archiver0:17665"

[cluster]
cache_ttl_secs = 300
peer_timeout_secs = 30

[[cluster.peers]]
name = "appliance1"
mgmt_url = "http://archiver1:17665/mgmt/bpl"
retrieval_url = "http://archiver1:17665/retrieval"
```

## Compatibility

This archiver is designed to be a drop-in replacement for the Java EPICS Archiver Appliance. It is compatible with:

- **Data format** — PlainPB protobuf files can be read by Java archiver tools
- **REST API** — same endpoint paths and JSON response formats
- **Retrieval clients** — works with Archiver Viewer, CSS/Phoebus, PyEPICS archiver client
- **Cluster protocol** — `getAppliancesInCluster` / `getApplianceInfo` match Java response format

## License

MIT
