# log-processor

**log-processor** is a Java 8-compatible Kafka consumer application that processes log and metric messages from Apache Kafka topics, writes them to files, persists alerts and metrics to MySQL, and sends alert notifications via Telegram.

---

## Requirements

| Requirement | Version |
|---|---|
| Java | 8 (1.8) |
| Maven | 3.6+ |
| Apache Kafka | Reachable broker |
| MySQL | 5.7+ or 8.x |
| OS | Linux deployment paths are shown; any OS works if configured paths are valid |

---

## Build

```bash
mvn clean package
```

Output JAR:

```
target/log-processor.jar
```

---

## Run

The config file path can be supplied in three ways (checked in this order):

1. **CLI argument:**

   ```bash
   java -jar target/log-processor.jar --config=/path/to/consumer_config.json
   ```

2. **Environment variable:**

   ```bash
   CONSUMER_CONFIG=/path/to/consumer_config.json java -jar target/log-processor.jar
   ```

3. **JVM system property:**

   ```bash
   java -Dconsumer.config=/path/to/consumer_config.json -jar target/log-processor.jar
   ```

If none of the above are provided, the application falls back to `config/consumer_config.json` on the classpath.

---

## Before First Run

The application **does not create any files or directories**. All output files and pattern store files declared in the config must exist and be writable before the app starts. The app aborts at startup with a clear error if any configured path is missing or not writable.

Create the required files manually:

```bash
touch /data/logs/app.log
touch /data/logs/server_storage.json

# Only if patternStoreFile is configured for a topic:
touch /data/patterns/app-logs.txt
```

---

## Configuration

The application is driven by a single JSON file.

### Full Example: `consumer_config.json`

```json
{
  "bootstrapServers": "localhost:9092",

  "telegramBotToken": "<BOT_TOKEN>",
  "telegramChatId": "<CHAT_ID>",

  "database": {
    "url": "jdbc:mysql://localhost:3306/logDB",
    "user": "dbuser",
    "password": "dbpassword",
    "tables": {
      "alertLogTable": "alert_logs",
      "serverStorageSnapshotTable": "server_storage_snapshot",
      "mountPathStorageUsageTable": "mount_path_storage_usage"
    }
  },

  "topics": [
    {
      "topic": "app-logs",
      "type": "LOG",
      "output": "/data/logs/app.log",
      "alertKeywords": ["error", "fatal", "exception", "timeout", "critical"],
      "patternStoreFile": "/data/patterns/app-logs.txt"
    },
    {
      "topic": "server-storage-snapshot",
      "type": "METRIC",
      "output": "/data/logs/server_storage.json"
    }
  ]
}
```

---

### Field Reference

#### Top-level

| Field | Type | Description |
|---|---|---|
| `bootstrapServers` | string | Kafka broker address(es), e.g. `host:9092` |
| `telegramBotToken` | string | Telegram Bot API token |
| `telegramChatId` | string | Telegram chat/channel ID to send alerts to |
| `database` | object | MySQL connection and table names |
| `topics` | array | List of Kafka topic consumers |

#### `database`

| Field | Description |
|---|---|
| `url` | JDBC connection URL |
| `user` | Database username |
| `password` | Database password |
| `tables.alertLogTable` | Table for alert log records |
| `tables.serverStorageSnapshotTable` | Table for storage snapshot summaries |
| `tables.mountPathStorageUsageTable` | Table for per-mount disk usage rows |

#### `topics[]`

| Field | Required | Description |
|---|---|---|
| `topic` | Yes | Kafka topic name |
| `type` | Yes | `LOG` or `METRIC` |
| `output` | Yes | Absolute path to the output file — must exist before app starts |
| `alertKeywords` | No | Keywords that trigger an alert (LOG topics only, case-insensitive) |
| `patternStoreFile` | No | Absolute path to the pattern store file for alert deduplication (LOG topics only) — must exist before app starts |

---

## Topic Types

### LOG

- Each message is expected to be a JSON `LogEvent` with fields: `serverName`, `path`, `topic`, `timestamp`, `message`
- The `message` field is written to the output `.log` file, one line per Kafka record
- If any `alertKeywords` match the message (case-insensitive), the record is saved to the `alert_logs` database table and queued for a Telegram notification

### METRIC

- Each message is expected to be a JSON `ServerStorageSnapshot`
- Written to the output `.json` file in pretty-printed JSON format
- Saved atomically to `server_storage_snapshot` and `mount_path_storage_usage` in a single DB transaction

---

## Alert Deduplication

When `patternStoreFile` is set for a LOG topic, the app deduplicates alerts by log pattern before sending to Telegram or saving to the database.

### How it works

On each alert, the message is normalized into a pattern by replacing variable tokens with placeholders:

| Token type | Example input | Normalized |
|---|---|---|
| UUID | `550e8400-e29b-41d4-a716` | `<UUID>` |
| IP with port | `192.168.1.5:5432` | `<IP>:<PORT>` |
| IP | `192.168.1.5` | `<IP>` |
| Hex (0x prefix) | `0xdeadbeef` | `<HEX>` |
| Bare hex (8+ chars) | `deadbeef1234` | `<HEX>` |
| key=value pair | `host=db-server` | `host=<VAL>` |
| Token starting with digit | `30s`, `95%`, `2048MB` | `<TOKEN>` |

The result is lowercased and whitespace-normalized. Example:

```
Input:   could not connect to 192.168.1.5:5432 after 30s retries=3
Pattern: could not connect to <IP>:<PORT> after <TOKEN> retries=<VAL>
```

### Deduplication behavior

- First occurrence of a pattern → saved to DB + sent to Telegram + pattern appended to `patternStoreFile`
- Subsequent occurrences of the same pattern → suppressed (no DB write, no Telegram), logged locally as `[SUPPRESSED]`
- Patterns are loaded into memory at startup from `patternStoreFile` and checked in O(1) via `HashSet`
- The pattern store survives app restarts — patterns persist on disk
- If the pattern store reaches 10,000 entries a warning is logged, which may indicate a normalization miss

### Resetting suppressed alerts

To allow a suppressed alert pattern to fire again, clear or edit `patternStoreFile` on the server and restart the app:

```bash
# Clear all suppressed patterns
> /data/patterns/app-logs.txt

# Restart the app to reload the now-empty pattern store
```

### Deduplication disabled

If `patternStoreFile` is not set for a topic, every alert is sent to Telegram and saved to DB as normal. No deduplication overhead is added.

---

## Database Schema

### `alert_logs`

| Column | Type |
|---|---|
| `id` | BIGINT (PK, auto-increment) |
| `topic` | VARCHAR |
| `server_name` | VARCHAR |
| `file_path` | VARCHAR |
| `event_timestamp` | DATETIME |
| `message` | TEXT |

### `server_storage_snapshot`

| Column | Type |
|---|---|
| `id` | BIGINT (PK, auto-increment) |
| `system_id` | VARCHAR |
| `system_name` | VARCHAR |
| `server_ip` | VARCHAR |
| `server_name` | VARCHAR |
| `collected_at` | DATETIME |

### `mount_path_storage_usage`

| Column | Type |
|---|---|
| `id` | BIGINT (PK, auto-increment) |
| `server_storage_snapshot_id` | BIGINT (FK) |
| `path` | VARCHAR |
| `total_bytes` | BIGINT |
| `used_bytes` | BIGINT |
| `used_percent` | DOUBLE |

---

## Runtime Behavior

- One Kafka consumer thread per topic
- Manual offset commit — offsets are committed only after the batch is fully written to the output file and all required DB writes succeed
- Output file is opened, written, and closed per poll batch — the file is never held open between polls, so log rotation scripts (`> file`, `cat /dev/null > file`) can safely clear the output file at any time without conflicting with the app
- Up to 5 Telegram alerts queued per poll batch (prevents log-storm flooding)
- Telegram sends are rate-limited to 1 per 3 seconds; adaptive backoff doubles the interval on each 429 response (up to 60 seconds), resets to baseline on success
- Graceful shutdown on `CTRL+C`: consumers stop, in-flight Kafka commits complete, and the app waits briefly for queued Telegram alerts to drain

---

## File Safety Rules

- The app never creates, deletes, or moves any file
- The app only reads or appends to paths explicitly declared in the config
- If any configured output file or pattern store file does not exist at startup, the app aborts immediately with a clear error message
- Output files must be created manually before the app starts (see [Before First Run](#before-first-run))

---

## Notes

- Do not put real credentials in committed config files. Keep the checked-in classpath `consumer_config.json` as a blank/template config
- Kafka topics must exist before the application starts (or broker auto-creation must be enabled)
- The database schema must be created manually before first run
- The application does not expose any HTTP endpoints

---

## License

Specify your license here (e.g. Internal, Proprietary, Apache 2.0).
