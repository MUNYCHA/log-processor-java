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
      "alertKeywords": ["error", "fatal", "exception", "timeout", "critical"]
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
| `output` | Yes | Absolute path to the output file |
| `alertKeywords` | No | List of keywords that trigger an alert (LOG topics only, case-insensitive) |

---

## Topic Types

### LOG

- Each message is expected to be a JSON `LogEvent` with fields: `serverName`, `path`, `topic`, `timestamp`, `message`
- Written to the output `.log` file as the raw `message` field, one line per Kafka record
- If any `alertKeywords` match the message (case-insensitive), the record is:
  - Saved to the `alert_logs` database table
  - Queued for a Telegram notification after the batch commits

### METRIC

- Each message is expected to be a JSON `ServerStorageSnapshot`
- Written to the output `.json` file in pretty-printed JSON format
- Saved atomically to `server_storage_snapshot` and `mount_path_storage_usage` in a single DB transaction

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
- Manual offset commit - offsets are committed after each successful poll batch is flushed to the output file and any required DB writes succeed
- Up to 5 Telegram alerts queued per poll batch (prevents log-storm flooding)
- Telegram sends are rate-limited to 1 per 3 seconds; adaptive backoff doubles the interval on each 429 response (up to 60 seconds), resets to baseline on success
- Output directories are created automatically if they do not exist
- Graceful shutdown on `CTRL+C`: consumers stop, in-flight Kafka commits complete, and the app waits briefly for queued Telegram alerts to drain

---

## Notes

- Do not put real credentials in committed config files. Keep the checked-in classpath `consumer_config.json` as a blank/template config
- Kafka topics must exist before the application starts (or broker auto-creation must be enabled)
- The database schema must be created manually before first run
- The application does not expose any HTTP endpoints

---

## License

Specify your license here (e.g. Internal, Proprietary, Apache 2.0).
