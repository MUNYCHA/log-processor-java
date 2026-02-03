
---

````md
# log-processor

**log-processor** is a **Java 8–compatible Kafka log processing application** that consumes log and metric data from **Apache Kafka** and processes it for storage, alerting, and analysis.

It provides:
- Continuous consumption of log and metric messages from Kafka topics
- File-based persistence of logs and metrics
- Keyword-based alert detection
- Alert notifications via Telegram
- Persistence of alerts and metrics to MySQL

---

## Requirements

- **Java:** 1.8 (Java 8)
- **Build tool:** Maven 3.6+
- **Kafka:** Reachable Kafka broker
- **Database:** MySQL 5.7+ / 8.x
- **OS:** Linux (paths and filesystem layout are Linux-oriented)

Verify Java:
```bash
java -version
````

---

## Build

```bash
mvn clean package
```

### Output

```text
target/log-processor.jar
```

---

## Run

```bash
java -jar /path/to/log-processor.jar --config=/path/to/config/config.json
```

### Example

```bash
java -jar target/log-processor.jar --config=./config/config.json
```

> The application requires an external JSON configuration file.

---

## Configuration Overview

The application is configured using **one JSON file**.

### Example: `config.json`

```json
{
  "bootstrapServers": "localhost:9092",

  "telegramBotToken": "<BOT_TOKEN>",
  "telegramChatId": "<CHAT_ID>",

  "topics": [
    {
      "topic": "app1-topic",
      "type": "LOG",
      "output": "/data/logs/received_app1.log"
    },
    {
      "topic": "app2-topic",
      "type": "LOG",
      "output": "/data/logs/received_app2.log"
    },
    {
      "topic": "app3-topic",
      "type": "LOG",
      "output": "/data/logs/received_app3.log"
    },
    {
      "topic": "app4-topic",
      "type": "LOG",
      "output": "/data/logs/received_app4.log"
    },
    {
      "topic": "server-topic",
      "type": "LOG",
      "output": "/data/logs/received_server.log"
    },
    {
      "topic": "system-topic",
      "type": "LOG",
      "output": "/data/logs/received_system.log"
    },
    {
      "topic": "server-storage-snapshot",
      "type": "METRIC",
      "output": "/data/logs/received_system_resources_daily.json"
    }
  ],

  "alertKeywords": [
    "error",
    "fail",
    "failure",
    "fatal",
    "exception",
    "timeout",
    "server error",
    "critical",
    "warn",
    "warning",
    "panic",
    "crash",
    "500",
    "404",
    "503"
  ],

  "database": {
    "url": "jdbc:mysql://localhost:3306/logDB",
    "user": "dbuser",
    "password": "dbpassword",

    "tables": {
      "alertLogTable": "alert_logs",
      "serverStorageSnapshotTable": "server_storage_snapshot",
      "mountPathStorageUsageTable": "mount_path_storage_usage"
    }
  }
}
```

---

## Configuration Details

### Kafka Connection

```json
"bootstrapServers": "localhost:9092"
```

* Kafka bootstrap server list
* Used by all Kafka consumers in the application

---

### Topics

Defines which Kafka topics are consumed and how messages are processed.

Fields:

* `topic`: Kafka topic name
* `type`: Message type (`LOG` or `METRIC`)
* `output`: Absolute path to the output file

#### Supported Types

* **LOG**

   * Plain or structured log messages
   * Scanned for alert keywords
   * Written to `.log` files

* **METRIC**

   * Structured metric data
   * Written as JSON
   * Persisted to database tables

---

### Alert Keywords

```json
"alertKeywords": [ "error", "fatal", "exception" ]
```

* Keywords used to detect alert conditions in log messages
* Matching is typically case-insensitive
* When matched:

   * An alert record is stored
   * A Telegram notification is sent

---

### Telegram Alerts

```json
"telegramBotToken"
"telegramChatId"
```

Used to send alert notifications when alert keywords are detected.

---

### Database Configuration

```json
"database": { ... }
```

The database is used to persist:

* Alert logs
* Server storage snapshots
* Mount path storage usage metrics

#### Tables

| Purpose                  | Table Name                 |
| ------------------------ | -------------------------- |
| Alert logs               | `alert_logs`               |
| Storage snapshot summary | `server_storage_snapshot`  |
| Mount path storage usage | `mount_path_storage_usage` |

---

## Output Files

All output paths must be **absolute paths**.

Examples:

```
/data/logs/received_app1.log
/data/logs/received_system_resources_daily.json
```

The application does not create directories automatically.

---

## Java Compatibility

* Compiled with **Java 8**
* Bytecode target: **Java 8**
* Runs on Java 8 runtime without additional flags

---

## Runtime Characteristics

* Single JVM process
* Long-running Kafka consumer
* Console-based application
* No HTTP endpoints
* Designed to be stopped with `CTRL+C`

---

## Notes

* Kafka topics must exist or auto-creation must be enabled
* Output directories must exist and be writable
* Database must be reachable at startup
* Secrets should not be committed to version control

---

## Typical Use Cases

* Centralized Kafka log processing
* Log-based alerting and monitoring
* Persisting infrastructure metrics
* Backend log analysis pipelines

---

## License

Specify your license here (e.g. Internal, Proprietary, Apache 2.0).

---

## Maintainer

* **Project:** log-processor
* **Runtime:** Java 8
* **Build:** Maven

```
