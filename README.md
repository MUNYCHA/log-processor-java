
---

# 🪵 Kafka Log Consumer

### Multi-Topic Processing • Telegram Alerts • MySQL Logging • Topic Validation

This application consumes multiple Kafka topics in real time, writes their messages to log files, sends Telegram alerts when important keywords appear, stores alert records in MySQL, **and validates Kafka topics at startup**.

Every topic is handled in its own thread for fast, parallel log processing.

---

# ⚙️ How It Works

1. The application loads **all settings from `config.json`**:

   * Kafka bootstrap servers
   * Telegram bot token & chat ID
   * List of topics + output log file paths
   * Alert keywords
   * MySQL connection settings

2. **KafkaTopicValidator** checks if all configured topics exist.

   * If any topic is missing → ❌ program stops
   * Protects from silent failures

3. For each topic:

   * A Kafka consumer is created using **KafkaConsumerFactory**
   * A new thread is started (`TopicConsumer`)
   * Each received message is:

      * Written to a log file
      * Checked for alert keywords
      * If keyword found:

         * 📢 Telegram alert sent
         * 🗄 Record saved to MySQL

4. Everything is configurable.
   **No Java code changes needed** to add or remove topics, keywords, or alerts.

---

# 📁 Configuration Files

## ✔ `config.example.json` (included in Git)

```json
{
   "bootstrapServers": "YOUR_KAFKA_BOOTSTRAP_SERVER",

   "telegramBotToken": "YOUR_TELEGRAM_BOT_TOKEN",
   "telegramChatId": "YOUR_TELEGRAM_CHAT_ID",

   "topics": [
      { "topic": "app1-topic", "output": "/path/to/logs/received_app1.log" },
      { "topic": "app2-topic", "output": "/path/to/logs/received_app2.log" },
      { "topic": "app3-topic", "output": "/path/to/logs/received_app3.log" },
      { "topic": "app4-topic", "output": "/path/to/logs/received_app4.log" },
      { "topic": "system-topic", "output": "/path/to/logs/received_system.log" },
      { "topic": "server-topic", "output": "/path/to/logs/received_server.log" }
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
      "url": "jdbc:mysql://YOUR_DB_HOST:3306/YOUR_DATABASE_NAME",
      "user": "YOUR_DATABASE_USER",
      "password": "YOUR_DATABASE_PASSWORD",
      "table": "alert_logs"
   }
}
```

### Copy and customize:

```
cp src/main/resources/config.example.json src/main/resources/config.json
```

Modify:

* Kafka server
* Telegram token & chat ID
* Topic/output paths
* Alert keywords
* DB credentials

### ❗ `config.json` is ignored by Git

Sensitive credentials remain private.

---

# 🔔 Extensible Alert Keywords

Alert words are configured through `config.json`.

Example:

```json
"alertKeywords": [
  "error",
  "panic",
  "service unavailable",
  "memory leak",
  "disconnect",
  "unauthorized"
]
```

Add or remove keywords anytime → restart app → done.

---

# 🗄 MySQL Alert Logging

When a message matches a keyword, it's saved into `alert_logs`.

### Table schema:

```sql
CREATE TABLE alert_logs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    topic VARCHAR(255) NOT NULL,
    timestamp DATETIME NOT NULL,
    message TEXT NOT NULL
);
```

Stored fields:

| Column      | Description                   |
| ----------- | ----------------------------- |
| `topic`     | Kafka topic name              |
| `timestamp` | Time the message was consumed |
| `message`   | Content of the log message    |

---

# ▶️ Get Your Telegram Chat ID

1. Open Telegram
2. Search for: **@userinfobot**
3. Start the bot
4. Copy the value of "Your chat ID"
5. Paste into `config.json`

---

# 🧰 Requirements

* Java **17+**
* Apache Kafka running
* MySQL server running
* Internet access (Telegram API)
* Kafka topics created (validated before startup)

---

# ▶️ Run Instructions

### 1. Build JAR

```
mvn clean package
```

### 2. Run application

```
java -jar target/kafkaConsumerApp-1.0.jar
```

Expected output:

```
Listening to app1-topic -> writing to /home/.../received_app1.log
[12:34:56] (app1-topic) ERROR Something bad happened

```

---

# 📝 Logging Configuration (`simplelogger.properties`)

Located in:

```
src/main/resources/simplelogger.properties
```

Controls SLF4J logging:

```
org.slf4j.simpleLogger.defaultLogLevel=warn
org.slf4j.simpleLogger.log.org.apache.kafka=error
```

Helps suppress noisy Kafka internals and keeps console clean.

---

# 📂 Project Structure

```
src/
 └── main/
     ├── java/
     │   └── com/munycha/kafkaconsumer/
     │       ├── AppMain.java                         # Entry point
     │       ├── consumer/
     │       │   ├── TopicConsumer.java               # Handles 1 topic in its own thread
     │       │   └── KafkaConsumerFactory.java        # Creates KafkaConsumer instances
     │       ├── utility/
     │       │   └── KafkaTopicValidator.java         # Validates all topic names
     │       ├── config/
     │       │   ├── ConfigLoader.java                # Reads and parses config.json
     │       │   ├── ConfigData.java                  # Full JSON configuration model
     │       │   ├── TopicConfig.java                 # Topic + output path mapping
     │       │   └── DatabaseConfig.java              # DB credentials + table
     │       ├── telegram/
     │       │   └── TelegramNotifier.java            # Sends alert messages w/ rate limiting
     │       └── db/
     │           └── AlertDatabase.java               # Handles DB inserts
     └── resources/
         ├── config.example.json
         ├── config.json                              # User configuration (ignored by Git)
         └── simplelogger.properties                  # NEW: SLF4J logger configuration
```

---

# 🧩 Class Overview (UPDATED)

| Class                    | Purpose                                                                    |
| ------------------------ | -------------------------------------------------------------------------- |
| **AppMain**              | Loads config, validates topics, starts all TopicConsumers                  |
| **TopicConsumer**        | Listens to a Kafka topic, writes logs, triggers alerts, inserts DB records |
| **KafkaConsumerFactory** | NEW: Creates configured KafkaConsumer for each topic                       |
| **KafkaTopicValidator**  | NEW: Ensures all topics exist before starting                              |
| **ConfigLoader**         | Reads and parses config.json                                               |
| **ConfigData**           | Full config: Kafka, Telegram, DB, topics, keywords                         |
| **TopicConfig**          | Represents one topic → output mapping                                      |
| **DatabaseConfig**       | Holds MySQL connection settings                                            |
| **AlertDatabase**        | Inserts alert rows into MySQL                                              |
| **TelegramNotifier**     | Sends Telegram alerts with rate limiting                                   |

---

# 💡 Tips

* Add/remove Kafka topics instantly via `config.json`
* Add or modify alert keywords anytime
* Consumers run in parallel threads → high throughput
* Pairs perfectly with your Kafka File Log Producer
* Ideal for **real-time log monitoring + alerting**

---

# 🧑‍💻 Author

**Munycha**
Real-time Kafka Log Consumer — Multi-Topic • Alerts • DB Storage • Topic Validation

---