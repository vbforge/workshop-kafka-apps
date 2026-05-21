# 🚀 Apache Kafka KRaft Mode Startup configurations - Complete Windows 11 Guide

> **My Setup:** `C:\Soft\develop\kafka_2.13-4.1.0`  
> **Kafka Version:** 4.1.0 (KRaft mode - no Zookeeper needed!)

---

## 📋 Table of Contents

1. [Understanding KRaft Mode](#understanding-kraft-mode)
2. [One-Time Initial Setup](#one-time-initial-setup)
3. [Daily Usage (Start/Stop)](#daily-usage-startstop)
4. [Complete Reset (When Needed)](#complete-reset-when-needed)
5. [Helper Batch Scripts](#helper-batch-scripts)
6. [Troubleshooting](#troubleshooting)
7. [Quick Command Reference](#quick-command-reference)

---

## understanding-kraft-mode

### What is KRaft?
- **KRaft** = **K**afka **Raft** (consensus protocol)
- **New in Kafka 3.0+**, production-ready since Kafka 3.3
- **No Zookeeper needed!** Kafka manages itself
- Simpler setup, faster, more reliable

### Traditional vs KRaft

| Traditional Kafka | KRaft Mode |
|-------------------|------------|
| Kafka + Zookeeper (2 processes) | Kafka only (1 process) |
| Complex setup | Simple setup |
| Zookeeper manages metadata | Kafka manages itself |
| Legacy | Modern approach ✨ |

---

## one-time-initial-setup

> ⚠️ **Do this ONLY ONCE when setting up Kafka for the first time!**

### Step 1: Configure KRaft Mode ⚙️

**File Location:**
```
C:\Soft\develop\kafka_2.13-4.1.0\config\server.properties
```

**Open the file and verify/add these settings:**

```properties
# Node ID - unique identifier for this Kafka broker
node.id=1

# Directory where Kafka will store logs and data
log.dirs=C:/Soft/develop/kafka_2.13-4.1.0/kafka-logs

# This server acts as both broker (handles messages) and controller (manages cluster)
process.roles=broker,controller

# Controller communication settings
controller.listener.names=CONTROLLER
controller.quorum.voters=1@localhost:9093

# Network listeners - how clients connect
listeners=PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093
inter.broker.listener.name=PLAINTEXT
advertised.listeners=PLAINTEXT://localhost:9092
```

**What these settings mean:**
- `node.id=1` - Your Kafka broker identifier (use 1 for single node)
- `log.dirs` - Where Kafka stores all data (topics, messages, metadata)
- `process.roles=broker,controller` - This node does everything (good for local dev)
- `listeners` - Port 9092 for clients, port 9093 for internal control
- `advertised.listeners` - How clients should connect (localhost:9092)

---

### Step 2: Generate Cluster ID 🔑

**Open Command Prompt:**
```cmd
cd C:\Soft\develop\kafka_2.13-4.1.0
.\bin\windows\kafka-storage.bat random-uuid
```

**Example Output:**
```
dvlf8cKfTceHTB7oYkRnw
```

**❗ IMPORTANT:** Save this ID! You'll need it in the next step.

**What is Cluster ID?**
- A unique identifier for your Kafka cluster
- Required for KRaft metadata initialization
- Generate once, use forever (unless you reset)

---

### Step 3: Format Storage (Initialize Metadata) 📦

**Use the UUID from Step 2:**
```cmd
.\bin\windows\kafka-storage.bat format -t dvlf8cKfTceHTB7oYkRnw -c .\config\server.properties
```

**Example Output:**
```
Formatting /kafka-logs with metadata.version 3.6-IV2
```

**What this does:**
- Creates the `kafka-logs` directory structure
- Initializes KRaft metadata
- Prepares Kafka for first startup

⚠️ **WARNING:** This command **wipes all existing data**. Only run when:
- First time setup ✅
- Resetting cluster (deleting all topics) ✅
- Never during normal operation ❌

---

### Step 4: Start Kafka Server 🟢

```cmd
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

**Look for this success message:**
```
INFO [KafkaServer id=1] started (kafka.server.KafkaServer)
```

**What you'll see:**
- Lots of INFO logs (normal)
- "started" message (success!)
- Server keeps running (don't close the window)

---

### Step 5: Verify Kafka is Running ✅

**Open a NEW Command Prompt window and test:**

```cmd
cd C:\Soft\develop\kafka_2.13-4.1.0

REM Create a test topic
.\bin\windows\kafka-topics.bat --create --topic hello-kafka --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092

REM List topics (should show hello-kafka)
.\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092
```

**Expected Output:**
```
Created topic hello-kafka.

hello-kafka
```

🎉 **Congratulations! Kafka is running!**

---

## daily-usage-startstop

> 💡 After initial setup, this is what you do every day!

### Starting Kafka (Normal Startup)

**Method 1: Manual Start**
```cmd
cd C:\Soft\develop\kafka_2.13-4.1.0
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

**Method 2: Using Batch Script (Recommended)**
- Double-click `start-kafka.bat` (see [Helper Scripts](#helper-batch-scripts))
- Opens in new window automatically

**When to use:**
- Every time you restart your computer
- After shutting down Kafka
- Starting your work session

**What happens:**
- Kafka loads existing data from `kafka-logs`
- All topics and messages are preserved
- Picks up where it left off

---

### Stopping Kafka

**Method 1: Graceful Shutdown (Recommended)**
- Go to the Kafka window
- Press `Ctrl + C`
- Wait for "Kafka Server shut down completed"

**Method 2: Force Stop (If needed)**
```cmd
taskkill /F /FI "WINDOWTITLE eq Kafka Server*" /T
```

**Method 3: Using Batch Script**
- Run `stop-kafka.bat` (see [Helper Scripts](#helper-batch-scripts))

---

## complete-reset-when-needed

> ⚠️ **WARNING: This deletes ALL topics, messages, and consumer offsets!**

### When to Reset?
- Want to start completely fresh
- Corrupted data
- Learning/testing and want clean slate
- NOT for normal operations!

### Reset Process

**Option A: Manual Reset**

1. **Stop Kafka** (Ctrl+C or taskkill)

2. **Delete logs directory:**
```cmd
cd C:\Soft\develop\kafka_2.13-4.1.0
rmdir /S /Q kafka-logs
```

3. **Generate new Cluster ID:**
```cmd
.\bin\windows\kafka-storage.bat random-uuid
REM Copy the output UUID
```

4. **Format with new UUID:**
```cmd
.\bin\windows\kafka-storage.bat format -t YOUR_NEW_UUID -c .\config\server.properties
```

5. **Start Kafka:**
```cmd
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

**Option B: Using Batch Script (Easier)**
- Run `reset-kafka.bat` (see [Helper Scripts](#helper-batch-scripts))
- Then run `start-kafka.bat`

---

## helper-batch-scripts

Create these `.bat` files in your Kafka root folder: `C:\Soft\develop\kafka_2.13-4.1.0`

### 🟢 start-kafka.bat

```batch
@echo off
REM ============================================
REM  Start Kafka Server (KRaft Mode)
REM ============================================

echo.
echo ========================================
echo   Starting Apache Kafka (KRaft Mode)
echo ========================================
echo.

cd /d C:\Soft\develop\kafka_2.13-4.1.0

REM Check if Kafka is already running
tasklist /FI "WINDOWTITLE eq Kafka Server*" 2>NUL | find /I /N "cmd.exe">NUL
if "%ERRORLEVEL%"=="0" (
    echo [WARNING] Kafka appears to be already running!
    echo Close existing Kafka window first, or use stop-kafka.bat
    echo.
    pause
    exit /b 1
)

echo Starting Kafka in new window...
start "Kafka Server" cmd /k ".\bin\windows\kafka-server-start.bat .\config\server.properties"

echo.
echo [SUCCESS] Kafka is starting...
echo.
echo Check the new window for startup messages.
echo Look for: "INFO [KafkaServer id=1] started"
echo.
echo To stop: Close the Kafka window or run stop-kafka.bat
echo.
pause
```

**Usage:**
- Double-click `start-kafka.bat`
- Opens Kafka in separate window
- Checks if already running

---

### 🔴 stop-kafka.bat

```batch
@echo off
REM ============================================
REM  Stop Kafka Server
REM ============================================

echo.
echo ========================================
echo   Stopping Apache Kafka
echo ========================================
echo.

REM Find and kill Kafka process
echo Looking for Kafka processes...

REM More targeted approach - finds the specific Kafka window
tasklist /FI "WINDOWTITLE eq Kafka Server*" 2>NUL | find /I /N "cmd.exe">NUL
if "%ERRORLEVEL%"=="0" (
    echo Found Kafka Server process. Stopping...
    taskkill /F /FI "WINDOWTITLE eq Kafka Server*" /T
    echo.
    echo [SUCCESS] Kafka has been stopped.
) else (
    echo [INFO] Kafka Server window not found.
    echo If Kafka is running, close it manually or use Ctrl+C in its window.
)

echo.
pause
```

**Usage:**
- Double-click `stop-kafka.bat`
- Stops Kafka gracefully
- Safe to run even if Kafka not running

---

### 🔄 reset-kafka.bat

```batch
@echo off
REM ============================================
REM  RESET Kafka Cluster (DELETES ALL DATA)
REM ============================================

echo.
echo =========================================
echo   KAFKA CLUSTER RESET - DANGER ZONE
echo =========================================
echo.
echo WARNING: This will DELETE:
echo   - All topics
echo   - All messages
echo   - All consumer offsets
echo   - All metadata
echo.
echo This action CANNOT be undone!
echo.

set /p CONFIRM="Type 'YES' to confirm reset: "
if /i not "%CONFIRM%"=="YES" (
    echo.
    echo [CANCELLED] Reset cancelled. No changes made.
    echo.
    pause
    exit /b 0
)

cd /d C:\Soft\develop\kafka_2.13-4.1.0

echo.
echo [STEP 1/4] Checking for running Kafka...
tasklist /FI "WINDOWTITLE eq Kafka Server*" 2>NUL | find /I /N "cmd.exe">NUL
if "%ERRORLEVEL%"=="0" (
    echo Kafka is running. Stopping it first...
    taskkill /F /FI "WINDOWTITLE eq Kafka Server*" /T
    timeout /t 3 /nobreak >NUL
)

echo.
echo [STEP 2/4] Deleting kafka-logs directory...
if exist kafka-logs (
    rmdir /S /Q kafka-logs
    echo Deleted kafka-logs successfully.
) else (
    echo kafka-logs directory not found (already clean).
)

echo.
echo [STEP 3/4] Generating new Cluster UUID...
for /f "delims=" %%i in ('.\bin\windows\kafka-storage.bat random-uuid') do set CLUSTER_ID=%%i
echo.
echo Generated Cluster ID: %CLUSTER_ID%

echo.
echo [STEP 4/4] Formatting storage with new metadata...
.\bin\windows\kafka-storage.bat format -t %CLUSTER_ID% -c .\config\server.properties

echo.
echo =========================================
echo   RESET COMPLETE
echo =========================================
echo.
echo Kafka cluster has been reset.
echo.
echo Next step: Run start-kafka.bat to start fresh.
echo.
pause
```

**Usage:**
- Double-click `reset-kafka.bat`
- Confirms before deletion
- Complete automated reset
- Use ONLY when you want to start fresh!

---

### 📊 check-kafka.bat

```batch
@echo off
REM ============================================
REM  Check Kafka Status
REM ============================================

echo.
echo ========================================
echo   Kafka Status Check
echo ========================================
echo.

cd /d C:\Soft\develop\kafka_2.13-4.1.0

REM Check if Kafka process is running
tasklist /FI "WINDOWTITLE eq Kafka Server*" 2>NUL | find /I /N "cmd.exe">NUL
if "%ERRORLEVEL%"=="0" (
    echo [STATUS] Kafka Server: RUNNING ^(green light^)
) else (
    echo [STATUS] Kafka Server: NOT RUNNING ^(red light^)
    echo.
    echo To start: Run start-kafka.bat
    echo.
    pause
    exit /b 1
)

echo.
echo Testing connection to Kafka...
echo.

REM Try to list topics (if Kafka is accessible)
.\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092 2>NUL
if "%ERRORLEVEL%"=="0" (
    echo.
    echo [SUCCESS] Kafka is running and accessible!
    echo Bootstrap server: localhost:9092
) else (
    echo.
    echo [WARNING] Kafka process found but not responding.
    echo It might still be starting up. Wait a moment and try again.
)

echo.
pause
```

**Usage:**
- Double-click `check-kafka.bat`
- Shows if Kafka is running
- Lists all topics
- Verifies connectivity

---

## troubleshooting

### Problem: "Connection refused" when trying to connect

**Symptoms:**
```
Error: Connection to node -1 (localhost/127.0.0.1:9092) could not be established
```

**Solutions:**
1. **Check if Kafka is running:**
   ```cmd
   tasklist | find "java.exe"
   ```

2. **Check Kafka logs:**
   - Look in the Kafka window for errors
   - Check if port 9092 is in use

3. **Verify port availability:**
   ```cmd
   netstat -ano | findstr :9092
   ```

4. **Restart Kafka:**
   - Run `stop-kafka.bat`
   - Wait 5 seconds
   - Run `start-kafka.bat`

---

### Problem: "Kafka won't start" or crashes immediately

**Symptoms:**
- Kafka window closes immediately
- Error messages about "metadata" or "log directory"

**Solutions:**

1. **Check if logs directory exists:**
   ```cmd
   dir C:\Soft\develop\kafka_2.13-4.1.0\kafka-logs
   ```

2. **If logs directory is missing, format storage:**
   ```cmd
   .\bin\windows\kafka-storage.bat random-uuid
   REM Copy the UUID output
   
   .\bin\windows\kafka-storage.bat format -t YOUR_UUID -c .\config\server.properties
   ```

3. **Check server.properties configuration:**
   - Verify `log.dirs` path is correct
   - Ensure no typos in configuration

4. **Last resort - complete reset:**
   - Run `reset-kafka.bat`
   - Then `start-kafka.bat`

---

### Problem: Port 9092 already in use

**Symptoms:**
```
Error: Address already in use (Bind failed)
```

**Solutions:**

1. **Find what's using port 9092:**
   ```cmd
   netstat -ano | findstr :9092
   ```

2. **Kill the process:**
   ```cmd
   taskkill /F /PID <PID_NUMBER>
   ```

3. **Or change Kafka port in server.properties:**
   ```properties
   listeners=PLAINTEXT://localhost:9093,CONTROLLER://localhost:9094
   advertised.listeners=PLAINTEXT://localhost:9093
   ```

---

### Problem: "Insufficient memory" errors

**Symptoms:**
- OutOfMemoryError in logs
- Kafka crashes randomly

**Solution:**

Create `kafka-server-start-custom.bat`:
```batch
@echo off
cd /d C:\Soft\develop\kafka_2.13-4.1.0

REM Allocate less memory (1GB heap instead of default)
set KAFKA_HEAP_OPTS=-Xmx1G -Xms1G

.\bin\windows\kafka-server-start.bat .\config\server.properties
```

---

### Problem: Topics disappeared after restart

**Cause:** You ran `format` command again after initial setup

**Solution:**
- ⚠️ Data is GONE if you formatted again
- For future: NEVER run `format` unless resetting intentionally
- Normal startups don't need `format`, just start the server

---

## quick-command-reference

### Essential Kafka Commands

**Always run from Kafka directory:**
```cmd
cd C:\Soft\develop\kafka_2.13-4.1.0
```

#### Topic Management

```cmd
REM Create topic
.\bin\windows\kafka-topics.bat --create ^
  --topic my-topic ^
  --partitions 3 ^
  --replication-factor 1 ^
  --bootstrap-server localhost:9092

REM List all topics
.\bin\windows\kafka-topics.bat --list ^
  --bootstrap-server localhost:9092

REM Describe topic
.\bin\windows\kafka-topics.bat --describe ^
  --topic my-topic ^
  --bootstrap-server localhost:9092

REM Delete topic
.\bin\windows\kafka-topics.bat --delete ^
  --topic my-topic ^
  --bootstrap-server localhost:9092

REM Increase partitions (cannot decrease!)
.\bin\windows\kafka-topics.bat --alter ^
  --topic my-topic ^
  --partitions 5 ^
  --bootstrap-server localhost:9092
```

#### Producer/Consumer Testing

```cmd
REM Start console producer
.\bin\windows\kafka-console-producer.bat ^
  --topic my-topic ^
  --bootstrap-server localhost:9092

REM Start console producer with keys
.\bin\windows\kafka-console-producer.bat ^
  --topic my-topic ^
  --bootstrap-server localhost:9092 ^
  --property "parse.key=true" ^
  --property "key.separator=:"

REM Start console consumer (from beginning)
.\bin\windows\kafka-console-consumer.bat ^
  --topic my-topic ^
  --from-beginning ^
  --bootstrap-server localhost:9092

REM Start console consumer (showing keys)
.\bin\windows\kafka-console-consumer.bat ^
  --topic my-topic ^
  --from-beginning ^
  --bootstrap-server localhost:9092 ^
  --property print.key=true ^
  --property key.separator=:

REM Consumer with specific group
.\bin\windows\kafka-console-consumer.bat ^
  --topic my-topic ^
  --group my-consumer-group ^
  --bootstrap-server localhost:9092
```

#### Consumer Group Management

```cmd
REM List all consumer groups
.\bin\windows\kafka-consumer-groups.bat ^
  --list ^
  --bootstrap-server localhost:9092

REM Describe consumer group
.\bin\windows\kafka-consumer-groups.bat ^
  --describe ^
  --group my-consumer-group ^
  --bootstrap-server localhost:9092

REM Reset consumer group offset to beginning
.\bin\windows\kafka-consumer-groups.bat ^
  --bootstrap-server localhost:9092 ^
  --group my-consumer-group ^
  --reset-offsets ^
  --to-earliest ^
  --topic my-topic ^
  --execute

REM Reset to specific offset
.\bin\windows\kafka-consumer-groups.bat ^
  --bootstrap-server localhost:9092 ^
  --group my-consumer-group ^
  --reset-offsets ^
  --to-offset 10 ^
  --topic my-topic:0 ^
  --execute

REM Reset to latest (skip all)
.\bin\windows\kafka-consumer-groups.bat ^
  --bootstrap-server localhost:9092 ^
  --group my-consumer-group ^
  --reset-offsets ^
  --to-latest ^
  --topic my-topic ^
  --execute
```

#### Topic Configuration

```cmd
REM View topic configuration
.\bin\windows\kafka-configs.bat ^
  --bootstrap-server localhost:9092 ^
  --entity-type topics ^
  --entity-name my-topic ^
  --describe

REM Modify retention time (1 hour = 3600000 ms)
.\bin\windows\kafka-configs.bat ^
  --bootstrap-server localhost:9092 ^
  --entity-type topics ^
  --entity-name my-topic ^
  --alter ^
  --add-config retention.ms=3600000

REM Delete configuration
.\bin\windows\kafka-configs.bat ^
  --bootstrap-server localhost:9092 ^
  --entity-type topics ^
  --entity-name my-topic ^
  --alter ^
  --delete-config retention.ms
```

---

## 🎯 Common Workflows

### Workflow 1: First Time Setup
```
1. Configure server.properties (Step 1)
2. Generate UUID (Step 2)
3. Format storage (Step 3)
4. Start Kafka (Step 4)
5. Test with sample topic (Step 5)
```

### Workflow 2: Daily Development
```
1. Run start-kafka.bat
2. Wait for "started" message
3. Do your work (create topics, run apps)
4. When done: Ctrl+C in Kafka window
```

### Workflow 3: Project Reset
```
1. Run stop-kafka.bat
2. Run reset-kafka.bat (confirm with YES)
3. Run start-kafka.bat
4. All topics and data are gone
```

### Workflow 4: System Restart
```
1. After reboot, just run start-kafka.bat
2. All previous topics and data are preserved
3. No need to format or configure again
```

---

## 📊 Understanding Kafka Directories

```
C:\Soft\develop\kafka_2.13-4.1.0\
│
├── bin\
│   └── windows\          # Windows batch scripts
│       ├── kafka-server-start.bat
│       ├── kafka-topics.bat
│       └── ...
│
├── config\
│   ├── server.properties # Main configuration file
│   └── ...
│
├── kafka-logs\           # DATA DIRECTORY (created after format)
│   ├── __cluster_metadata-0\  # KRaft metadata
│   ├── my-topic-0\       # Topic partition data
│   ├── my-topic-1\
│   └── ...
│
├── logs\                 # Application logs (not data)
│   └── server.log
│
└── libs\                 # Kafka libraries
```

**Important:**
- `kafka-logs/` = Your data (topics, messages)
- `logs/` = Application logs (debugging)
- Deleting `kafka-logs/` = Deleting all data
- Deleting `logs/` = Safe, just logging

---

## ✅ Best Practices

### DO ✅
- Use `start-kafka.bat` for convenience
- Keep Kafka window open while working
- Stop Kafka gracefully (Ctrl+C)
- Back up important topics before reset
- Read error messages carefully

### DON'T ❌
- Don't run `format` command unless resetting
- Don't delete `kafka-logs` while Kafka is running
- Don't use `taskkill /F` unless necessary
- Don't ignore startup errors
- Don't run multiple Kafka instances on same port

---

## 🎓 Summary

| Task | When | Command |
|------|------|---------|
| **Initial Setup** | Once ever | Steps 1-5 (configure, format, start) |
| **Start Kafka** | Every session | `start-kafka.bat` or `kafka-server-start.bat` |
| **Stop Kafka** | End of session | `Ctrl+C` or `stop-kafka.bat` |
| **Reset Everything** | When needed | `reset-kafka.bat` then `start-kafka.bat` |
| **Check Status** | Anytime | `check-kafka.bat` |

---

## 🆘 Quick Help

**Kafka won't start?** → Check server.properties and run format if needed  
**Connection refused?** → Kafka not running, use start-kafka.bat  
**Port in use?** → Kill process or change port in config  
**Topics disappeared?** → Did you run format? Data is gone if so  
**Need fresh start?** → Use reset-kafka.bat

---

## 📚 Additional Resources

- **Apache Kafka Documentation:** https://kafka.apache.org/documentation/
- **KRaft Mode Details:** https://kafka.apache.org/documentation/#kraft
- **Your Learning Project:** Use the Java scenarios provided!

---

**Happy Kafka Learning! 🚀**

*Keep this guide handy for quick reference!*
