# AzsbEmu — Azure Service Bus Local Emulator

**AzsbEmu** is a lightweight, high-performance, and fully persistent local emulator for Azure Service Bus. It is built in C# for .NET 10 to overcome the entity limits and state-recovery gaps of alternative options, making it ideal for local development, rapid developer feedback, and automated integration testing pipelines.

---

## Key Features

- **Unlimited Entities:** Support for 500+ active queues, topics, and subscriptions without runtime degradation.
- **Durable Persistence:** Survives application restarts and system crashes.
  - **Metadata (SQLite):** Durable, transaction-protected metadata storage for entity definitions and subscription rule filters.
  - **Message Streams (Microsoft FASTER Log):** Persistent, append-only, high-performance stream storage serving as the sole source of truth for message data.
- **Feature Compatibility Scope:**
  - **Dynamic Entity Management ($management):** Create and delete queues, topics, and subscriptions dynamically at runtime without restarting.
  - **Basic Messaging:** Send, receive, PeekLock, receive-and-delete, and standard correlation or SQL filters.
  - **Scheduled Messages:** Schedule delayed deliveries (`com.microsoft:schedule-message` and `x-opt-scheduled-enqueue-time` annotation sends).
  - **Deferred Messages:** Deferred message delivery and retrieval by sequence number (`com.microsoft:receive-by-sequence-number`).
  - **Expiration / TTL:** Automated message time-to-live enforcement with background dead-lettering (`TTLExpiredException`).
  - **Claims-Based Security ($cbs):** Integrated SASL Claims-Based Security authentication handshake.

---

## Getting Started

### 1. Connection String
To connect your Azure SDK clients (`Azure.Messaging.ServiceBus` in .NET, Java, Python, or JS) to the emulator, configure your client using the following local connection string:

```text
Endpoint=sb://localhost/;SharedAccessKeyName=Root;SharedAccessKey=dummy;UseDevelopmentEmulator=true
```

*(Note: When running on a custom port like `5672`, standard SDK clients automatically resolve it. No port overrides are required unless you change it via the port options below).*

---

### 2. Configuration & CLI Parameters
AzsbEmu can be configured using environment variables (prefixed with `AZSB_EMU_`), command-line arguments, or `appsettings.json`.

| CLI Option | Environment Variable | Default Value | Description |
| :--- | :--- | :--- | :--- |
| `--host` | `AZSB_EMU_HOST` | `localhost` | The host address to listen on. |
| `--port` | `AZSB_EMU_PORT` | `5672` | The port address to listen on. |
| `--data-dir` | `AZSB_EMU_DATA_DIR` | `%LOCALAPPDATA%/AzsbEmu` | Path where SQLite database and FASTER logs are persisted. |

---

### 3. Deployments & Running

#### Option A: Running from Source (.NET 10 SDK)
To start the emulator directly from your local source code:
```powershell
dotnet run --project AzsbEmu.Server/AzsbEmu.Server.csproj
```

#### Option B: Self-Contained Executable (Windows)
For standalone distribution without requiring .NET installed, compile a single-file executable:
```powershell
# Compile the single-file binary
dotnet publish AzsbEmu.Server/AzsbEmu.Server.csproj -r win-x64 -c Release --self-contained true -p:PublishSingleFile=true -p:PublishReadyToRun=true -o ./publish/win-x64

# Start the executable directly
./publish/win-x64/AzsbEmu.Server.exe
```

#### Option C: Containerized (Docker)
Build and run the emulator inside a lightweight, sandboxed container:
```powershell
# Build the docker image
docker build -t azsbemu:latest .

# Run the container with a persistent directory mount
docker run -d -p 5672:5672 -v azsbemu-data:/app/data --name azsbemu azsbemu:latest
```

---

## Development & Verification

### Running the Test Suite
The solution contains extensive xUnit integration tests covering message serialization, scheduled deliveries, dynamic CRUD topology changes, TTL expiration, and recovery:
```powershell
dotnet test AzsbEmu.slnx
```

---

## Known Functional Gaps & Future Roadmap
- **Phase 4 Target:** Message Sessions (`RequiresSession = true` support, accept-session lock logic, and session state tracking).
- **Phase 4 Target:** Lock Renewal (`com.microsoft:renew-lock`).
- **Phase 4 Target:** Distributed Transaction Support (`$coordinator` transaction controller).
