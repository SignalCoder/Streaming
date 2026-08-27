# Real-Time Fleet Telemetry Pipeline using Azure Event Hubs & Azure Functions

This document outlines a serverless, event-driven architecture designed to process high-throughput telemetry streams using **Azure Event Hubs** and **Azure Functions**.

---

## 🎯 The Business Use Case
A logistics company operates a fleet of **10,000 delivery trucks**. Each vehicle continuously broadcasts real-time telemetry data (GPS coordinates, current speed, engine temperature, and brake status) every second. 

### Core System Requirements:
1. **Massive Scale:** Ingest millions of incoming metrics concurrently without dropping packets.
2. **Low Latency Processing:** Analyze the telemetry stream instantly in real-time.
3. **Automated Alerts:** Automatically flag anomalies (e.g., engine temperature spiking above safe operational limits) and notify mechanics immediately.
4. **Data Offloading:** Save raw telemetry to a database for live fleet tracking dashboards.

---

## 🏛️ System Architecture Workflow

The system utilizes a decoupled, serverless pattern where compute scaling directly matches the ingestion load.

```
[ 🚛 10,000 Trucks ] 
         │
         ▼ (HTTPS / AMQP Stream)
┌────────────────────────────────────────┐
│         AZURE EVENT HUBS               │
│  [Part 0]  [Part 1]  [Part 2]  [Part 3]│ <── Scales dynamically to hold millions of messages
└────────────────────────────────────────┘
         │
         ▼ (Automatic Serverless Trigger)
┌────────────────────────────────────────┐
│         AZURE FUNCTIONS                │
│  [Func]    [Func]    [Func]    [Func]  │ <── Scales up instance-per-partition to match load
└────────────────────────────────────────┘
         │
         ├──(If Temp > 100°C)──> [ 📧 Azure Communication Services ] ──> Alert Mechanic
         │
         └──(All Data)─────────> [ 💾 Azure Cosmos DB ] ──> Fleet Live Map Dashboard
```

### End-to-End Processing Steps:
1. **Data Generation:** Truck sensors act as event producers, pushing raw JSON telemetry packets into Azure Event Hubs.
2. **Ingestion & Buffering:** Event Hubs acts as the ingestion buffer, distributing data across horizontal **Partitions** to preserve message ordering per truck.
3. **Serverless Triggering:** An Azure Function configured with an `EventHubTrigger` monitors the stream. The moment data enters a partition, the Azure runtime automatically invokes the function.
4. **Dynamic Elasticity:** During peak traffic hours, Azure Functions automatically instantiates multiple concurrent instances (up to one instance per partition) to handle heavy parallel computation.
5. **Business Logic Evaluation:** The function parses incoming events, triggers conditional mechanics alerts if critical thresholds are crossed, and passes the clean data onward to downstream databases.

---

## 💻 Python Reference Implementation

Azure Functions handles the underlying connection pooling, stream listening, and checkpointing out of the box. The framework passes the structured stream events straight into your primary function handler.

### `function_app.py`

```python
import logging
import azure.functions as func
import json

# Initialize the Function App client
app = func.FunctionApp()

@app.event_hub_message_trigger(
    arg_name="events", 
    event_hub_name="fleet-telemetry-hub",
    connection="EventHubConnectionString"
)
def main(events: list[func.EventHubEvent]):
    """
    Executes automatically in batches whenever the assigned Event Hub 
    partitions receive new truck telemetry data.
    """
    for event in events:
        try:
            # 1. Extract and parse raw byte payload from the event stream
            telemetry_bytes = event.get_body()
            telemetry_str = telemetry_bytes.decode('utf-8')
            data = json.loads(telemetry_str)
            
            truck_id = data.get("truck_id")
            engine_temp = data.get("engine_temp")
            
            logging.info(f"Processing data for Truck {truck_id}. Engine Temp: {engine_temp}°C")
            
            # 2. Evaluate alert thresholds
            if engine_temp and engine_temp > 100:
                logging.warning(f"⚠️ CRITICAL ALERT: Truck {truck_id} is overheating at {engine_temp}°C!")
                dispatch_mechanic_alert(truck_id, engine_temp)
                
        except json.JSONDecodeError:
            logging.error("Failed to parse event message payload. Invalid JSON formatting.")
        except Exception as e:
            logging.error(f"Error handling streaming telemetry event: {str(e)}")

def dispatch_mechanic_alert(truck_id: str, temperature: float):
    """
    Simulated helper method to trigger alert notifications via downstream communication systems.
    """
    # Integration logic for sending an automated SMS/Email alert to the dispatch team
    pass
```

---

## 🌟 Architectural Benefits

* **No Server Infrastructure Overhead:** Development teams manage zero virtual machines, operating system patches, or container orchestrators.
* **Granular Cost Efficiency:** Operating on a serverless consumption model implies paying only for the exact computational execution duration (measured to the millisecond). When vehicles stop running overnight, compute resource usage drops automatically to zero.
* **Built-in Resilience:** If a downstream database crashes or experiences a temporary outage, Event Hubs holds the telemetry data securely for its configured retention period (typically 1 to 7 days), preventing critical operational data loss.
