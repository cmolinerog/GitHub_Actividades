## Data Flow 1: Transaction Fraud Detection
 
### Pipeline Decision
- **Pipeline Type:** Streaming
- **Architecture:** ELT
- **Latency Target:** Sub-second
- **Target System:** Datalake (to store all the data) and Datawarehouse (for analytics)
 
### Justification
1. **Why this pipeline type?**
   - Need to  detect frauds fastly
 
2. **Why this architecture (ETL/ELT)?**
   - Need to load data quickly
 
3. **Data flow description:**
   - Source → Ingestion → Stream processing → Target
 
### Failure Strategy
- **What fails?** Data duplication, latency
- **Recovery plan:** Deduplication, 
- **Idempotency:** Yes
 
### Trade-offs
- **Chose streamming over batch because:** it adds latency
- **Risk of this approach:** operational complexity
- **Mitigation:** optimize the model


## Data Flow 2: Daily Financial Reporting
 
### Pipeline Decision
- **Pipeline Type:** Batch
- **Architecture:** ETL
- **Latency Target:** Dayly
- **Target System:** Datawarehouse
 
### Justification
1. **Why this pipeline type?**
   - Processing happens once per day, so real-time is unnecessary
 
2. **Why this architecture (ETL/ELT)?**
   - Only want to store clean data
 
3. **Data flow description:**
   - Source → Extraction → Transform (cleaning) → Load
 
### Failure Strategy
- **What fails?** calculation errors, incomplete run
- **Recovery plan:** Re-run batch, 
- **Idempotency:** yes, same input should have same output
 
### Trade-offs
- **Chose batch over streamming because:** it priotitizes accuracy
- **Risk of this approach:** Delayed availability of reports (only daily)
- **Mitigation:** Automated retries and alerting


## Data Flow 3: Customer 360 Profile
 
### Pipeline Decision
- **Pipeline Type:** Hybrid
- **Architecture:** Hybrid
- **Latency Target:** Subseconds for events and daily for analytics
- **Target System:** Datalake  + Datawarehouse 
 
### Justification
1. **Why this pipeline type?**
   - Mobile evventss need real time updates but batch for reporting
 
2. **Why this architecture (ETL/ELT)?**
   - ELT for raw data (event streams), and ETL for reports
 
3. **Data flow description:**
   - Source -> Extraction -> Load -> Trasnform (for streamming)
   - Source -> Extraction -> Transform -> Load (for batch)
 
### Failure Strategy
- **What fails?** Event duplication
- **Recovery plan:**  Deduplication
- **Idempotency:** Yes
 
### Trade-offs
- **Chose hybrid over streamming/batch because:** ensures real-time personalization and reliable analytical reporting
- **Risk of this approach:** Higher system complexity
- **Mitigation:** Clear separation of profile store and data lake


## Data Flow 4: Application Logs
 
### Pipeline Decision
- **Pipeline Type:** Streaming
- **Architecture:** ELT
- **Latency Target:** Subseconds
- **Target System:** Datalake 
 
### Justification
1. **Why this pipeline type?**
   - Logs are continuous and in high-volume
 
2. **Why this architecture (ETL/ELT)?**
   - Store raw logs first and then transform
 
3. **Data flow description:**
   - Source -> Extraction -> Load -> Transform
 
### Failure Strategy
- **What fails?** Ingestion spike
- **Recovery plan:** 
- **Idempotency:** Yes, logs should be idempotent
 
### Trade-offs
- **Chose streamming over batch because:** because incident response requires real-time alerting
- **Risk of this approach:** High ingestion load
- **Mitigation:**



## Data Flow 5: Partner Data Ingestion
 
### Pipeline Decision
- **Pipeline Type:** Batch
- **Architecture:** ETL
- **Latency Target:** Weekly
- **Target System:** Data Warehouse 
 
### Justification
1. **Why this pipeline type?**
   - Batch processing is the simplest and most reliable approach
 
2. **Why this architecture (ETL/ELT)?**
   - Ensures data validation
 
3. **Data flow description:**
   - Source -> Extraction -> Transform -> Load (Data warehouse)
 
### Failure Strategy
- **What fails?** File not delivered on time, inconsistent CSV structure
- **Recovery plan:** Implement alerts, sschema validation
- **Idempotency:** Yes
 
### Trade-offs
- **Chose batch over streamming because:** data arrives infrequently
- **Risk of this approach:** partner delay
- **Mitigation:** strong monitoring

| Data Flow | Pipeline Type | Architecture | Latency | Target | Key Risk |
|----------|--------------|--------------|----------|---------|----------|
| 1. Fraud Detection | Streaming | ETL | Sub-second | Data lake | Latency |
| 2. Financial Reporting | Batch | ETL | Daily | Data warehouse | Incorrect calculations |
| 3. Customer 360 | Hybrid | Hybrid | Subseconds for events and daily for analytics | Data lake + warehouse | Event duplication |
| 4. Application Logs | Streaming | ELT | Seconds| Datalake | Log ingestion overload |
| 5. Partner Data | Batch | ETL | Weekly | Data warehouse | Late partner data delivery |



## Reflection Questions

### 1. Which data flow was hardest to decide? Why?
The Customer 360 flow  was the hardest because it requires real-time and batch-based consistency. 

### 2. Where did compliance most influence your architecture?
Compliance most strongly influenced the Fraud Detection

### 3. If you could use only ONE pipeline type (batch or streaming), which would you choose and why?
I would choose streaming because it can be adapted to support both real-time and batch-like workloads
