# **StateSync Protocol Specification (v1.0)**  
*A reactive, versioned, per‑type state synchronization protocol over Server‑Sent Events (SSE).*

---

## **1. Purpose**
StateSync is a **real‑time, incremental synchronization protocol** designed for applications that maintain shared state across multiple clients. It provides:

- Deterministic per‑type versioning  
- Incremental deltas with lifecycle semantics  
- Full snapshot fallback  
- Reactive push‑based updates  
- Schema versioning  
- Transport via SSE  

StateSync guarantees that any compliant client can converge to the server’s authoritative state without requiring polling or bidirectional communication.

---

## **2. Core Concepts**

### **2.1 Types**
State is partitioned into **types** (e.g., `"users"`, `"tasks"`, `"messages"`).  
Each type has:

- A **snapshot provider**  
- A **current snapshot**  
- A **version counter**  
- A **change log**  

### **2.2 Entities**
Each entity:

- Is a dictionary/object  
- Must contain a unique identifier under `id_key` (default `"id"`)  
- May contain arbitrary nested fields  
- May optionally contain `_v` (explicit version)

### **2.3 Versioning**
Each type maintains an independent monotonically increasing integer version:

```
versions[type] ∈ ℕ
```

Version increments only when the snapshot for that type changes.

### **2.4 Change Log**
For each type, the server stores a bounded history of deltas:

```
change_logs[type] = deque(maxlen = max_history)
```

Each entry contains:

```json
{
  "version": <int>,
  "added": [ids],
  "updated": [ids],
  "deleted": [ids]
}
```

---

## **3. Snapshot Providers**

### **3.1 Requirements**
A snapshot provider must return:

```
List[Dict]
```

Each dict must:

- Contain `id_key`
- Have a unique ID (string or integer)
- Be JSON‑serializable after sanitization

### **3.2 Validation**
The server performs strict validation:

- Return type must be list/tuple  
- Each entity must be a dict  
- ID must exist and be str/int  
- No duplicate IDs  
- Entities are deep‑copied to prevent mutation  

Invalid provider output raises `ProviderValidationError`.

---

## **4. Commit Cycle**

### **4.1 Dirty Types**
The server tracks types marked dirty via:

```
mark_dirty(type)
```

### **4.2 Commit Steps**
Commit consists of:

1. Capture dirty types under lock  
2. Execute providers outside lock  
3. Validate and sanitize snapshots  
4. Apply changes under lock  
5. Compute added/updated/deleted  
6. Append delta to change log  
7. Update current snapshot  
8. Increment version  
9. Notify active streams

Commit is atomic per type.

---

## **5. Delta Semantics**

### **5.1 Full Snapshot Conditions**
A full snapshot is returned when:

- No history exists  
- Client version < earliest history version  
- Client version > current version  
- History was truncated  

### **5.2 Incremental Delta Conditions**
Incremental deltas are returned when:

```
history exists AND earliest_history_version ≤ client_version ≤ current_version
```

### **5.3 Lifecycle FSM**
The server computes entity lifecycle transitions using a finite‑state machine:

| Previous State | Event     | New State |
|----------------|-----------|-----------|
| —              | added     | added     |
| —              | updated   | updated   |
| added          | updated   | added     |
| updated        | updated   | updated   |
| deleted        | updated   | deleted   |
| added          | deleted   | removed   |
| updated        | deleted   | deleted   |
| —              | deleted   | deleted   |

This ensures correct behavior for sequences like:

- delete → add → update  
- add → delete  
- update → delete  

### **5.4 Delta Payload**
Incremental delta:

```json
{
  "type": "<type>",
  "full": false,
  "version": <int>,
  "added": [entities],
  "updated": [entities],
  "deleted": [ids]
}
```

Full snapshot:

```json
{
  "type": "<type>",
  "full": true,
  "version": <int>,
  "added": [entities],
  "updated": [],
  "deleted": []
}
```

---

## **6. Transport Layer (SSE)**

### **6.1 Connection Initialization**
Client sends:

```
GET /stream?versions=<json_encoded_local_versions>
```

### **6.2 Initial Manifest**
Server sends:

```json
event: manifest
data: {
  "schema_version": <int>,
  "versions": { type: version },
  "types": [type1, type2, ...]
}
```

### **6.3 Initial Deltas**
Server sends all deltas required to bring client up to date.

### **6.4 Reactive Updates**
Server maintains a list of per‑stream events.  
On commit:

- All stream events are triggered  
- Streams compute new deltas  
- Streams send them immediately

### **6.5 Keepalive**
If no commit occurs within `keepalive_interval`:

```json
event: keepalive
data: {}
```

### **6.6 Disconnect Handling**
Every `yield` is wrapped in a disconnect guard.  
Streams clean themselves up on exit.

---

## **7. Client Behavior**

### **7.1 Version Tracking**
Client maintains:

- `localVersions[type]` — last applied version  
- `manifestVersions[type]` — server-reported version  

### **7.2 Stale Delta Rejection**
Client rejects:

```
delta.version <= localVersions[type]
```

Except for full snapshots.

### **7.3 Full Snapshot Handling**
Full snapshots overwrite local state for that type.

### **7.4 Schema Version Handling**
If schema version changes:

- Client triggers `onSchemaMismatch`  
- Application decides how to recover  

### **7.5 Type Pruning**
If server removes a type:

- Client deletes store[type]  
- Client deletes localVersions[type]  
- Client deletes manifestVersions[type]

### **7.6 Reconnection Logic**
Client reconnects automatically with:

- Guarded reconnection  
- Debounced retry  
- Preservation of localVersions  

---

## **8. Error Handling**

### **8.1 Provider Errors**
Raise `ProviderValidationError`.  
Dirty type is re-marked for retry.

### **8.2 Transport Errors**
Client triggers:

- `onError` callback  
- Reconnection cycle  

### **8.3 Schema Mismatch**
Client triggers:

- `onSchemaMismatch` callback  

---

## **9. Guarantees**

### **9.1 Convergence**
Any compliant client will converge to server state.

### **9.2 Deterministic Deltas**
Lifecycle FSM ensures deterministic incremental updates.

### **9.3 No Race Conditions**
Locking model ensures atomicity.

### **9.4 No Ghost Entities**
Type pruning prevents stale state.

### **9.5 No Duplicate Streams**
Client reconnection guards prevent multiple EventSources.

---

## **10. Non‑Goals**

StateSync does **not** provide:

- Bidirectional communication  
- Conflict resolution  
- CRDT semantics  
- Partial entity updates  
- Compression or chunking  
- Authentication or authorization  

These must be implemented externally.

---

## **11. Versioning of the Protocol**

This specification describes:

```
StateSync Protocol v1.0
```

Schema versioning is independent and application‑defined.

