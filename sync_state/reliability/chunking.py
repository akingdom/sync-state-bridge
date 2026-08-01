"""
sync_state/chunking.py

Snapshot chunking & reassembly (64 KB chunks).
"""

import base64
import hashlib
import json
import time
from typing import List, Dict, Any, Optional

CHUNK_SIZE = 64 * 1024  # 64 KiB

def chunk_snapshot(snapshot_payload: Dict[str, Any],
                   snapshot_id: str) -> List[Dict[str, Any]]:
    """
    Split a snapshot dict into base64‑encoded chunks.

    Returns a list of chunk frames, each containing:
        type, snapshot_id, chunk_idx, total_chunks, chunk_hash, data
    """
    raw_bytes = json.dumps(snapshot_payload).encode('utf-8')
    total_len = len(raw_bytes)
    total_chunks = (total_len + CHUNK_SIZE - 1) // CHUNK_SIZE
    chunks = []

    for idx in range(total_chunks):
        start = idx * CHUNK_SIZE
        end = min(start + CHUNK_SIZE, total_len)
        segment = raw_bytes[start:end]
        chunk_hash = hashlib.md5(segment).hexdigest()[:8]
        encoded_data = base64.b64encode(segment).decode('ascii')

        chunks.append({
            "type": "SNAPSHOT_CHUNK",
            "snapshot_id": snapshot_id,
            "chunk_idx": idx,
            "total_chunks": total_chunks,
            "chunk_hash": chunk_hash,
            "data": encoded_data,
        })

    return chunks


class SnapshotReassembler:
    """
    Reassemble snapshot chunks in memory.
    Timeout: evicts incomplete snapshots after 5 seconds.
    """

    def __init__(self, timeout_seconds: float = 5.0):
        self.timeout = timeout_seconds
        self._buffers: Dict[str, Dict[int, bytes]] = {}
        self._meta: Dict[str, Dict[str, Any]] = {}

    def ingest_chunk(self, chunk_frame: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Ingest a single chunk.

        Returns:
            Fully reassembled snapshot dict if complete, else None.
        """
        snap_id = chunk_frame["snapshot_id"]
        idx = chunk_frame["chunk_idx"]
        total = chunk_frame["total_chunks"]
        raw_segment = base64.b64decode(chunk_frame["data"])

        # Verify hash
        if hashlib.md5(raw_segment).hexdigest()[:8] != chunk_frame["chunk_hash"]:
            self._buffers.pop(snap_id, None)
            self._meta.pop(snap_id, None)
            raise ValueError(f"Corrupted chunk {idx} for snapshot {snap_id}")

        now = time.time()
        if snap_id not in self._buffers:
            self._buffers[snap_id] = {}
            self._meta[snap_id] = {"total": total, "created": now}

        # Evict if expired
        if now - self._meta[snap_id]["created"] > self.timeout:
            self._buffers.pop(snap_id, None)
            self._meta.pop(snap_id, None)
            return None

        self._buffers[snap_id][idx] = raw_segment

        # Check completeness
        if len(self._buffers[snap_id]) == total:
            full_bytes = b"".join(self._buffers[snap_id][i]
                                  for i in range(total))
            self._buffers.pop(snap_id)
            self._meta.pop(snap_id)
            return json.loads(full_bytes.decode('utf-8'))

        return None

    def evict_expired(self) -> None:
        """Remove snapshots that have timed out."""
        now = time.time()
        expired = [sid for sid, meta in self._meta.items()
                   if now - meta["created"] > self.timeout]
        for sid in expired:
            self._buffers.pop(sid, None)
            self._meta.pop(sid, None)