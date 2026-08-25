"""Everything that talks to S3-compatible object storage.

MinIO under compose, S3 in phase 6 — one implementation for both, because the
difference is an endpoint and a credential rather than a protocol. The Protocol
this implements stays in `core/artifacts.py`, like every other adapter here.
"""
