"""Tenant-aware storage resolution: silo / pool / bridge decided by tenant configuration, not by code."""
from __future__ import annotations
from dataclasses import dataclass

@dataclass(frozen=True)
class StorageLayout:
    mode: str            # silo | pool | bridge
    root: str            # from environment layer, e.g. s3://platform-lake-eu
    silo_root: str | None = None   # tenant-specific root when mode == silo

def resolve(ctx, logical_path: str, layout: StorageLayout) -> str:
    """Never called by core with a physical path; core passes logical names only."""
    if layout.mode == "silo":
        if not layout.silo_root:
            raise ValueError(f"tenant {ctx.tenant_id}: silo mode requires silo_root")
        return f"{layout.silo_root}/{ctx.environment}/{logical_path}"
    if layout.mode == "pool":
        return f"{layout.root}/{ctx.environment}/tenants/{ctx.tenant_id}/{logical_path}"
    if layout.mode == "bridge":
        # shared root, dedicated prefix + dedicated credentials (resolved elsewhere)
        return f"{layout.root}/{ctx.environment}/{ctx.tenant_id}/{logical_path}"
    raise ValueError(f"unknown isolation mode {layout.mode}")
