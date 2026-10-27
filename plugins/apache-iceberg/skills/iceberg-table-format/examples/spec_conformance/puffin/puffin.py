"""Minimal Puffin reader (https://iceberg.apache.org/puffin-spec/) plus the two blob types Iceberg writes.

File layout:  Magic | Blob_1 ... Blob_n | Footer
Footer:       Magic | FooterPayload (UTF-8 JSON) | FooterPayloadSize (int32 LE) | Flags (4 bytes) | Magic
Magic = 'PFA1'. Flag bit 0 of byte 0 = footer payload LZ4-compressed.
"""
import io
import json
import zlib
from dataclasses import dataclass, field

import zstandard
from pyroaring import BitMap

MAGIC = b"PFA1"
FOOTER_STRUCT_SIZE = 12                     # payload size (4) + flags (4) + magic (4)
DV_MAGIC = bytes.fromhex("D1D33964")
THETA_MAX = (1 << 63) - 1                   # theta is stored as a fraction of Long.MAX_VALUE


class PuffinError(ValueError):
    pass


@dataclass
class BlobMetadata:
    type: str
    fields: list[int]
    snapshot_id: int
    sequence_number: int
    offset: int
    length: int
    compression_codec: str | None = None
    properties: dict = field(default_factory=dict)


@dataclass
class PuffinFile:
    size: int
    footer_size: int
    blobs: list[BlobMetadata]
    properties: dict


def read_footer(data: bytes) -> PuffinFile:
    """Parse the footer from the end of the file, the way a reader seeks to it on object storage."""
    if data[:4] != MAGIC or data[-4:] != MAGIC:
        raise PuffinError("missing PFA1 magic at file start or end")
    payload_size = int.from_bytes(data[-12:-8], "little", signed=True)
    flags = data[-8:-4]
    start = len(data) - FOOTER_STRUCT_SIZE - payload_size - len(MAGIC)
    if start < 4 or data[start:start + 4] != MAGIC:
        raise PuffinError("missing PFA1 magic at footer start")
    payload = data[start + 4:start + 4 + payload_size]
    if flags[0] & 1:
        raise PuffinError("LZ4-compressed footer payloads are not supported by this reader")
    footer = json.loads(payload)
    blobs = [BlobMetadata(b["type"], b["fields"], b["snapshot-id"], b["sequence-number"], b["offset"],
                          b["length"], b.get("compression-codec"), b.get("properties", {}))
             for b in footer["blobs"]]
    for b in blobs:
        if b.offset < 4 or b.offset + b.length > start:
            raise PuffinError(f"blob {b.type} at {b.offset}+{b.length} overlaps the footer")
    return PuffinFile(len(data), len(data) - start, blobs, footer.get("properties", {}))


def read_blob(data: bytes, blob: BlobMetadata) -> bytes:
    raw = data[blob.offset:blob.offset + blob.length]
    if blob.compression_codec is None:
        return raw
    if blob.compression_codec == "zstd":
        return zstandard.ZstdDecompressor().stream_reader(io.BytesIO(raw)).read()
    raise PuffinError(f"codec {blob.compression_codec} not supported")


def decode_deletion_vector(blob: bytes) -> list[int]:
    """deletion-vector-v1: length (int32 BE) | magic D1D33964 | 64-bit roaring bitmap | CRC-32 (BE).

    The bitmap uses the portable 64-bit layout: bucket count (int64 LE), then per bucket the high 32
    bits (int32 LE) followed by a standard 32-bit portable roaring bitmap of the low 32 bits.
    """
    length = int.from_bytes(blob[:4], "big")
    if length + 8 != len(blob):
        raise PuffinError(f"length field {length} does not match blob size {len(blob)}")
    body = blob[4:4 + length]
    if body[:4] != DV_MAGIC:
        raise PuffinError("bad deletion vector magic")
    if zlib.crc32(body) != int.from_bytes(blob[-4:], "big"):
        raise PuffinError("deletion vector CRC-32 mismatch")
    bitmap, pos = body[4:], 8
    positions = []
    for _ in range(int.from_bytes(bitmap[:8], "little")):
        key = int.from_bytes(bitmap[pos:pos + 4], "little")
        bm = BitMap.deserialize(bitmap[pos + 4:])
        pos += 4 + len(bm.serialize())
        positions += [(key << 32) | low for low in bm]
    if pos != len(bitmap):
        raise PuffinError("trailing bytes after the roaring bitmap")
    return positions


def theta_estimate(sketch: bytes) -> float:
    """Distinct-count estimate of an apache-datasketches-theta-v1 blob (serialized CompactSketch)."""
    pre_longs, ser_ver, family, flags = sketch[0] & 0x3F, sketch[1], sketch[2], sketch[5]
    if family != 3:                                    # 3 = COMPACT
        raise PuffinError(f"not a compact theta sketch (family {family}, serVer {ser_ver})")
    if flags & 0x04:                                   # EMPTY
        return 0.0
    if pre_longs == 1:                                 # single-item sketch
        return 1.0
    retained = int.from_bytes(sketch[8:12], "little")
    theta = int.from_bytes(sketch[16:24], "little") if pre_longs >= 3 else THETA_MAX
    return retained * THETA_MAX / theta
