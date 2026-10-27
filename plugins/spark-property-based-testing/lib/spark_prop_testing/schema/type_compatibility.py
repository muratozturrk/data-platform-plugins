# spark_prop_testing/schema/type_compatibility.py

"""
Defines compatibility rules between Spark DataTypes.

Four levels of compatibility:
  EXACT       → types are identical (including nested element/field types)
  COMPATIBLE  → safe implicit conversion exists, no data loss (e.g. INT → LONG)
  COERCIBLE   → possible but lossy (e.g. DOUBLE → INT, LONG → DOUBLE above 2^53)
  INCOMPATIBLE→ no safe path exists (e.g. STRING → TIMESTAMP)
"""

from enum import Enum, auto
from typing import Tuple
from pyspark.sql.types import (
    DataType, ByteType, ShortType, IntegerType, LongType,
    FloatType, DoubleType, StringType,
    DateType, TimestampType, DecimalType,
    ArrayType, MapType, StructType,
)


class Compatibility(Enum):
    EXACT        = auto()
    COMPATIBLE   = auto()   # safe, no data loss
    COERCIBLE    = auto()   # possible, but lossy
    INCOMPATIBLE = auto()   # no valid path


def _worst(*levels: Compatibility) -> Compatibility:
    return max(levels, key=lambda c: c.value)


# Integral types and the decimal digits they need
_INTEGRAL_DIGITS = {ByteType: 3, ShortType: 5, IntegerType: 10, LongType: 19}
# Integer → float types are exact only up to the mantissa width
_FLOAT_EXACT_DIGITS = {FloatType: 7, DoubleType: 15}


def _integral_digits(t: DataType):
    return next((d for cls, d in _INTEGRAL_DIGITS.items() if isinstance(t, cls)), None)


def _float_digits(t: DataType):
    return next((d for cls, d in _FLOAT_EXACT_DIGITS.items() if isinstance(t, cls)), None)


def check_type_compatibility(
    source: DataType,
    target: DataType,
) -> Tuple[Compatibility, str]:
    """
    Checks whether source type can be read as target type.

    Returns:
        (Compatibility level, human-readable explanation)

    Examples:
        IntegerType → LongType           : COMPATIBLE  (safe widening)
        LongType    → IntegerType        : COERCIBLE   (possible overflow)
        LongType    → DoubleType         : COERCIBLE   (precision loss above 2^53)
        StringType  → IntegerType        : INCOMPATIBLE
        array<int>  → array<string>      : COMPATIBLE  (element widening to string)
        decimal(10,2) → decimal(10,4)    : COERCIBLE   (integer digits shrink)
        IntegerType → IntegerType        : EXACT
    """
    src_name, tgt_name = type(source).__name__, type(target).__name__

    # ── Nested types: recurse (same class is NOT enough) ──────────────────────
    if isinstance(source, ArrayType) and isinstance(target, ArrayType):
        elem_compat, msg = check_type_compatibility(source.elementType, target.elementType)
        return elem_compat, f"Array element: {msg}"

    if isinstance(source, MapType) and isinstance(target, MapType):
        key_c, key_msg = check_type_compatibility(source.keyType, target.keyType)
        val_c, val_msg = check_type_compatibility(source.valueType, target.valueType)
        return _worst(key_c, val_c), f"Map key: {key_msg} | Map value: {val_msg}"

    if isinstance(source, StructType) and isinstance(target, StructType):
        return _struct_compatibility(source, target)

    # ── Decimal ↔ decimal: compare integer digits and scale separately ────────
    if isinstance(source, DecimalType) and isinstance(target, DecimalType):
        if source.precision == target.precision and source.scale == target.scale:
            return Compatibility.EXACT, "Identical decimal types"
        src_int = source.precision - source.scale
        tgt_int = target.precision - target.scale
        if tgt_int >= src_int and target.scale >= source.scale:
            return Compatibility.COMPATIBLE, (
                f"Decimal widening: ({source.precision},{source.scale}) "
                f"→ ({target.precision},{target.scale})"
            )
        return Compatibility.COERCIBLE, (
            f"Decimal narrowing (possible overflow or rounding): "
            f"({source.precision},{source.scale}) → ({target.precision},{target.scale}) "
            f"— integer digits {src_int}→{tgt_int}, scale {source.scale}→{target.scale}"
        )

    # ── Exact match for non-parameterised types ───────────────────────────────
    if type(source) == type(target):
        return Compatibility.EXACT, f"Identical types: {src_name}"

    # ── Numeric conversions ───────────────────────────────────────────────────
    src_int, tgt_int = _integral_digits(source), _integral_digits(target)
    src_flt, tgt_flt = _float_digits(source), _float_digits(target)

    if src_int is not None and tgt_int is not None:
        if src_int <= tgt_int:
            return Compatibility.COMPATIBLE, f"Safe integer widening: {src_name} → {tgt_name}"
        return Compatibility.COERCIBLE, f"Integer narrowing (possible overflow): {src_name} → {tgt_name}"

    if src_int is not None and tgt_flt is not None:
        if src_int <= tgt_flt:
            return Compatibility.COMPATIBLE, f"Integer → float, exact: {src_name} → {tgt_name}"
        return Compatibility.COERCIBLE, (
            f"Integer → float loses precision for large values: {src_name} → {tgt_name}"
        )

    if src_flt is not None and tgt_flt is not None:
        if src_flt <= tgt_flt:
            return Compatibility.COMPATIBLE, f"Safe float widening: {src_name} → {tgt_name}"
        return Compatibility.COERCIBLE, f"Float narrowing (precision loss): {src_name} → {tgt_name}"

    if src_flt is not None and tgt_int is not None:
        return Compatibility.COERCIBLE, f"Float → integer truncates: {src_name} → {tgt_name}"

    if src_int is not None and isinstance(target, DecimalType):
        if target.precision - target.scale >= src_int:
            return Compatibility.COMPATIBLE, f"{src_name} fits {target.simpleString()}"
        return Compatibility.COERCIBLE, f"{src_name} may overflow {target.simpleString()}"

    if isinstance(source, DecimalType) and (tgt_int is not None or tgt_flt is not None):
        return Compatibility.COERCIBLE, f"{source.simpleString()} → {tgt_name} (rounding/overflow possible)"

    # ── Anything → String (always representable, often unintentional) ────────
    if isinstance(target, StringType):
        return Compatibility.COMPATIBLE, (
            f"{src_name} → StringType (always representable, but likely unintentional)"
        )

    # ── Date ↔ Timestamp ──────────────────────────────────────────────────────
    if isinstance(source, DateType) and isinstance(target, TimestampType):
        return Compatibility.COMPATIBLE, "Date → Timestamp (safe widening)"

    if isinstance(source, TimestampType) and isinstance(target, DateType):
        return Compatibility.COERCIBLE, "Timestamp → Date (time component lost)"

    # ── Default: incompatible ─────────────────────────────────────────────────
    return Compatibility.INCOMPATIBLE, f"No safe conversion: {src_name} → {tgt_name}"


def _struct_compatibility(source: StructType, target: StructType) -> Tuple[Compatibility, str]:
    src_fields = {f.name: f for f in source.fields}
    levels, notes = [Compatibility.EXACT], []
    for tf in target.fields:
        sf = src_fields.get(tf.name)
        if sf is None:
            levels.append(Compatibility.INCOMPATIBLE)
            notes.append(f"field '{tf.name}' missing")
            continue
        c, msg = check_type_compatibility(sf.dataType, tf.dataType)
        if sf.nullable and not tf.nullable:
            c = _worst(c, Compatibility.COERCIBLE)
            msg += " (nullable → NOT NULL)"
        if c != Compatibility.EXACT:
            notes.append(f"{tf.name}: {msg}")
        levels.append(c)
    extra = sorted(set(src_fields) - {f.name for f in target.fields})
    if extra:
        levels.append(Compatibility.COMPATIBLE)
        notes.append(f"extra fields {extra}")
    return _worst(*levels), "Struct: " + ("; ".join(notes) if notes else "identical fields")
