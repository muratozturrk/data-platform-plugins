"""
sparkopt.codegen.context
=========================
Case'lerin yazdığı ara temsil (CodegenContext) ve
buradan Python kodu üreten CodeRenderer.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sparkopt.core.analyzer import DataProfile


@dataclass
class CodegenContext:
    profile: "DataProfile"
    join_cols: list[str] = field(default_factory=list)
    agg_cols: list[str] = field(default_factory=list)
    df_name: str = "df"
    spark_parallelism: int = 200

    _imports: list[str] = field(default_factory=list, init=False, repr=False)
    _configs: dict[str, str] = field(default_factory=dict, init=False, repr=False)
    _transformations: dict[str, str] = field(default_factory=dict, init=False, repr=False)
    _hints: dict[str, str] = field(default_factory=dict, init=False, repr=False)
    _notes: dict[str, str] = field(default_factory=dict, init=False, repr=False)
    _warnings: list[str] = field(default_factory=list, init=False, repr=False)
    _applied_cases: list[str] = field(default_factory=list, init=False, repr=False)

    def add_import(self, stmt: str) -> None:
        if stmt not in self._imports:
            self._imports.append(stmt)

    def add_config(self, key: str, value: str) -> None:
        self._configs[key] = value

    def add_transformation(self, key: str, code: str) -> None:
        self._transformations[key] = code

    def add_hint(self, key: str, code: str) -> None:
        self._hints[key] = code

    def add_note(self, key: str, text: str) -> None:
        self._notes[key] = text

    def add_warning(self, msg: str) -> None:
        self._warnings.append(msg)

    def mark_applied(self, case_name: str) -> None:
        self._applied_cases.append(case_name)

    @property
    def imports(self): return list(self._imports)
    @property
    def configs(self): return dict(self._configs)
    @property
    def transformations(self): return dict(self._transformations)
    @property
    def hints(self): return dict(self._hints)
    @property
    def notes(self): return dict(self._notes)
    @property
    def warnings(self): return list(self._warnings)
    @property
    def applied_cases(self): return list(self._applied_cases)


class CodeRenderer:
    SECTION_SEP = "# " + "─" * 58

    def __init__(self, ctx: CodegenContext):
        self._ctx = ctx

    def render(self, include_header: bool = True) -> str:
        parts: list[str] = []
        if include_header:
            parts.append(self._render_header())
        if self._ctx.imports:
            parts.append(self._render_imports())
        if self._ctx.configs:
            parts.append(self._render_configs())
        if self._ctx.transformations:
            parts.append(self._render_transformations())
        if self._ctx.hints:
            parts.append(self._render_hints())
        if self._ctx.notes:
            parts.append(self._render_notes())
        if self._ctx.warnings:
            parts.append(self._render_warnings())
        return "\n\n".join(parts)

    def _render_header(self) -> str:
        p = self._ctx.profile
        lin = p.lineage
        mem = p.memory
        cases = ", ".join(self._ctx.applied_cases) or "—"
        return (
            f"# {'='*58}\n"
            f"# sparkopt — Otomatik Optimizasyon Kodu\n"
            f"# {'='*58}\n"
            f"# Tablo profili:\n"
            f"#   Satır sayısı   : {p.row_count:,}\n"
            f"#   Tahmini boyut  : {p.estimated_mb:.1f} MB\n"
            f"#   Partition      : {p.partition.count} adet "
            f"(skew={p.partition.skew_ratio:.1f}x)\n"
            f"# Lineage:\n"
            f"#   DAG derinliği  : ~{lin.depth} adım\n"
            f"#   Reuse sayısı   : {lin.estimated_reuse_count}x\n"
            f"#   İteratif döngü : {'Evet' if lin.has_iterative_loop else 'Hayır'}\n"
            f"#   Streaming      : {'Evet' if lin.has_streaming_source else 'Hayır'}\n"
            f"# Bellek:\n"
            f"#   Executor RAM   : {mem.executor_memory_mb} MB × {mem.executor_count}\n"
            f"#   Kullanılabilir : {mem.usable_memory_mb:.0f} MB\n"
            f"#   Önerilen level : {mem.recommended_storage_level}\n"
            f"# Uygulanan case'ler: {cases}\n"
            f"# {'='*58}"
        )

    def _render_imports(self) -> str:
        lines = [self.SECTION_SEP, "# Import'lar", self.SECTION_SEP]
        lines.extend(self._ctx.imports)
        return "\n".join(lines)

    # Uygulama başlarken sabitlenen ayarlar: spark.conf.set ile değiştirilemez
    # (Spark 3.x "Cannot modify the value of a Spark config" hatası verir).
    _LAUNCH_TIME_PREFIXES = (
        "spark.memory.", "spark.serializer", "spark.kryo", "spark.executor.",
        "spark.driver.", "spark.dynamicAllocation.", "spark.shuffle.service.",
    )

    def _render_configs(self) -> str:
        lines = [self.SECTION_SEP, "# Spark konfigürasyonu", self.SECTION_SEP]
        launch = {k: v for k, v in self._ctx.configs.items()
                  if k.startswith(self._LAUNCH_TIME_PREFIXES)}
        for k, v in self._ctx.configs.items():
            if k not in launch:
                lines.append(f'spark.conf.set("{k}", "{v}")')
        if launch:
            lines.append("# Launch-time ayarlar (spark.conf.set ile DEĞİŞTİRİLEMEZ) —")
            lines.append("# spark-submit veya SparkSession.builder ile verin:")
            for k, v in launch.items():
                lines.append(f"#   --conf {k}={v}")
        return "\n".join(lines)

    def _render_transformations(self) -> str:
        lines = [self.SECTION_SEP, "# DataFrame dönüşümleri", self.SECTION_SEP]
        for name, code in self._ctx.transformations.items():
            lines.append(f"# [{name}]")
            lines.append(code)
            lines.append("")
        return "\n".join(lines)

    def _render_hints(self) -> str:
        lines = [self.SECTION_SEP, "# Optimizasyon önerileri (hazır kod)", self.SECTION_SEP]
        for name, code in self._ctx.hints.items():
            lines.append(f"\n# ── {name} ──")
            lines.append(code)
        return "\n".join(lines)

    def _render_notes(self) -> str:
        lines = [self.SECTION_SEP, "# Ek notlar", self.SECTION_SEP]
        for name, text in self._ctx.notes.items():
            lines.append(f"\n# ── {name} ──")
            lines.append(text)
        return "\n".join(lines)

    def _render_warnings(self) -> str:
        lines = [self.SECTION_SEP, "# ⚠️  UYARILAR", self.SECTION_SEP]
        for w in self._ctx.warnings:
            lines.append(f"# ⚠️  {w}")
        return "\n".join(lines)
