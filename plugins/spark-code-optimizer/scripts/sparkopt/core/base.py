"""
sparkopt.core.base
==================
Tüm OptimizationCase'lerin türediği soyut temel sınıf.

Her case şunları tanımlar:
  - matches(profile)  → Bu case geçerli mi?
  - priority          → Çakışma durumunda hangisi kazanır?
  - apply(ctx)        → CodegenContext'e dönüşüm ekler
  - explain()         → İnsan okunabilir açıklama

Case'ler compose edilebilir:
  case_a & case_b   → her ikisi de eşleşmeli (AND)
  case_a | case_b   → biri eşleşmeli (OR)
  ~case_a           → eşleşmemeli (NOT)
"""

from __future__ import annotations

import abc
from dataclasses import dataclass, field
from enum import IntEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sparkopt.core.analyzer import DataProfile
    from sparkopt.codegen.context import CodegenContext


# ──────────────────────────────────────────────
# Öncelik sırası
# ──────────────────────────────────────────────

class Priority(IntEnum):
    LOW    = 10
    MEDIUM = 50
    HIGH   = 90
    CRITICAL = 100   # güvenlik kritik (örn: OOM riski)


# ──────────────────────────────────────────────
# Temel Case Sınıfı
# ──────────────────────────────────────────────

class OptimizationCase(abc.ABC):
    """
    Tek bir optimizasyon kuralını temsil eder.

    Alt sınıflar şunları override etmeli:
      - name      : str  — benzersiz tanımlayıcı
      - priority  : Priority
      - matches() : DataProfile → bool
      - apply()   : CodegenContext → None
      - explain() : str
    """

    name: str = "base_case"
    priority: Priority = Priority.MEDIUM

    # ── Soyut arayüz ────────────────────────────

    @abc.abstractmethod
    def matches(self, profile: "DataProfile") -> bool:
        """Bu case'in uygulanabileceği koşul."""
        ...

    @abc.abstractmethod
    def apply(self, ctx: "CodegenContext") -> None:
        """CodegenContext'e dönüşüm adımını ekler."""
        ...

    @abc.abstractmethod
    def explain(self) -> str:
        """Neden bu optimizasyonu önerdik?"""
        ...

    # ── Kompozisyon operatörleri ─────────────────

    def __and__(self, other: "OptimizationCase") -> "AndCase":
        return AndCase(self, other)

    def __or__(self, other: "OptimizationCase") -> "OrCase":
        return OrCase(self, other)

    def __invert__(self) -> "NotCase":
        return NotCase(self)

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} priority={self.priority}>"


# ──────────────────────────────────────────────
# Kompozit Case'ler
# ──────────────────────────────────────────────

class AndCase(OptimizationCase):
    name = "and_case"

    def __init__(self, left: OptimizationCase, right: OptimizationCase):
        self.left = left
        self.right = right
        self.priority = max(left.priority, right.priority)

    def matches(self, profile: "DataProfile") -> bool:
        return self.left.matches(profile) and self.right.matches(profile)

    def apply(self, ctx: "CodegenContext") -> None:
        self.left.apply(ctx)
        self.right.apply(ctx)

    def explain(self) -> str:
        return f"[{self.left.explain()}] VE [{self.right.explain()}]"


class OrCase(OptimizationCase):
    name = "or_case"

    def __init__(self, left: OptimizationCase, right: OptimizationCase):
        self.left = left
        self.right = right
        self.priority = max(left.priority, right.priority)

    def matches(self, profile: "DataProfile") -> bool:
        return self.left.matches(profile) or self.right.matches(profile)

    def apply(self, ctx: "CodegenContext") -> None:
        # Eşleşen tarafı uygula, ikisi de eşleşiyorsa her ikisini
        if self.left.matches(ctx.profile):
            self.left.apply(ctx)
        if self.right.matches(ctx.profile):
            self.right.apply(ctx)

    def explain(self) -> str:
        return f"[{self.left.explain()}] VEYA [{self.right.explain()}]"


class NotCase(OptimizationCase):
    name = "not_case"

    def __init__(self, inner: OptimizationCase):
        self.inner = inner
        self.priority = inner.priority

    def matches(self, profile: "DataProfile") -> bool:
        return not self.inner.matches(profile)

    def apply(self, ctx: "CodegenContext") -> None:
        pass  # NOT case doğrudan kod üretmez, filtre olarak kullanılır

    def explain(self) -> str:
        return f"DEĞİL [{self.inner.explain()}]"


# ──────────────────────────────────────────────
# Case Registry — tüm tanımlı case'leri tutar
# ──────────────────────────────────────────────

class CaseRegistry:
    """
    Singleton registry. Tüm OptimizationCase'ler buraya register edilir.
    DSL otomatik olarak profile'a uyan case'leri bulur.

    Kullanım:
        registry = CaseRegistry.instance()
        matched = registry.find_matches(profile)
    """

    _instance: "CaseRegistry | None" = None
    _cases: list[OptimizationCase]

    def __init__(self):
        self._cases = []

    @classmethod
    def instance(cls) -> "CaseRegistry":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def register(self, case: OptimizationCase) -> None:
        self._cases.append(case)

    def register_all(self, cases: list[OptimizationCase]) -> None:
        self._cases.extend(cases)

    def find_matches(self, profile: "DataProfile") -> list[OptimizationCase]:
        """Profile'a uyan case'leri öncelik sırasına göre döndürür."""
        matched = [c for c in self._cases if c.matches(profile)]
        return sorted(matched, key=lambda c: c.priority, reverse=True)

    def all_cases(self) -> list[OptimizationCase]:
        return list(self._cases)

    def clear(self) -> None:
        self._cases = []


def register_case(case: OptimizationCase) -> OptimizationCase:
    """Dekoratör olarak veya doğrudan kullanılabilir."""
    CaseRegistry.instance().register(case)
    return case
