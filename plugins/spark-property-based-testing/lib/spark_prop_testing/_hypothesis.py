# spark_prop_testing/_hypothesis.py

"""
Shared Hypothesis settings for Spark property tests.

Spark actions take hundreds of milliseconds and vary with JVM warm-up, so the
Hypothesis defaults (200 ms deadline, too_slow health check) produce
DeadlineExceeded / Flaky errors instead of real findings.

- ``register_spark_profiles()`` registers "spark" (local runs) and "spark-ci"
  (more examples, derandomized) profiles. The pytest plugin loads "spark"
  unless ``--hypothesis-profile`` is given. Explicit ``@settings(...)`` in a
  test only override the keys they name, so they inherit ``deadline=None``.
- ``spark_settings(max_examples)`` is used by the library's own runners.
- ``SPARK_PROP_MAX_EXAMPLES`` (set by ``--spark-prop-max-examples`` or the
  CLI's ``-n``) overrides max_examples in every library runner.
"""

import os
from typing import Optional

from hypothesis import HealthCheck, settings

# large_base_example: DataFrames with min_rows in the hundreds are legitimate
SPARK_HEALTH_CHECKS = [HealthCheck.too_slow, HealthCheck.data_too_large, HealthCheck.large_base_example]
MAX_EXAMPLES_ENV = "SPARK_PROP_MAX_EXAMPLES"


def register_spark_profiles() -> None:
    settings.register_profile(
        "spark",
        deadline=None,
        suppress_health_check=SPARK_HEALTH_CHECKS,
        max_examples=20,
    )
    settings.register_profile(
        "spark-ci",
        parent=settings.get_profile("spark"),
        max_examples=100,
        derandomize=True,
        print_blob=True,
    )


def effective_max_examples(requested: Optional[int]) -> int:
    override = os.environ.get(MAX_EXAMPLES_ENV)
    if override:
        return int(override)
    return requested if requested is not None else 20


def spark_settings(max_examples: Optional[int] = None, phases=None) -> settings:
    kwargs = {}
    if phases is not None:
        kwargs["phases"] = phases   # e.g. (Phase.explicit, Phase.generate): no shrinking
    return settings(
        deadline=None,
        suppress_health_check=SPARK_HEALTH_CHECKS,
        max_examples=effective_max_examples(max_examples),
        **kwargs,
    )
