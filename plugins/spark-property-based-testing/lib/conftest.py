# Loads the library's pytest plugin when running from source (an installed
# package registers it through the pytest11 entry point instead).
import importlib.metadata as _md

if not any(ep.name == "spark_prop_testing" for ep in _md.entry_points(group="pytest11")):
    pytest_plugins = ["spark_prop_testing.plugin"]
