from typing import Any
from inspect import isclass

from flamesdk.plugins.nextflow.nextflow_api import NextFlowAPI


_PLUGINS = {
    "nf":
        {
            "service_name": "NextFlow Launcher",    # arbitrary service name
            "env": "NF_PLUGIN",                     # env bool variable name (upper case per convention)
            "api": "_nf_api",                       # FlameCoreSDK attribute (has to start with '_' and end with 'api')
            "client": "nf_client",                  # FlameAPI attribute (has to end with '_client')
            "api_class": NextFlowAPI                # API class (has to be initializable with class(NodeConfig(), FlameLogger()) and has to have an attribute 'client')
        }
}


def get_plugins() -> dict[str, dict[str, Any]]:
    return _sanitize_plugin_configs()


def _sanitize_plugin_configs() -> dict[str, dict[str, Any]]:
    plugins = {}
    for plugin, config in _PLUGINS.items():
        sanitized_config = {}
        for key, value in config.items():
            if key == "service_name":
                sanitized_config[key] = value
            elif key == "env":
                sanitized_config[key] = value.upper()
            elif key == "api":
                if value.startswith('_') and value.endswith('api'):
                    sanitized_config[key] = value
                else:
                    break
            elif key == "client":
                if value.endswith('client'):
                    sanitized_config[key] = value
                else:
                    break
            elif key == "api_class":
                if isclass(value):
                    sanitized_config[key] = value
                else:
                    break

        if all(k in sanitized_config for k in ["service_name", "env", "api", "client", "api_class"]):
            plugins[plugin] = sanitized_config

    return plugins
