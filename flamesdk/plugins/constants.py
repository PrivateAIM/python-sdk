import os

from flamesdk.plugins.nextflow.nextflow_api import NextFlowAPI


PLUGINS = {
    "nf":
        {
            "service_name": "NextFlow Launcher",    # arbitrary service name
            "env": "NF_PLUGIN",                     # env bool variable name (upper case per convention)
            "api": "_nf_api",                       # FlameCoreSDK attribute (has to start with '_' and end with 'api')
            "client": "nf_client",                  # FlameAPI attribute (has to end with '_client')
            "api_class": NextFlowAPI                # API class (has to be initializable with class(NodeConfig(), FlameLogger()) and has to have an attribute 'client')
        }
}


def any_plugin() -> bool:
    for value in PLUGINS.values():
        if os.getenv(value["env"], False):
            return True
    return False