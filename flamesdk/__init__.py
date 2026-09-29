"""Python SDK for building federated analyses on the FLAME/PrivateAIM platform.

An analysis container instantiates :class:`FlameCoreSDK`, which bootstraps the
connections to the platform's sidecar services (MessageBroker, PO service,
ResultService/Storage and DataAPI) and exposes the methods used for inter-node
messaging, result exchange and data source access::

    from flamesdk import FlameCoreSDK

    flame = FlameCoreSDK()
"""

from flamesdk.flame_core import FlameCoreSDK

__all__ = ["FlameCoreSDK"]
