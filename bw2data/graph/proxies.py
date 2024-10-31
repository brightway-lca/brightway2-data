from bw2data.graph.schema import Node
from bw2data.proxies import ActivityProxyBase, ProxyBase


class GraphNode(ProxyBase):
    def __init__(self, document=None, **kwargs):
        if document is None:
            self._document = Node()
            self._data = kwargs
        else:
            self._document = document
            self._data = self._document.payload
