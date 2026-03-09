# nexus-rpc-sdk

Python SDK for the Nexus microservice framework.

## Installation

```bash
pip install nexus-rpc-sdk
```

## Quick example

```python
from nexus_sdk import Node, Request, Response


def ping_handler(req: Request) -> Response:
    return Response(payload=b"pong")


node = Node(name="example-service")
node.handle("ping", ping_handler)
node.serve()  # registers with the Nexus registry and starts serving
```

For full framework documentation, see the main Nexus repository:
https://github.com/maxesisnclaw/nexus

## License

MIT
