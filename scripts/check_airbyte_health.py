import os
import sys
import json
import asyncio
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

# Ensure src/ is on path
ROOT = Path(__file__).resolve().parents[1]
src_path = ROOT / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from elt_mcp_server.clients.airbyte_client import AirbyteClient


def main() -> int:
    # Load .env if available
    if load_dotenv:
        load_dotenv()

    base_url = os.getenv("AIRBYTE_BASE_URL")
    workspace_id = os.getenv("AIRBYTE_WORKSPACE_ID")
    access_token = os.getenv("AIRBYTE_ACCESS_TOKEN", "")
    username = os.getenv("AIRBYTE_USERNAME", "airbyte")
    password = os.getenv("AIRBYTE_PASSWORD", "password")
    timeout = int(os.getenv("AIRBYTE_TIMEOUT", "30"))

    if not base_url:
        print("AIRBYTE_BASE_URL is not set. Please set it in environment or .env.")
        return 1

    async def run():
        client = AirbyteClient(
            base_url=base_url,
            workspace_id=workspace_id,
            access_token=access_token,
            username=username,
            password=password,
            timeout=timeout,
        )
        health = await client.get_health()
        print(json.dumps(health, indent=2))
        if client.client:
            await client.client.aclose()

    asyncio.run(run())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
