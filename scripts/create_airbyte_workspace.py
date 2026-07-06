import asyncio
import os
from dotenv import load_dotenv

from elt_mcp_server.config import load_settings
from elt_mcp_server.orchestrator import PipelineOrchestrator


async def amain():
    load_dotenv()
    # Enforce Basic auth if a stale token exists in .env
    # Many self-hosted Airbyte instances default to Basic auth.
    os.environ["AIRBYTE_ACCESS_TOKEN"] = ""
    settings = load_settings(force_reload=True)
    orch = PipelineOrchestrator(settings)

    # Name from env or default argument
    name = os.getenv("AIRBYTE_NEW_WORKSPACE_NAME", "fastmcptries")

    # Create workspace
    ws = await orch.airbyte_client.create_workspace(name=name)
    print({
        "success": True,
        "workspaceId": ws.get("workspaceId"),
        "name": ws.get("name"),
        "slug": ws.get("slug"),
    })


def main():
    asyncio.run(amain())


if __name__ == "__main__":
    main()
