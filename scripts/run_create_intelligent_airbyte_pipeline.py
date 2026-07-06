import os
from dotenv import load_dotenv
import json
import asyncio
from typing import List, Dict, Any

from elt_mcp_server.config import load_settings
from elt_mcp_server.orchestrator import PipelineOrchestrator
from elt_mcp_server.tools.data_movement import register_data_movement_tools


async def main() -> None:
    # Load .env into process environment for os.getenv lookups
    load_dotenv()
    # Ensure required env vars exist to satisfy Settings validation
    os.environ.setdefault("TERADATA_HOST", os.environ.get("TERADATA_HOST", "localhost"))
    os.environ.setdefault("TERADATA_USERNAME", os.environ.get("TERADATA_USERNAME", "demo"))
    os.environ.setdefault("TERADATA_PASSWORD", os.environ.get("TERADATA_PASSWORD", "demo"))
    os.environ.setdefault("TERADATA_DATABASE", os.environ.get("TERADATA_DATABASE", "demo"))
    os.environ.setdefault("AIRFLOW_BASE_URL", os.environ.get("AIRFLOW_BASE_URL", "http://localhost:8080"))
    os.environ.setdefault("AIRFLOW_USERNAME", os.environ.get("AIRFLOW_USERNAME", "airflow"))
    os.environ.setdefault("AIRFLOW_PASSWORD", os.environ.get("AIRFLOW_PASSWORD", "airflow"))
    os.environ.setdefault("AIRBYTE_ENABLED", os.environ.get("AIRBYTE_ENABLED", "true"))
    os.environ.setdefault("AIRBYTE_BASE_URL", os.environ.get("AIRBYTE_BASE_URL", "http://localhost:8000"))
    os.environ.setdefault("AIRBYTE_USERNAME", os.environ.get("AIRBYTE_USERNAME", "airbyte"))
    os.environ.setdefault("AIRBYTE_PASSWORD", os.environ.get("AIRBYTE_PASSWORD", "password"))

    # Load settings from .env
    settings = load_settings()
    orchestrator = PipelineOrchestrator(settings)
    tools = register_data_movement_tools(orchestrator)

    # Read source/destination config from environment
    src_name = os.getenv("AIRBYTE_SOURCE_NAME")
    src_type = os.getenv("AIRBYTE_SOURCE_TYPE")
    src_cfg_str = os.getenv("AIRBYTE_SOURCE_CONFIG_JSON")

    dst_name = os.getenv("AIRBYTE_DESTINATION_NAME")
    dst_type = os.getenv("AIRBYTE_DESTINATION_TYPE")
    dst_cfg_str = os.getenv("AIRBYTE_DESTINATION_CONFIG_JSON")

    if not all([src_name, src_type, src_cfg_str, dst_name, dst_type, dst_cfg_str]):
        print({
            "success": False,
            "error": "Missing AIRBYTE_* env vars for source/destination configuration",
            "required": [
                "AIRBYTE_SOURCE_NAME","AIRBYTE_SOURCE_TYPE","AIRBYTE_SOURCE_CONFIG_JSON",
                "AIRBYTE_DESTINATION_NAME","AIRBYTE_DESTINATION_TYPE","AIRBYTE_DESTINATION_CONFIG_JSON",
            ],
        })
        return

    try:
        source_config: Dict[str, Any] = json.loads(src_cfg_str)
        destination_config: Dict[str, Any] = json.loads(dst_cfg_str)
    except Exception as e:
        print({"success": False, "error": f"Invalid JSON in env: {e}"})
        return

    # Discover streams after creating/reusing source via definition lookup
    # We need stream names for the connection; try intent-driven if provided
    # Fallback: discover and pick first up to 3 streams
    intent = os.getenv("AIRBYTE_STREAM_INTENT")

    # Resolve definition IDs and create/reuse source/destination via pipeline helper
    # Build preliminary selected streams using discovery
    # We'll do a lightweight discovery using list of existing sources if present
    # Otherwise create_intelligent_airbyte_pipeline will create the source and we can still pass names

    # To get stream names, attempt listing configured sources and match by name
    src_list = await tools["list_airbyte_sources"]()
    stream_selections: List[Dict[str, Any]] = []
    source_id = None

    if src_list.get("success"):
        # Try to find source by name
        for s in src_list.get("sources", []):
            if str(s.get("name") or "").strip().lower() == src_name.strip().lower():
                source_id = s.get("sourceId")
                break

    if source_id:
        disc = await tools["list_streams"](source_id)
        if disc.get("success") and disc.get("streams"):
            streams = disc.get("streams")
            # If intent provided, try to select using intent tool
            if intent:
                intent_res = await tools["select_streams_from_intent"](
                    source_id=source_id,
                    prompt=intent,
                    schemas=None,
                    policy=None,
                    limit=None,
                )
                if intent_res.get("success"):
                    stream_selections = intent_res.get("selected_streams") or []
            # Fallback: pick first up to 3
            if not stream_selections:
                for s in streams[:3]:
                    modes = [str(m).lower() for m in (s.get("supported_sync_modes") or [])]
                    sync_mode = "incremental" if "incremental" in modes else "full_refresh"
                    stream_selections.append({
                        "name": s.get("name"),
                        "syncMode": sync_mode,
                        "destinationSyncMode": "append",
                        "selected": True,
                    })

    # Final connection name and schedule
    connection_name = os.getenv("AIRBYTE_TEST_CONNECTION_NAME", f"{src_name} -> {dst_name}")
    schedule_type = os.getenv("AIRBYTE_SCHEDULE_TYPE", "manual")

    print({
        "info": "Invoking create_intelligent_airbyte_pipeline",
        "connection_name": connection_name,
        "schedule_type": schedule_type,
        "source": {"name": src_name, "type": src_type},
        "destination": {"name": dst_name, "type": dst_type},
        "selected_stream_count": len(stream_selections),
    })

    result = await tools["create_intelligent_airbyte_pipeline"](
        source_name=src_name,
        source_type=src_type,
        source_connection_configuration=source_config,
        destination_name=dst_name,
        destination_type=dst_type,
        destination_connection_configuration=destination_config,
        streams=stream_selections,
        connection_name=connection_name,
        schedule_type=schedule_type,
    )

    print(result)


if __name__ == "__main__":
    asyncio.run(main())
