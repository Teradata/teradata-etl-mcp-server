import os
import json
import asyncio
from dotenv import load_dotenv

from elt_mcp_server.config import load_settings
from elt_mcp_server.orchestrator import PipelineOrchestrator
from elt_mcp_server.tools.data_movement import register_data_movement_tools


async def amain():
    load_dotenv()

    settings = load_settings(force_reload=True)
    orchestrator = PipelineOrchestrator(settings)
    # Ensure connector registry cache is available for definition lookups
    orchestrator.preload_airbyte_registry()
    tools = register_data_movement_tools(orchestrator)

    # Inputs from environment
    source_name = os.getenv("AIRBYTE_SOURCE_NAME")
    source_type = os.getenv("AIRBYTE_SOURCE_TYPE")  # e.g., "Postgres"
    source_config_json = os.getenv("AIRBYTE_SOURCE_CONFIG_JSON", "{}").strip()

    destination_name = os.getenv("AIRBYTE_DESTINATION_NAME")
    destination_type = os.getenv("AIRBYTE_DESTINATION_TYPE")  # e.g., "Teradata" or "BigQuery"
    destination_config_json = os.getenv("AIRBYTE_DESTINATION_CONFIG_JSON", "{}").strip()

    connection_name = os.getenv("AIRBYTE_CONNECTION_NAME", f"{source_name or 'source'}_to_{destination_name or 'destination'}")
    schedule_type = os.getenv("AIRBYTE_SCHEDULE_TYPE", "manual")  # manual | basic | cron
    wait_sync = os.getenv("AIRBYTE_WAIT_FOR_SYNC", "true").lower() in ("1", "true", "yes")

    if not (source_name and source_type and destination_name and destination_type):
        print("Missing required env vars: AIRBYTE_SOURCE_NAME, AIRBYTE_SOURCE_TYPE, AIRBYTE_DESTINATION_NAME, AIRBYTE_DESTINATION_TYPE")
        return

    try:
        source_config = json.loads(source_config_json) if source_config_json else {}
    except Exception as e:
        print(f"Invalid AIRBYTE_SOURCE_CONFIG_JSON: {e}")
        return
    try:
        destination_config = json.loads(destination_config_json) if destination_config_json else {}
    except Exception as e:
        print(f"Invalid AIRBYTE_DESTINATION_CONFIG_JSON: {e}")
        return

    # Best-effort normalization for Teradata destination config
    if (destination_type or "").lower() == "teradata":
        # Build or merge spec-compliant 'logmech' block and remove top-level aliases
        auth = (
            destination_config.pop("authorizationMechanism", None)
            or destination_config.pop("auth_type", None)
        )
        user = (
            destination_config.pop("user", None)
            or destination_config.pop("username", None)
        )
        pwd = (
            destination_config.pop("password", None)
            or destination_config.pop("pwd", None)
        )

        existing_logmech = destination_config.get("logmech") if isinstance(destination_config, dict) else None
        if isinstance(existing_logmech, dict):
            # Merge: only fill missing keys from aliases; don't override explicit values
            if auth is not None and "auth_type" not in existing_logmech:
                existing_logmech["auth_type"] = str(auth).upper() if isinstance(auth, str) else auth
            if user is not None and "username" not in existing_logmech:
                existing_logmech["username"] = user
            if pwd is not None and "password" not in existing_logmech:
                existing_logmech["password"] = pwd
            destination_config["logmech"] = existing_logmech
        else:
            # Synthesize a new logmech only from provided aliases
            logmech: dict = {}
            if auth is not None:
                logmech["auth_type"] = str(auth).upper() if isinstance(auth, str) else auth
            if user is not None:
                logmech["username"] = user
            if pwd is not None:
                logmech["password"] = pwd
            if logmech:
                destination_config["logmech"] = logmech

    # Resolve definitions via registry cache
    source_def_id = await orchestrator.airbyte_client.find_definition_id_by_name("source", source_type)
    destination_def_id = await orchestrator.airbyte_client.find_definition_id_by_name("destination", destination_type)
    if not source_def_id:
        print(f"Could not find source definition for type '{source_type}' in registry")
        return
    if not destination_def_id:
        print(f"Could not find destination definition for type '{destination_type}' in registry")
        return

    # Create or reuse source
    src_res = await tools["create_airbyte_source"](
        name=source_name,
        source_definition_id=source_def_id,
        connection_configuration=source_config,
    )
    if not src_res.get("success"):
        print("Failed to create/reuse source:", src_res)
        return
    source = src_res["source"]
    source_id = source.get("sourceId")
    print("SOURCE:", {"id": source_id, "name": source.get("name"), "reused": src_res.get("reused")})

    # Create or reuse destination
    dst_res = await tools["create_airbyte_destination"](
        name=destination_name,
        destination_definition_id=destination_def_id,
        connection_configuration=destination_config,
    )
    if not dst_res.get("success"):
        print("Failed to create/reuse destination:", dst_res)
        return
    destination = dst_res["destination"]
    destination_id = destination.get("destinationId")
    print("DESTINATION:", {"id": destination_id, "name": destination.get("name"), "reused": dst_res.get("reused")})

    # Discover streams and select all with sensible defaults
    streams_info = await tools["list_streams"](source_id=source_id)
    if not streams_info.get("success"):
        print("Failed to list streams:", streams_info)
        return
    selected_streams = []
    for s in streams_info.get("streams", []):
        name = s.get("name")
        
        # FILTER: Only process the stream named "customer"
        if name == "customer":
            namespace = s.get("namespace")  # Critical: Keep capturing namespace
            
            supported = s.get("supported_sync_modes", []) or []
            sync_mode = "incremental" if "incremental" in supported else "full_refresh"
            dest_mode = "append"
            
            selected_streams.append({
                "name": name,
                "namespace": namespace,
                "syncMode": sync_mode,
                "destinationSyncMode": dest_mode,
                "selected": True,
            })
            
    print("STREAMS_SELECTED:", len(selected_streams))

    # Create connection
    conn_res = await tools["create_airbyte_connection"](
        connection_name=connection_name,
        source_id=source_id,
        destination_id=destination_id,
        selected_streams=selected_streams,
        schedule_type=schedule_type,
    )
    if not conn_res.get("success"):
        print("Failed to create connection:", conn_res)
        return
    connection_id = conn_res.get("connection_id")
    print("CONNECTION:", {"id": connection_id, "name": connection_name, "status": conn_res.get("status")})

    # Trigger sync
    sync_res = await tools["trigger_airbyte_sync"](connection_id=connection_id, wait_for_completion=wait_sync)
    if not sync_res.get("success"):
        print("Failed to trigger sync:", sync_res)
        return
    job_id = sync_res.get("job_id")
    print("JOB_TRIGGERED:", {"job_id": job_id, "status": sync_res.get("status")})

    # If waiting was requested, final status is already included in sync_res
    if job_id and wait_sync:
        print("JOB_FINAL_STATUS:", sync_res.get("final_status"))


def main():
    asyncio.run(amain())


if __name__ == "__main__":
    main()
