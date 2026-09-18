"""Standalone frontend process for the opaque SPA relay acceptance test."""

import argparse
import json
import os
from pathlib import Path
from typing import Any

from genro_tytx import SUFFIX_TO_TYPE

from kajenn import AsgiConfigBuilder, AsgiServer, BaseApplication
from kajenn import wsx as wsx_module
from kajenn import wsx_payload as wsx_payload_module
from kajenn_orchestra.spa_app import SpaApplication

CODEC_CALLS = {"wsx_to_tytx": 0, "payload_decode": 0, "response_encode": 0}
_to_tytx = wsx_module.to_tytx
_payload_decode = wsx_payload_module.SerializedWsxPayload.decode
_response_encode = wsx_payload_module.WsxResponseEncoder.encode


def counted_to_tytx(*args: Any, **kwargs: Any) -> Any:
    CODEC_CALLS["wsx_to_tytx"] += 1
    return _to_tytx(*args, **kwargs)


def counted_payload_decode(self: Any) -> Any:
    CODEC_CALLS["payload_decode"] += 1
    return _payload_decode(self)


def counted_response_encode(self: Any, *args: Any, **kwargs: Any) -> Any:
    CODEC_CALLS["response_encode"] += 1
    return _response_encode(self, *args, **kwargs)


wsx_module.to_tytx = counted_to_tytx
wsx_payload_module.SerializedWsxPayload.decode = counted_payload_decode
wsx_payload_module.WsxResponseEncoder.encode = counted_response_encode


class ProbeApplication(BaseApplication):
    """Frontend-only probe; importing this module never imports the worker module."""

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        body = json.dumps(
            {
                "pid": os.getpid(),
                "opaque_registered": "OZ" in SUFFIX_TO_TYPE,
                "routing_codec_calls": CODEC_CALLS,
            }
        ).encode()
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"application/json")],
            }
        )
        await send({"type": "http.response.body", "body": body})


def configuration(root_path: Path) -> type[AsgiConfigBuilder]:
    """Build the recipe while keeping the worker class an opaque import string."""

    class FrontConfiguration(AsgiConfigBuilder):
        default_config = False

        def main(self, root: Any) -> None:
            config = root.configuration()
            applications = config.applications()
            applications.application(code="probe", mount="", app_class=ProbeApplication)
            front = applications.application(
                code="main", mount="main", app_class=SpaApplication
            )
            commander = front.orchestration().commander(
                frozen_users_path=str(root_path / "frozen"),
                instance_dir=str(root_path / "i"),
                orchestration_log_path=str(root_path / "orders.log"),
                memory_max_percent=90.0,
                machine_memory_alarm_percent=95.0,
            )
            commander.groups(default="opaque").group(
                name="opaque",
                worker_memory_max_percent=50.0,
                worker_memory_admission_percent=80.0,
                restart_occupancy_max_percent=95.0,
                worker_min_life_seconds=0.0,
                user_idle_freeze_minutes=60.0,
                entry_module="kajenn_orchestra.orchestration.worker_entry",
                worker_class="tests.boundary.opaque_spa_worker:OpaqueSpaWorker",
                main_threadpool_size=4,
                aux_threadpool_size=1,
            )

    return FrontConfiguration


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--port", type=int, required=True)
    options = parser.parse_args()
    options.root.mkdir(parents=True, exist_ok=True)
    AsgiServer(config=configuration(options.root)).serve(host="127.0.0.1", port=options.port)


if __name__ == "__main__":
    main()
