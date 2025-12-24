import subprocess
import shutil
import logging
from typing import Dict

logger = logging.getLogger(__name__)

class DockerManager:
    @staticmethod
    def is_docker_available() -> bool:
        return shutil.which("docker") is not None

    @staticmethod
    def check_container_status(container_name: str) -> str:
        if not DockerManager.is_docker_available():
            return "docker_not_found"

        try:
            # Check if container exists and is running
            result = subprocess.run(
                ["docker", "inspect", "-f", "{{.State.Status}}", container_name],
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                return result.stdout.strip()
            else:
                return "not_found"
        except Exception as e:
            logger.error(f"Error checking container status: {e}")
            return "error"

    @staticmethod
    def start_falkordb(container_name: str = "memlayer-falkor", port: int = 6379) -> Dict[str, str]:
        if not DockerManager.is_docker_available():
            return {"status": "error", "message": "Docker CLI not found"}

        current_status = DockerManager.check_container_status(container_name)

        if current_status == "running":
            return {"status": "ok", "message": "FalkorDB is already running"}

        if current_status == "exited" or current_status == "created":
            # Start existing container
            try:
                subprocess.run(["docker", "start", container_name], check=True)
                return {"status": "ok", "message": "FalkorDB started"}
            except subprocess.CalledProcessError as e:
                return {"status": "error", "message": f"Failed to start container: {e}"}

        # Create and run new container
        try:
            cmd = [
                "docker", "run", "-d",
                "-p", f"{port}:6379",
                "--name", container_name,
                "falkordb/falkordb"
            ]
            subprocess.run(cmd, check=True)
            return {"status": "ok", "message": "FalkorDB container created and started"}
        except subprocess.CalledProcessError as e:
            return {"status": "error", "message": f"Failed to create container: {e}"}

    @staticmethod
    def stop_falkordb(container_name: str = "memlayer-falkor") -> Dict[str, str]:
        if not DockerManager.is_docker_available():
            return {"status": "error", "message": "Docker CLI not found"}

        current_status = DockerManager.check_container_status(container_name)

        if current_status == "not_found":
            return {"status": "error", "message": "FalkorDB container not found"}

        try:
            subprocess.run(["docker", "stop", container_name], check=True)
            return {"status": "ok", "message": "FalkorDB stopped"}
        except subprocess.CalledProcessError as e:
            return {"status": "error", "message": f"Failed to stop container: {e}"}

    @staticmethod
    def remove_falkordb(container_name: str = "memlayer-falkor") -> Dict[str, str]:
        if not DockerManager.is_docker_available():
            return {"status": "error", "message": "Docker CLI not found"}

        try:
            subprocess.run(["docker", "rm", "-f", container_name], check=True)
            return {"status": "ok", "message": "FalkorDB container removed"}
        except subprocess.CalledProcessError as e:
            return {"status": "error", "message": f"Failed to remove container: {e}"}
