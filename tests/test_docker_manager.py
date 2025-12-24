import pytest
from unittest.mock import patch, MagicMock
from memlayer.core.docker_manager import DockerManager

@pytest.fixture
def mock_subprocess():
    with patch("memlayer.core.docker_manager.subprocess.run") as mock:
        yield mock

@pytest.fixture
def mock_shutil_which():
    with patch("memlayer.core.docker_manager.shutil.which") as mock:
        yield mock

def test_is_docker_available_true(mock_shutil_which):
    mock_shutil_which.return_value = "/usr/bin/docker"
    assert DockerManager.is_docker_available() is True

def test_is_docker_available_false(mock_shutil_which):
    mock_shutil_which.return_value = None
    assert DockerManager.is_docker_available() is False

def test_check_container_status_running(mock_shutil_which, mock_subprocess):
    mock_shutil_which.return_value = "/usr/bin/docker"
    mock_subprocess.return_value.returncode = 0
    mock_subprocess.return_value.stdout = "running\n"

    status = DockerManager.check_container_status("test-container")
    assert status == "running"
    mock_subprocess.assert_called_with(
        ["docker", "inspect", "-f", "{{.State.Status}}", "test-container"],
        capture_output=True,
        text=True
    )

def test_start_falkordb_creates_new(mock_shutil_which, mock_subprocess):
    mock_shutil_which.return_value = "/usr/bin/docker"
    # First call: status check (not found)
    # Second call: docker run

    # We need to simulate subprocess behavior
    def side_effect(args, **kwargs):
        if "inspect" in args:
            # Simulate "not found" which usually means inspect returns non-zero
            res = MagicMock()
            res.returncode = 1
            res.stdout = ""
            return res
        if "run" in args:
            return MagicMock(returncode=0)
        return MagicMock()

    mock_subprocess.side_effect = side_effect

    result = DockerManager.start_falkordb()
    assert result["status"] == "ok"
    assert "created" in result["message"]

def test_stop_falkordb_success(mock_shutil_which, mock_subprocess):
    mock_shutil_which.return_value = "/usr/bin/docker"

    # Status check -> running
    # Stop -> success
    def side_effect(args, **kwargs):
        if "inspect" in args:
            res = MagicMock()
            res.returncode = 0
            res.stdout = "running"
            return res
        if "stop" in args:
            return MagicMock(returncode=0)
        return MagicMock()

    mock_subprocess.side_effect = side_effect

    result = DockerManager.stop_falkordb()
    assert result["status"] == "ok"
