"""Test configuration and fixtures for ds-tools."""

import pytest


@pytest.fixture
def temp_dir(tmp_path):
    """Create a temporary directory for testing."""
    return tmp_path


@pytest.fixture
def sample_file_structure(temp_dir):
    """Create a sample file structure for testing path operations."""
    # Create some test files
    (temp_dir / "file1.txt").write_text("content1")
    (temp_dir / "file2.txt").write_text("content2" * 100)

    # Create subdirectory with files
    subdir = temp_dir / "subdir"
    subdir.mkdir()
    (subdir / "file3.txt").write_text("content3" * 50)

    return temp_dir


@pytest.fixture
def mock_ssh_key(temp_dir):
    """Create a mock SSH key file for testing."""
    key_file = temp_dir / "test_key"
    key_file.write_text("mock ssh key content")
    key_file.chmod(0o600)
    return str(key_file)
