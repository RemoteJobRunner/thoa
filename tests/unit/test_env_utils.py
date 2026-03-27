import pytest
from thoa.core.env_utils import resolve_environment_spec


class TestResolveEnvironmentSpec:

    def test_none_returns_empty_string(self):
        assert resolve_environment_spec(None) == ""

    def test_yml_file_read(self, tmp_path):
        env_file = tmp_path / "environment.yml"
        env_file.write_text("name: test\ndependencies:\n  - numpy")
        result = resolve_environment_spec(str(env_file))
        assert "numpy" in result
        assert "name: test" in result

    def test_yaml_extension_accepted(self, tmp_path):
        env_file = tmp_path / "env.yaml"
        env_file.write_text("name: env")
        result = resolve_environment_spec(str(env_file))
        assert "name: env" in result

    def test_unsupported_format_raises_value_error(self):
        with pytest.raises(ValueError, match="Unsupported"):
            resolve_environment_spec("config.txt")

    def test_missing_file_raises_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            resolve_environment_spec("/nonexistent/path/env.yml")
