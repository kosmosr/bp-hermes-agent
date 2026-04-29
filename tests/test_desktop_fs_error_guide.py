import ast
from pathlib import Path


EXPECTED_FS_ERROR_CODES = {
    "out_of_workspace",
    "catastrophic_workspace_root",
    "unc_not_supported",
    "path_too_long_app",
    "path_too_long_os",
    "invalid_path",
    "invalid_filename",
    "invalid_pattern",
    "not_found",
    "not_a_directory",
    "is_a_directory",
    "not_empty",
    "new_exists",
    "parent_not_found",
    "permission_denied",
    "disk_full",
    "file_busy",
    "file_too_large",
    "not_text_file",
    "search_timeout",
    "approval_denied",
    "approval_timeout",
    "no_workspace",
    "cross_device_dir",
    "service_unavailable",
}


def _load_guide_text() -> str:
    source = Path("gateway/platforms/desktop.py").read_text(encoding="utf-8")
    module = ast.parse(source)
    for node in module.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "_HERMES_FS_ERROR_GUIDE":
                    assert isinstance(node.value, ast.Constant)
                    assert isinstance(node.value.value, str)
                    return node.value.value
    raise AssertionError("_HERMES_FS_ERROR_GUIDE not found")


def test_desktop_fs_error_guide_covers_all_codes():
    guide = _load_guide_text()

    missing = sorted(code for code in EXPECTED_FS_ERROR_CODES if code not in guide)

    assert missing == []

