"""Validate the package using the pinned HA Marketplace implementation."""

import ast
import json
from pathlib import Path

from homeassistant.components.marketplace.repositories.integration import (
    _check_loadable_manifest,
)
from homeassistant.components.marketplace.utils.validate import (
    REPOSITORY_MANIFEST_VALUES,
)


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    component = root / "custom_components" / "arlo_local"
    metadata = json.loads((root / "hacs.json").read_text())
    manifest = json.loads((component / "manifest.json").read_text())
    for key, value in metadata.items():
        REPOSITORY_MANIFEST_VALUES[key](value)
    _check_loadable_manifest(component, "arlo_local")
    constants = {
        target.id: ast.literal_eval(node.value)
        for node in ast.parse((component / "const.py").read_text()).body
        if isinstance(node, ast.Assign)
        for target in node.targets
        if isinstance(target, ast.Name) and target.id in {"DOMAIN", "VERSION"}
    }
    assert manifest["domain"] == constants["DOMAIN"]
    assert manifest["version"] == constants["VERSION"]
    assert metadata["name"] == manifest["name"]
    assert metadata["homeassistant"] == "2026.11.3"
    assert (root / "README.md").is_file()
    assert {item.name for item in component.parent.iterdir() if item.is_dir()} == {
        "arlo_local"
    }
    assert all(not item.is_symlink() for item in component.rglob("*"))
    print(f"Marketplace manifest and version validated: {manifest['version']}")


if __name__ == "__main__":
    main()
