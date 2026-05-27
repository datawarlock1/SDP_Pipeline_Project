import os
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat

ROOT = Path(__file__).resolve().parent.parent
NOTEBOOKS_DIR = ROOT / "notebooks"
SUPPORTED_EXTENSIONS = {".py", ".ipynb", ".sql", ".scala", ".r"}

DEFAULT_TARGET_ROOT = "/Workspace/Shared/SDP_Pipeline_Project/notebooks"


def collect_notebook_files(root_dir: Path):
    for path in sorted(root_dir.rglob("*")):
        if path.is_file() and path.suffix.lower() in SUPPORTED_EXTENSIONS:
            yield path


def import_file(client: WorkspaceClient, local_path: Path, target_root: str):
    relative_path = local_path.relative_to(NOTEBOOKS_DIR)
    target_path = Path(target_root) / relative_path
    target_path = target_path.as_posix()

    parent_dir = os.path.dirname(target_path)
    if parent_dir and parent_dir != "/":
        print(f"Ensuring parent directory exists: {parent_dir}")
        client._workspace.mkdirs(path=parent_dir)

    print(f"Uploading {local_path} -> {target_path}")
    with local_path.open("rb") as f:
        client._workspace.upload(
            path=target_path,
            content=f,
            format=ImportFormat.AUTO,
            overwrite=True,
        )


def main():
    if not NOTEBOOKS_DIR.exists():
        raise FileNotFoundError(f"Notebooks directory not found: {NOTEBOOKS_DIR}")

    databricks_host = os.environ.get("DATABRICKS_HOST")
    databricks_token = os.environ.get("DATABRICKS_TOKEN")
    if not databricks_host or not databricks_token:
        raise EnvironmentError(
            "DATABRICKS_HOST and DATABRICKS_TOKEN must be set in the environment."
        )

    print("Connecting to Databricks...")
    client = WorkspaceClient()

    target_root = os.environ.get("DATABRICKS_TARGET_ROOT", DEFAULT_TARGET_ROOT)
    print(f"Deploying notebooks to Databricks target path: {target_root}")

    files = list(collect_notebook_files(NOTEBOOKS_DIR))
    if not files:
        print("No notebook files found to deploy.")
        return

    print(f"Found {len(files)} notebook files to deploy.")
    for path in files:
        import_file(client, path, target_root)

    print("Databricks deployment completed successfully.")


if __name__ == "__main__":
    main()
