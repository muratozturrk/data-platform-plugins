import os
import sys

def ensure_dirs(path):
    os.makedirs(path, exist_ok=True)
    os.chmod(path, 0o777)
    print(f"✅ Ensured directory: {path} (permissions: 777)")

def main():
    if len(sys.argv) != 2:
        print("Usage: python table_setup.py <namespace.table>")
        print("Example: python table_setup.py db.events")
        sys.exit(1)

    # Parse the namespace.table input
    try:
        namespace, table = sys.argv[1].split(".")
    except ValueError:
        print("❌ Error: Input must be in the format 'namespace.table' (e.g., db.events)")
        sys.exit(1)

    # Define the base directory path
    base_path = os.path.join(".", namespace, table)
    metadata_path = os.path.join(base_path, "metadata")
    data_path = os.path.join(base_path, "data")

    # Ensure both directories exist and are 777
    for path in [metadata_path, data_path]:
        ensure_dirs(path)

    print("🎉 Table directories are ready.")

if __name__ == "__main__":
    main()
