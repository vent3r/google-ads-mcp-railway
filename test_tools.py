#!/usr/bin/env python3
"""Verify all MCP tool files compile and are registered in run_server.py.

Usage:  python test_tools.py
Exit 0 = all good, exit 1 = problems found.
"""

import glob
import os
import py_compile
import re
import sys

TOOLS_DIR = os.path.join(os.path.dirname(__file__), "tools")
SERVER_FILE = os.path.join(os.path.dirname(__file__), "run_server.py")

# Utility modules that don't contain @mcp.tool() — skip in registration check
UTILITY_MODULES = {
    "__init__", "helpers", "options", "error_handler",
    "mutation", "validation", "audit", "name_resolver",
}


def check_syntax():
    """py_compile every tools/*.py file."""
    print("=== Syntax Check ===")
    files = sorted(glob.glob(os.path.join(TOOLS_DIR, "*.py")))
    passed = 0
    failed = 0
    for filepath in files:
        name = os.path.relpath(filepath)
        try:
            py_compile.compile(filepath, doraise=True)
            print(f"  [PASS] {name}")
            passed += 1
        except py_compile.PyCompileError as e:
            print(f"  [FAIL] {name}: {e}")
            failed += 1
    print(f"\n{passed}/{passed + failed} files OK")
    if failed:
        print(f"{failed} file(s) FAILED")
    return failed == 0


def check_registration():
    """Compare tool modules on disk vs imports in run_server.py."""
    print("\n=== Registration Check ===")

    # Modules on disk (minus utilities)
    disk_modules = set()
    for filepath in glob.glob(os.path.join(TOOLS_DIR, "*.py")):
        mod = os.path.splitext(os.path.basename(filepath))[0]
        if mod not in UTILITY_MODULES:
            disk_modules.add(mod)

    # Modules imported in run_server.py
    with open(SERVER_FILE) as f:
        server_src = f.read()

    imported = set()
    for m in re.findall(r"from tools import (.+?)(?:#.*)?$", server_src, re.MULTILINE):
        for name in m.split(","):
            name = name.strip().rstrip("\\")
            if name:
                imported.add(name)

    missing = disk_modules - imported
    extra = imported - disk_modules

    ok = True
    if missing:
        print(f"  [WARN] {len(missing)} tool module(s) on disk but NOT imported in run_server.py:")
        for m in sorted(missing):
            print(f"         - tools/{m}.py")
        ok = False
    if extra:
        print(f"  [WARN] {len(extra)} module(s) imported in run_server.py but no file found:")
        for m in sorted(extra):
            print(f"         - tools/{m}.py")
        ok = False
    if ok:
        print(f"  [OK] All {len(disk_modules)} tool modules registered in run_server.py")

    return ok


if __name__ == "__main__":
    s = check_syntax()
    r = check_registration()
    sys.exit(0 if (s and r) else 1)
