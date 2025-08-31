# VS Code Setup for PySpark Project

## Fix Import Resolution Issues

If you're seeing "Import could not be resolved" errors in VS Code, follow these steps:

### 1. Select the Correct Python Interpreter

1. Open VS Code in this project directory
2. Press `Ctrl+Shift+P` (or `Cmd+Shift+P` on Mac)
3. Type "Python: Select Interpreter"
4. Choose: `./venv/bin/python` (or the full path shown)

### 2. Verify Setup

1. Open any Python file (e.g., `main.py`)
2. Check the bottom-left corner of VS Code - it should show "Python 3.11.x ('venv': venv)"
3. Import errors should disappear

### 3. Test the Setup

Run the test script:
```bash
source venv/bin/activate
python test_import_fix.py
```

You should see `[SUCCESS]` messages indicating all imports work.

### 4. Force Reload (REQUIRED)

After changing the interpreter, you MUST reload VS Code:

1. **Reload Window**: `Ctrl+Shift+P` -> "Developer: Reload Window"
2. If still showing errors: **Clear Python Cache**: `Ctrl+Shift+P` -> "Python: Clear Cache and Reload Window"  
3. If still showing errors: **Restart Pylance**: `Ctrl+Shift+P` -> "Python: Restart Language Server"

### 5. Verify Interpreter Selection

- Bottom-left corner of VS Code should show: `Python 3.11.x ('venv': venv)`
- If it shows system Python, repeat step 1

### 6. Configuration Files

The following files ensure proper VS Code integration:

- `.vscode/settings.json` - VS Code Python configuration
- `pyrightconfig.json` - Pyright/Pylance configuration
- `.python-version` - Python version specification

### 7. Verify Everything Works

1. Open any Python file - no import errors should appear
2. Auto-completion should work for PySpark functions
3. `test_import_fix.py` should run without errors

---

**All PySpark imports have been fixed with explicit imports. VS Code should now work correctly.**