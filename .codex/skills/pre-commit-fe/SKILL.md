---
name: "pre-commit-fe"
description: "Prepare FE code before commit: clean build and generate a commit message title."
---

# FE Pre-commit Skill

This skill performs two main tasks before committing FE code:

1. Ensures that FE builds successfully.
2. Generates a concise and meaningful commit message title based on the staged code changes.

---

## Step 1: Verify FE Build

Run the FE clean build to ensure all code compiles correctly:

```bash
./build.sh --fe --clean
