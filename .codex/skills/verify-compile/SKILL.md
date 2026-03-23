---
name: "verify-compile"
description: "Compile FE Java sources and test sources using Gradle."
---

# Verify Compile Skill

This skill verifies that the FE module compiles successfully by running both
`compileJava` and `compileTestJava` tasks.

## Command

```bash
cd fe && ./gradlew compileJava compileTestJava
