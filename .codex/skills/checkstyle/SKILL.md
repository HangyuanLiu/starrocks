---
name: "checkstyle"
description: "Run Checkstyle for FE core module using Gradle."
---

# Checkstyle Skill

This skill runs the Checkstyle task for the FE module by running both
`checkstyleMain` and `checkstyleTest` tasks.

## Command

```bash
cd fe && ./gradlew checkstyleMain checkstyleTest