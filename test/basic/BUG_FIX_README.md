# Bug Fix: EMFILE Error on Projects with node_modules

## Issue
OpenCode server would crash with "Too many open files" (EMFILE) error when 
scanning projects containing `node_modules` directories. This happened because
the file scanning logic attempted to index thousands of files in node_modules,
exceeding OS file descriptor limits.

## Root Cause
In `packages/opencode/src/file/ripgrep.ts`, the `Ripgrep.files()` function
only excluded `.git/*` by default. When scanning projects with node_modules
(which can contain 10,000+ files), the combination of:
1. fast-glob opening many files in parallel
2. Additional direct filesystem checks

Would exceed the file descriptor limit.

## Fix
Added default glob exclusion patterns for common dependency and build directories:
- node_modules
- bower_components  
- vendor
- dist
- build
- target
- And 20+ other common directories

These are now automatically excluded in both `Ripgrep.files()` and `Ripgrep.search()`.

## Testing
This directory serves as a minimal reproduction case. Run:
```bash
cd packages/opencode
bun test test/file/ripgrep.test.ts
```

The fix has been verified to:
1. Exclude node_modules from file scanning (4 files scanned vs 10,000+)
2. Pass all existing tests
3. Handle projects with large node_modules directories without EMFILE errors
