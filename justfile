# Show available recipes
default:
    @just --list

# Format, lint, and test (run before committing)
check: fmt-check lint test

# Auto-format and apply easy lint fixes
fix:
    cargo fmt
    cargo clippy --all-targets --fix --allow-dirty --allow-staged

# Format all code
fmt:
    cargo fmt

# Fail if code is not formatted
fmt-check:
    cargo fmt --check

# Lint (warnings are errors)
lint:
    cargo clippy --all-targets -- -D warnings

# Build
build:
    cargo build

# Run all tests
test:
    cargo test
