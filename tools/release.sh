#!/usr/bin/env bash
# Release script for Bitdex V2
# Usage: ./tools/release.sh [patch|minor|major]
# Default: patch
#
# What it does:
#   1. Ensures working tree is clean
#   2. Bumps version in Cargo.toml
#   3. Updates Cargo.lock
#   4. Commits the version bump
#   5. Creates a git tag (v0.2.0, etc.)
#   6. Pushes commit + tag to origin
#
# CI builds a Docker image when it sees the v* tag.

set -euo pipefail

BUMP="${1:-patch}"

if [[ "$BUMP" != "patch" && "$BUMP" != "minor" && "$BUMP" != "major" ]]; then
    echo "Usage: $0 [patch|minor|major]"
    exit 1
fi

# Must be on main
BRANCH=$(git rev-parse --abbrev-ref HEAD)
if [[ "$BRANCH" != "main" ]]; then
    echo "Error: must be on main branch (currently on '$BRANCH')"
    exit 1
fi

# Working tree must be clean
if ! git diff --quiet || ! git diff --cached --quiet; then
    echo "Error: working tree is not clean. Commit or stash changes first."
    exit 1
fi

# Check for untracked files that matter
UNTRACKED=$(git ls-files --others --exclude-standard)
if [[ -n "$UNTRACKED" ]]; then
    echo "Error: untracked files present. Commit or remove them first."
    echo "$UNTRACKED"
    exit 1
fi

# Pull latest
git pull --ff-only origin main

# Read current version from Cargo.toml (workspace root)
CURRENT=$(grep -m1 '^version = ' Cargo.toml | sed 's/version = "\(.*\)"/\1/')
echo "Current version: $CURRENT"

# Split into parts
IFS='.' read -r MAJOR MINOR PATCH <<< "$CURRENT"

# Bump
case "$BUMP" in
    major) MAJOR=$((MAJOR + 1)); MINOR=0; PATCH=0 ;;
    minor) MINOR=$((MINOR + 1)); PATCH=0 ;;
    patch) PATCH=$((PATCH + 1)) ;;
esac

NEW_VERSION="$MAJOR.$MINOR.$PATCH"
TAG="v$NEW_VERSION"

echo "Bumping: $CURRENT -> $NEW_VERSION ($BUMP)"

# Check tag doesn't already exist
if git rev-parse "$TAG" >/dev/null 2>&1; then
    echo "Error: tag $TAG already exists"
    exit 1
fi

# Update Cargo.toml
sed -i "s/^version = \"$CURRENT\"/version = \"$NEW_VERSION\"/" Cargo.toml

# Update Cargo.lock
cargo generate-lockfile 2>/dev/null || cargo update -w 2>/dev/null || true

# Commit and tag
git add Cargo.toml Cargo.lock
git commit -m "release: $TAG"
git tag -a "$TAG" -m "Release $TAG"

echo ""
echo "Created commit and tag: $TAG"
echo ""

# Push
git push origin main
git push origin "$TAG"

echo ""
echo "Pushed to origin. CI will build the Docker image."
echo "  Image: ghcr.io/$(git remote get-url origin | sed 's|.*github.com[:/]||;s|\.git$||'):$NEW_VERSION"
