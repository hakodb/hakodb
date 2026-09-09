#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

VERSION="$(sed -n 's/^version = "\([^"]*\)"/\1/p' Cargo.toml | head -n 1)"
if [[ -z "$VERSION" ]]; then
    echo "Unable to determine the FireLite version" >&2
    exit 1
fi

RELEASE_DIR="$ROOT_DIR/target/release"
LINUX_DIR="$RELEASE_DIR/linux-x86_64"
ANDROID_TARGET="aarch64-linux-android"
ANDROID_DIR="$RELEASE_DIR/$ANDROID_TARGET"

echo "Building FireLite v$VERSION for Linux..."
cargo build --release
cargo build --release -p firelite-cli
cargo build --release -p firelite-cloudserver

echo "Compiling benchmark..."
g++ -O2 -std=c++17 -Iinclude benchmark.cpp -L"$RELEASE_DIR" -lfirelite -o "$RELEASE_DIR/benchmark"
g++ -O2 -std=c++17 -pthread sqlite_bench.cpp -lsqlite3 -o "$RELEASE_DIR/sqlite_bench"

echo "Building FireLite for Android ($ANDROID_TARGET)..."
ANDROID_API_LEVEL="${ANDROID_API_LEVEL:-21}"
ANDROID_CLANG=""
for ndk_root in "${ANDROID_NDK_HOME:-}" "${ANDROID_NDK_ROOT:-}" /home/codespace/android-ndk/android-ndk-r26c; do
    candidate="${ndk_root}/toolchains/llvm/prebuilt/linux-x86_64/bin/${ANDROID_TARGET}${ANDROID_API_LEVEL}-clang"
    if [[ -n "$ndk_root" && -x "$candidate" ]]; then
        ANDROID_CLANG="$candidate"
        break
    fi
done

if [[ -z "$ANDROID_CLANG" ]]; then
    echo "Unable to find the Android NDK compiler for $ANDROID_TARGET (API $ANDROID_API_LEVEL)" >&2
    exit 1
fi

export CC_aarch64_linux_android="$ANDROID_CLANG"
export CXX_aarch64_linux_android="${ANDROID_CLANG}++"
export CARGO_TARGET_AARCH64_LINUX_ANDROID_LINKER="$ANDROID_CLANG"
cargo build --release --target aarch64-linux-android

echo "Refreshing Linux bundle..."
rm -rf "$LINUX_DIR"
mkdir -p "$LINUX_DIR"
cp "$RELEASE_DIR/benchmark" \
    "$RELEASE_DIR/sqlite_bench" \
   "$RELEASE_DIR/firelite-cli" \
   "$RELEASE_DIR/firelite-cli.d" \
   "$RELEASE_DIR/firelite-cloudserver" \
   "$RELEASE_DIR/firelite-cloudserver.d" \
   "$RELEASE_DIR/libfirelite.so" \
   "$RELEASE_DIR/libfirelite.rlib" \
   "$RELEASE_DIR/libfirelite.d" \
   "$LINUX_DIR/"

echo "Refreshing Android bundle..."
rm -rf "$ANDROID_DIR"
mkdir -p "$ANDROID_DIR"
cp "$ROOT_DIR/target/$ANDROID_TARGET/release/libfirelite.so" "$ANDROID_DIR/"

LINUX_ARCHIVE="$RELEASE_DIR/linux-build-$VERSION.tar.gz"
ANDROID_ARCHIVE="$RELEASE_DIR/android-build-$VERSION.tar.gz"
rm -f "$LINUX_ARCHIVE" "$ANDROID_ARCHIVE"

echo "Creating archives..."
tar -C "$RELEASE_DIR" -czf "$LINUX_ARCHIVE" linux-x86_64
tar -C "$RELEASE_DIR" -czf "$ANDROID_ARCHIVE" "$ANDROID_TARGET"

echo "Release artifacts created in $RELEASE_DIR"