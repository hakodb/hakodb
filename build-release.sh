#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

TARGET="${1:-all}"
if [[ "$TARGET" != "all" && "$TARGET" != "linux" && "$TARGET" != "android" ]]; then
    echo "Usage: $0 [linux|android]" >&2
    exit 1
fi

VERSION="$(sed -n 's/^version = "\([^"]*\)"/\1/p' Cargo.toml | head -n 1)"
if [[ -z "$VERSION" ]]; then
    echo "Unable to determine the HakoDB version" >&2
    exit 1
fi

RELEASE_DIR="$ROOT_DIR/target/release"
LINUX_DIR="$RELEASE_DIR/linux-x86_64"
ANDROID_TARGET="aarch64-linux-android"
ANDROID_DIR="$RELEASE_DIR/$ANDROID_TARGET"

LINUX_ARCHIVE="$RELEASE_DIR/linux-build-$VERSION.tar.gz"
ANDROID_ARCHIVE="$RELEASE_DIR/android-build-$VERSION.tar.gz"

build_linux() {
    echo "Building HakoDB v$VERSION for Linux (library only)..."
    cargo build --release

    echo "Refreshing Linux bundle..."
    rm -rf "$LINUX_DIR"
    mkdir -p "$LINUX_DIR"
    cp "$RELEASE_DIR/libhakodb.so" \
        "$RELEASE_DIR/libhakodb.rlib" \
        "$RELEASE_DIR/libhakodb.d" \
        "$ROOT_DIR/include/hako.h" \
        "$LINUX_DIR/"

    rm -f "$LINUX_ARCHIVE"
    echo "Creating Linux archive..."
    tar -C "$RELEASE_DIR" -czf "$LINUX_ARCHIVE" linux-x86_64
}

build_android() {
    echo "Building HakoDB for Android ($ANDROID_TARGET)..."
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

    echo "Refreshing Android bundle..."
    rm -rf "$ANDROID_DIR"
    mkdir -p "$ANDROID_DIR"
    cp "$ROOT_DIR/target/$ANDROID_TARGET/release/libhakodb.so" "$ANDROID_DIR/"

    rm -f "$ANDROID_ARCHIVE"
    echo "Creating Android archive..."
    tar -C "$RELEASE_DIR" -czf "$ANDROID_ARCHIVE" "$ANDROID_TARGET"
}

case "$TARGET" in
    all)
        build_linux
        build_android
        ;;
    linux)
        build_linux
        ;;
    android)
        build_android
        ;;
esac

echo "Release artifacts created in $RELEASE_DIR"