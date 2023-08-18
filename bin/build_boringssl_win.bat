@echo off
set BORINGSSL_SRC_DIR=%1
set BORINGSSL_BUILD_DIR=%2
set BORINGSSL_OUTPUT_DIR=%3
set BORINGSSL_PREFIX=%4
set BUILD_TYPE=%5
set PIC=%6
set VERBOSE_MAKEFILE=%7
set "BORINGSSL_LIB_DIR=%BORINGSSL_OUTPUT_DIR%\lib"
set "BORINGSSL_INCLUDE_DIR=%BORINGSSL_OUTPUT_DIR%\include"

cd "%BORINGSSL_SRC_DIR%"
git rev-parse HEAD > "%BORINGSSL_OUTPUT_DIR%\boringssl_sha.txt"

@rd /S /Q build
@md build
@echo "Starting initial build phase."
cmake -S"%BORINGSSL_SRC_DIR%" -B"%BORINGSSL_BUILD_DIR%" -GNinja
ninja -v -C "%BORINGSSL_BUILD_DIR%"

cd "%BORINGSSL_BUILD_DIR%"
if not exist "%BORINGSSL_BUILD_DIR%\ssl\ssl.lib" (
    @echo "Failed to build ssl.lib"
    exit 1
)
if not exist "%BORINGSSL_BUILD_DIR%\crypto\crypto.lib" (
    @echo "Failed to build crypto.lib"
    exit 1
)

cd "%BORINGSSL_SRC_DIR%\util"
>"%BORINGSSL_OUTPUT_DIR%\symbols.txt" (
    go run read_symbols.go "%BORINGSSL_BUILD_DIR%\ssl\ssl.lib"
    go run read_symbols.go "%BORINGSSL_BUILD_DIR%\crypto\crypto.lib"
)

@echo "Starting build phase with symbol prefixing."
cmake -S"%BORINGSSL_SRC_DIR%"^
 -B"%BORINGSSL_BUILD_DIR%"^
 -GNinja^
 -DBORINGSSL_PREFIX=%BORINGSSL_PREFIX%^
 -DBORINGSSL_PREFIX_SYMBOLS="%BORINGSSL_OUTPUT_DIR%/symbols.txt"^
 -DCMAKE_BUILD_TYPE=%BUILD_TYPE%^
 -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=%PIC%^
 -DCMAKE_VERBOSE_MAKEFILE:BOOL=%VERBOSE_MAKEFILE%
ninja -v -C "%BORINGSSL_BUILD_DIR%"
cd "%BORINGSSL_BUILD_DIR%"
if not exist "%BORINGSSL_BUILD_DIR%\symbol_prefix_include\boringssl_prefix_symbols.h" (
    @echo "Failed to build boringssl_prefix_symbols.h"
    exit 1
)

@copy "%BORINGSSL_BUILD_DIR%\ssl\ssl.lib" "%BORINGSSL_LIB_DIR%"
@copy "%BORINGSSL_BUILD_DIR%\crypto\crypto.lib" "%BORINGSSL_LIB_DIR%"
@copy "%BORINGSSL_BUILD_DIR%\symbol_prefix_include\boringssl_prefix_symbols.h" "%BORINGSSL_INCLUDE_DIR%"
@robocopy "%BORINGSSL_SRC_DIR%\include\openssl" "%BORINGSSL_INCLUDE_DIR%\openssl" /s /np /nfl /njh /njs /ndl /nc /ns
@echo "Done.  Libs saved in: %BORINGSSL_LIB_DIR%, headers saved in: %BORINGSSL_INCLUDE_DIR%"
exit 0