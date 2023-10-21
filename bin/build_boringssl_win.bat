set BORINGSSL_SRC_DIR=%1
set BORINGSSL_BUILD_DIR=%2
set BORINGSSL_OUTPUT_DIR=%3
set BORINGSSL_PREFIX=%4
set LIB_CRYPTO=%5
set LIB_SSL=%6
set BORINGSSL_CMAKE_OPTIONS=%~7
set BORINGSSL_LIB_DIR=%BORINGSSL_OUTPUT_DIR%\lib
set BORINGSSL_INCLUDE_DIR=%BORINGSSL_OUTPUT_DIR%\include

cd %BORINGSSL_SRC_DIR%

rd /S /Q build
md build
@echo "Starting initial build phase."

cmake -S %BORINGSSL_SRC_DIR% ^
      -B %BORINGSSL_BUILD_DIR% ^
      -GNinja ^
      %BORINGSSL_CMAKE_OPTIONS%
cmake --build %BORINGSSL_BUILD_DIR% --verbose --target crypto ssl

cd %BORINGSSL_BUILD_DIR%
if not exist %BORINGSSL_BUILD_DIR%\ssl\%LIB_SSL% (
    @echo "Failed to build %LIB_SSL%"
    exit 1
)
if not exist %BORINGSSL_BUILD_DIR%\crypto\%LIB_CRYPTO% (
    @echo "Failed to build %LIB_CRYPTO%"
    exit 1
)

if [%BORINGSSL_PREFIX%] NEQ [] (
    cd %BORINGSSL_SRC_DIR%\util
    > %BORINGSSL_OUTPUT_DIR%\symbols.txt (
        go run read_symbols.go %BORINGSSL_BUILD_DIR%\ssl\%LIB_SSL%
        go run read_symbols.go %BORINGSSL_BUILD_DIR%\crypto\%LIB_CRYPTO%
    )

    setlocal enableextensions enabledelayedexpansion
    REM space delimited array of symbols to EXCLUDE
    set skipSymbols=snprintf
    set keepSymbols[0]=
    set /a kIdx=0
    for /F "usebackq tokens=*" %%s in (%BORINGSSL_OUTPUT_DIR%\symbols.txt) do (
        set found=
        for %%k in (%skipSymbols%) do (
            if %%k==%%s (
                set found=y
            )
        )
        if not defined found (
            set keepSymbols[!kIdx!]=%%s
            set /A kIdx+=1
        )
    )
    set /a end=%kIdx%-1

    for /L %%i in (0,1,%end%) do (
        if %%i equ 0 (
            echo !keepSymbols[%%i]! > %BORINGSSL_OUTPUT_DIR%\symbols_parsed.txt
        ) else (
            echo !keepSymbols[%%i]! >> %BORINGSSL_OUTPUT_DIR%\symbols_parsed.txt
        )
    )
    endlocal

    if exist %BORINGSSL_OUTPUT_DIR%\symbols_parsed.txt (
        move %BORINGSSL_OUTPUT_DIR%\symbols_parsed.txt %BORINGSSL_OUTPUT_DIR%\symbols.txt
    )

    @echo "Starting build phase with symbol prefixing."
    cmake -S %BORINGSSL_SRC_DIR% ^
          -B %BORINGSSL_BUILD_DIR% ^
          -GNinja ^
          -DBORINGSSL_PREFIX=%BORINGSSL_PREFIX% ^
          -DBORINGSSL_PREFIX_SYMBOLS=%BORINGSSL_OUTPUT_DIR%/symbols.txt ^
          %BORINGSSL_CMAKE_OPTIONS%
    cmake --build %BORINGSSL_BUILD_DIR% --verbose --target crypto ssl
    cd %BORINGSSL_BUILD_DIR%
    if not exist %BORINGSSL_BUILD_DIR%\symbol_prefix_include\boringssl_prefix_symbols.h (
        @echo "Failed to build boringssl_prefix_symbols.h"
        exit 1
    )

    robocopy %BORINGSSL_BUILD_DIR%\symbol_prefix_include ^
             %BORINGSSL_INCLUDE_DIR% boringssl_prefix_symbols.h ^
             /s /np /nfl /njh /njs /ndl /nc /ns
)

robocopy %BORINGSSL_BUILD_DIR%\ssl ^
         %BORINGSSL_LIB_DIR% %LIB_SSL% ^
         /s /np /nfl /njh /njs /ndl /nc /ns
robocopy %BORINGSSL_BUILD_DIR%\crypto ^
         %BORINGSSL_LIB_DIR% %LIB_CRYPTO% ^
         /s /np /nfl /njh /njs /ndl /nc /ns
robocopy %BORINGSSL_SRC_DIR%\include\openssl ^
         %BORINGSSL_INCLUDE_DIR%\openssl ^
         /s /np /nfl /njh /njs /ndl /nc /ns
@echo "Done.  Libs saved in: %BORINGSSL_LIB_DIR%, headers saved in: %BORINGSSL_INCLUDE_DIR%"
exit 0
