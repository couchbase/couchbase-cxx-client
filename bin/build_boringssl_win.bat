set BORINGSSL_SRC_DIR=%~1
set BORINGSSL_BUILD_DIR=%~2
set BORINGSSL_OUTPUT_DIR=%~3
set BORINGSSL_PREFIX=%~4
set LIB_CRYPTO=%~5
set LIB_SSL=%~6
set BORINGSSL_CMAKE_EXE=%7
set BORINGSSL_CMAKE_GENERATOR=%8
set BORINGSSL_CMAKE_BUILD_TYPE=%9
shift
set BORINGSSL_CMAKE_OPTIONS=%~9

rem Normalize all paths by replacing Unix '/' with Windows '\'.
rem Also all quotes removed from the arguments, so all paths must be quoted below.

set BORINGSSL_SRC_DIR=%BORINGSSL_SRC_DIR:/=\%
set BORINGSSL_BUILD_DIR=%BORINGSSL_BUILD_DIR:/=\%
set BORINGSSL_OUTPUT_DIR=%BORINGSSL_OUTPUT_DIR:/=\%
set BORINGSSL_PREFIX=%BORINGSSL_PREFIX:/=\%
set LIB_CRYPTO=%LIB_CRYPTO:/=\%
set LIB_SSL=%LIB_SSL:/=\%
set BORINGSSL_LIB_DIR=%BORINGSSL_OUTPUT_DIR%\lib
set BORINGSSL_INCLUDE_DIR=%BORINGSSL_OUTPUT_DIR%\include

cd %BORINGSSL_SRC_DIR%

@echo "Starting initial build phase."

%BORINGSSL_CMAKE_EXE% ^
      -G %BORINGSSL_CMAKE_GENERATOR% ^
      -S "%BORINGSSL_SRC_DIR%" 	^
      -B "%BORINGSSL_BUILD_DIR%" ^
      -D CMAKE_BUILD_TYPE=%BORINGSSL_CMAKE_BUILD_TYPE% ^
      %BORINGSSL_CMAKE_OPTIONS%

%BORINGSSL_CMAKE_EXE% ^
      --build "%BORINGSSL_BUILD_DIR%" ^
      --config=%BORINGSSL_CMAKE_BUILD_TYPE% ^
      --verbose ^
      --target crypto ssl

cd "%BORINGSSL_BUILD_DIR%"
if not exist "%BORINGSSL_BUILD_DIR%\ssl\%LIB_SSL%" (
    @echo "Failed to build %LIB_SSL%"
    exit 1
)
if not exist "%BORINGSSL_BUILD_DIR%\crypto\%LIB_CRYPTO%" (
    @echo "Failed to build %LIB_CRYPTO%"
    exit 1
)

if [%BORINGSSL_PREFIX%] NEQ [] (
    cd "%BORINGSSL_SRC_DIR%\util"
    > "%BORINGSSL_OUTPUT_DIR%\symbols.txt" (
        go run read_symbols.go "%BORINGSSL_BUILD_DIR%\ssl\%LIB_SSL%"
        go run read_symbols.go "%BORINGSSL_BUILD_DIR%\crypto\%LIB_CRYPTO%"
    )

    setlocal enableextensions enabledelayedexpansion
    REM space delimited array of symbols to EXCLUDE
    set skipSymbols=snprintf
    set keepSymbols[0]=
    set /a kIdx=0
    for /F "usebackq tokens=*" %%s in ("%BORINGSSL_OUTPUT_DIR%\symbols.txt") do (
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
            echo !keepSymbols[%%i]! > "%BORINGSSL_OUTPUT_DIR%\symbols_parsed.txt"
        ) else (
            echo !keepSymbols[%%i]! >> "%BORINGSSL_OUTPUT_DIR%\symbols_parsed.txt"
        )
    )
    endlocal

    if exist "%BORINGSSL_OUTPUT_DIR%\symbols_parsed.txt" (
        move "%BORINGSSL_OUTPUT_DIR%\symbols_parsed.txt" "%BORINGSSL_OUTPUT_DIR%\symbols.txt"
    )

    rd /S /Q "%BORINGSSL_BUILD_DIR%"

    @echo "Starting build phase with symbol prefixing."
    %BORINGSSL_CMAKE_EXE% ^
          -G %BORINGSSL_CMAKE_GENERATOR% ^
          -S "%BORINGSSL_SRC_DIR%" ^
          -B "%BORINGSSL_BUILD_DIR%" ^
          -DBORINGSSL_PREFIX=%BORINGSSL_PREFIX% ^
          -DBORINGSSL_PREFIX_SYMBOLS="%BORINGSSL_OUTPUT_DIR%/symbols.txt" ^
          %BORINGSSL_CMAKE_OPTIONS%
    %BORINGSSL_CMAKE_EXE% ^
          --build "%BORINGSSL_BUILD_DIR%" ^
          --config=%BORINGSSL_CMAKE_BUILD_TYPE% ^
          --verbose ^
          --target crypto ssl
    cd "%BORINGSSL_BUILD_DIR%"
    if not exist "%BORINGSSL_BUILD_DIR%\symbol_prefix_include\boringssl_prefix_symbols.h" (
        @echo "Failed to build boringssl_prefix_symbols.h"
        exit 1
    )

    copy "%BORINGSSL_BUILD_DIR%\symbol_prefix_include\boringssl_prefix_symbols.h" ^
         "%BORINGSSL_INCLUDE_DIR%"
)

copy "%BORINGSSL_BUILD_DIR%\ssl\%LIB_SSL%" "%BORINGSSL_LIB_DIR%"
copy "%BORINGSSL_BUILD_DIR%\crypto\%LIB_CRYPTO%" "%BORINGSSL_LIB_DIR%"
robocopy "%BORINGSSL_SRC_DIR%\include\openssl" ^
         "%BORINGSSL_INCLUDE_DIR%\openssl" ^
         /s /np /nfl /njh /njs /ndl /nc /ns
@echo "Done.  Libs saved in: %BORINGSSL_LIB_DIR%, headers saved in: %BORINGSSL_INCLUDE_DIR%"
exit 0
