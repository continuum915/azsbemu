<#
.SYNOPSIS
    Publishes SbBrowser as one self-contained executable that runs without .NET installed.
.EXAMPLE
    ./SbBrowser/publish.ps1                       # Windows x64: publish/SbBrowser/win-x64/SbBrowser.App.exe
    ./SbBrowser/publish.ps1 -Runtime linux-x64    # or osx-arm64, win-arm64, ...
#>
param(
    # The .NET runtime identifier to publish for.
    [string] $Runtime = 'win-x64',
    # Where the executable goes (default: publish/SbBrowser/<runtime> at the repo root).
    [string] $Output
)

$ErrorActionPreference = 'Stop'
$repo = Split-Path $PSScriptRoot -Parent
if (-not $Output) { $Output = Join-Path $repo "publish/SbBrowser/$Runtime" }

# IncludeAllContentForSelfExtract bundles wwwroot (SbBrowser's pages, scripts and styles) into the executable, which
# unpacks it to a per-user cache on first run; without it, wwwroot would have to sit next to the executable.
dotnet publish (Join-Path $repo 'SbBrowser/SbBrowser.App/SbBrowser.App.csproj') `
    -c Release -r $Runtime --self-contained true `
    -p:PublishSingleFile=true `
    -p:IncludeAllContentForSelfExtract=true `
    -p:EnableCompressionInSingleFile=true `
    -p:DebugType=none `
    -o $Output
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "Published to $Output"
