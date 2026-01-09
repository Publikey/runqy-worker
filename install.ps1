# runqy-worker Windows installer
# Usage: iwr https://raw.githubusercontent.com/publikey/runqy-worker/main/install.ps1 -useb | iex
#
# Or download and run:
#   Invoke-WebRequest -Uri "https://raw.githubusercontent.com/publikey/runqy-worker/main/install.ps1" -OutFile install.ps1
#   .\install.ps1
#
# Environment variables:
#   $env:VERSION     - Specific version to install (default: latest)
#   $env:INSTALL_DIR - Installation directory (default: %LOCALAPPDATA%\runqy-worker)

$ErrorActionPreference = "Stop"

$Repo = "publikey/runqy-worker"
$BinaryName = "runqy-worker"
$DefaultInstallDir = "$env:LOCALAPPDATA\runqy-worker"
$InstallDir = if ($env:INSTALL_DIR) { $env:INSTALL_DIR } else { $DefaultInstallDir }

function Write-Info { param($Message) Write-Host "[INFO] $Message" -ForegroundColor Green }
function Write-Warn { param($Message) Write-Host "[WARN] $Message" -ForegroundColor Yellow }
function Write-Err { param($Message) Write-Host "[ERROR] $Message" -ForegroundColor Red; exit 1 }
function Write-Header { param($Message) Write-Host "==> $Message" -ForegroundColor Cyan }

function Get-LatestVersion {
    try {
        $release = Invoke-RestMethod "https://api.github.com/repos/$Repo/releases/latest"
        return $release.tag_name
    } catch {
        Write-Err "Failed to get latest version: $_"
    }
}

function Get-Architecture {
    if ([Environment]::Is64BitOperatingSystem) {
        return "amd64"
    } else {
        return "386"
    }
}

function Install-RunqyWorker {
    Write-Header "runqy-worker installer for Windows"

    $Arch = Get-Architecture
    $Version = if ($env:VERSION) { $env:VERSION } else { Get-LatestVersion }
    $VersionNum = $Version.TrimStart('v')

    Write-Info "Installing $BinaryName $Version for windows/$Arch"

    # Construct download URL
    $ArchiveName = "${BinaryName}_${VersionNum}_windows_${Arch}.zip"
    $DownloadUrl = "https://github.com/$Repo/releases/download/$Version/$ArchiveName"

    # Create install directory
    if (-not (Test-Path $InstallDir)) {
        New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
        Write-Info "Created directory: $InstallDir"
    }

    # Download to temp
    $TempFile = Join-Path $env:TEMP $ArchiveName
    Write-Info "Downloading from $DownloadUrl"

    try {
        Invoke-WebRequest -Uri $DownloadUrl -OutFile $TempFile -UseBasicParsing
    } catch {
        Write-Err "Failed to download: $_"
    }

    # Extract
    Write-Info "Extracting..."
    $TempDir = Join-Path $env:TEMP "runqy-worker-install"
    if (Test-Path $TempDir) { Remove-Item $TempDir -Recurse -Force }
    Expand-Archive -Path $TempFile -DestinationPath $TempDir -Force

    # Move binary to install dir
    $BinaryPath = Join-Path $TempDir "$BinaryName.exe"
    if (-not (Test-Path $BinaryPath)) {
        # Try without .exe
        $BinaryPath = Join-Path $TempDir $BinaryName
    }
    Move-Item -Path $BinaryPath -Destination "$InstallDir\$BinaryName.exe" -Force

    # Cleanup
    Remove-Item $TempFile -Force -ErrorAction SilentlyContinue
    Remove-Item $TempDir -Recurse -Force -ErrorAction SilentlyContinue

    # Add to PATH if not already there
    $CurrentPath = [Environment]::GetEnvironmentVariable("PATH", "User")
    if ($CurrentPath -notlike "*$InstallDir*") {
        [Environment]::SetEnvironmentVariable("PATH", "$CurrentPath;$InstallDir", "User")
        Write-Warn "Added $InstallDir to PATH (restart terminal to use)"
    }

    Write-Host ""
    Write-Header "Installation complete!"
    Write-Info "Binary: $InstallDir\$BinaryName.exe"
    Write-Info "Version: $Version"
    Write-Host ""
    Write-Info "Next steps:"
    Write-Host "  1. Restart your terminal (to update PATH)"
    Write-Host ""
    Write-Host "  2. Create a config file:"
    Write-Host "     Invoke-WebRequest -Uri 'https://raw.githubusercontent.com/$Repo/main/config.yml.example' -OutFile config.yml"
    Write-Host ""
    Write-Host "  3. Edit config.yml with your Redis and handler settings"
    Write-Host ""
    Write-Host "  4. Run the worker:"
    Write-Host "     $BinaryName --config config.yml"
    Write-Host ""
    Write-Info "Run '$BinaryName --version' to verify installation"
}

Install-RunqyWorker
