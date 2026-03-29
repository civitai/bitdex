# Create 10M-line images CSV if it doesn't exist
$stageDir = "C:\Dev\Repos\open-source\bitdex-v2\data\load_stage"
$truncated = "$stageDir\images-10m.csv"
if (-not (Test-Path $truncated)) {
    Write-Host "Creating 10M-line images CSV (this takes ~30s)..." -ForegroundColor Cyan
    $reader = [System.IO.StreamReader]::new("$stageDir\images.csv")
    $writer = [System.IO.StreamWriter]::new($truncated)
    $count = 0
    while ($count -lt 10000000 -and -not $reader.EndOfStream) {
        $writer.WriteLine($reader.ReadLine())
        $count++
    }
    $writer.Close()
    $reader.Close()
    Write-Host "  Created $truncated ($count lines)" -ForegroundColor Green
} else {
    Write-Host "  Using existing $truncated" -ForegroundColor Green
}

# Clean data dirs
Remove-Item -Recurse -Force "C:\Dev\Repos\open-source\bitdex-v2\data\indexes\civitai\bitmaps\*" -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force "C:\Dev\Repos\open-source\bitdex-v2\data\indexes\civitai\docs\*" -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force "C:\Dev\Repos\open-source\bitdex-v2\data\shardstore\*" -ErrorAction SilentlyContinue
Remove-Item -Force "C:\Dev\Repos\open-source\bitdex-v2\data\dumps.json" -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force "C:\Dev\Repos\open-source\bitdex-v2\data\wal" -ErrorAction SilentlyContinue
Write-Host "Data dirs cleaned" -ForegroundColor Green

$logFile = "C:\Dev\Repos\open-source\bitdex-v2\.claude\worktrees\josh-dump-processor\server.log"
if (Test-Path $logFile) { Remove-Item $logFile }

# Start server — stderr goes to server.log as UTF-8, stdout to console
$env:RAYON_NUM_THREADS = "28"
Write-Host "Starting bitdex-server (RAYON_NUM_THREADS=28, port=3001)..." -ForegroundColor Cyan
Write-Host "Stderr -> server.log (UTF-8). Press Ctrl+C to stop." -ForegroundColor Yellow

$proc = Start-Process -NoNewWindow -PassThru -FilePath `
    "C:\Dev\Repos\open-source\bitdex-v2\.claude\worktrees\josh-dump-processor\target\release\bitdex-server.exe" `
    -ArgumentList "--port","3001","--data-dir","C:\Dev\Repos\open-source\bitdex-v2\data" `
    -RedirectStandardError $logFile

Write-Host "Server PID: $($proc.Id)" -ForegroundColor Green
Write-Host "Waiting for server to start..."

Start-Sleep -Seconds 10

# Verify server is up
try {
    $health = Invoke-WebRequest -Uri "http://localhost:3001/api/health" -TimeoutSec 5
    Write-Host "Server is up! ($($health.Content))" -ForegroundColor Green
} catch {
    Write-Host "WARNING: Health check failed, server may still be starting..." -ForegroundColor Yellow
}

# Run the benchmark
Write-Host "`nRunning images 10M benchmark..." -ForegroundColor Cyan
Set-Location "C:\Dev\Repos\open-source\bitdex-v2\.claude\worktrees\josh-dump-processor"
node tools/bench-images-10m.mjs

# Show timing results from log
Write-Host "`n=== Timing Results (from server.log) ===" -ForegroundColor Yellow
Get-Content $logFile | Select-String -Pattern "component_timing|save_timing|reload_timing|Enrichment.*loaded|parse\+merge|Component breakdown|parse:|filter:|enrich:|bitmap:|computed:|docstore:"

Write-Host "`nDone. Server still running (PID $($proc.Id)). Kill with: Stop-Process -Id $($proc.Id)" -ForegroundColor Cyan
