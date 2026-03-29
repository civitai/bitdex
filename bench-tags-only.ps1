$port = 3003
$base = "http://localhost:$port"

Write-Host "=== Tags Baseline Benchmark (port $port) ===" -ForegroundColor Cyan

# Check server
try {
    $null = Invoke-WebRequest -Uri "$base/api/health" -TimeoutSec 5
    Write-Host "  Server is up" -ForegroundColor Green
} catch {
    Write-Host "  ERROR: Server not running on $base" -ForegroundColor Red
    exit 1
}

# Submit tags dump
$body = @{
    name = "tags-baseline"
    csv_path = "C:/Dev/Repos/open-source/bitdex-v2/data/load_stage/tags.csv"
    format = "csv"
    slot_field = "imageId"
    columns = @("tagId", "imageId")
    sets_alive = $false
    fields = @(@{ column = "tagId"; target = "tagIds" })
} | ConvertTo-Json -Depth 5

Write-Host "  Submitting tags dump..." -ForegroundColor Yellow
$result = Invoke-WebRequest -Uri "$base/api/indexes/civitai/dumps" -Method PUT -ContentType "application/json" -Body $body
$json = $result.Content | ConvertFrom-Json
$taskId = $json.task_id

if (-not $taskId) {
    Write-Host "  ERROR: No task_id returned: $($result.Content)" -ForegroundColor Red
    exit 1
}

Write-Host "  Task: $taskId" -ForegroundColor Green

# Poll
$start = Get-Date
while ($true) {
    Start-Sleep -Seconds 3
    $elapsed = ((Get-Date) - $start).TotalSeconds
    $task = (Invoke-WebRequest -Uri "$base/api/tasks/$taskId").Content | ConvertFrom-Json
    $rows = if ($task.progress.records_processed) { $task.progress.records_processed } else { 0 }
    $rate = if ($rows -gt 0 -and $elapsed -gt 0) { [math]::Round($rows / $elapsed / 1000) } else { 0 }

    Write-Host ("  [{0:N0}s] {1:N1}M rows ({2}K/s)" -f $elapsed, ($rows / 1e6), $rate)

    if ($task.status -eq "complete") {
        Write-Host "`n  DONE: $([math]::Round($rows / 1e6, 1))M rows in $([math]::Round($elapsed, 1))s ($([math]::Round($rows / $elapsed / 1000))K/s)" -ForegroundColor Green
        break
    }
    if ($task.status -eq "error") {
        Write-Host "`n  ERROR: $($task.error)" -ForegroundColor Red
        break
    }
    if ($elapsed -gt 600) {
        Write-Host "`n  TIMEOUT after 10 minutes" -ForegroundColor Red
        break
    }
}
