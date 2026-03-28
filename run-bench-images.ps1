Set-Location "C:\Dev\Repos\open-source\bitdex-v2\.claude\worktrees\josh-dump-processor"
Write-Host "Running images 10M benchmark..." -ForegroundColor Cyan
node tools/bench-images-10m.mjs
Write-Host ""
Write-Host "=== Component Timing (from server.log) ===" -ForegroundColor Yellow
Select-String -Path server.log -Pattern "component_timing|save_timing|reload_timing|Enrichment.*loaded"
