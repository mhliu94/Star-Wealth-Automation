# Ready-to-run launcher for this workstation. No passwords are embedded here.
$ErrorActionPreference = 'Stop'
$taskRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../..'))
$runtime = Join-Path $env:USERPROFILE '.cache/codex-runtimes/codex-primary-runtime/dependencies/python/python.exe'
$env:PYTHONPATH = $PSScriptRoot
& $runtime -m starwealth --profile (Join-Path $taskRoot 'work/private/star.session.json') --credentials (Join-Path $taskRoot 'work/private/star.credentials.json') --journal (Join-Path $taskRoot 'work/private/orders.sqlite3') @args
exit $LASTEXITCODE
