param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ArgsForSuite
)

$scriptPath = Join-Path -Path $PSScriptRoot -ChildPath "memtier_suite.py"
if (-not (Test-Path $scriptPath)) {
    Write-Error "memtier suite script not found: $scriptPath"
    exit 2
}

# Use py launcher on Windows because `python` alias may not be configured.
& py -3 $scriptPath @ArgsForSuite
exit $LASTEXITCODE
