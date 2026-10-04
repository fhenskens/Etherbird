param(
    [Parameter(Mandatory)] [string] $Baseline,
    [Parameter(Mandatory)] [string] $Candidate,
    [string] $OutputDirectory = 'target/admission-results'
)
$ErrorActionPreference = 'Stop'
New-Item -ItemType Directory -Force $OutputDirectory | Out-Null
foreach ($runtime in @('current', 'two', 'default')) {
    foreach ($pass in @(0, 1)) {
        $phases = if ($pass -eq 0) { @('baseline', 'candidate') } else { @('candidate', 'baseline') }
        foreach ($phase in $phases) {
            $build = if ($phase -eq 'baseline') { $Baseline } else { $Candidate }
            $binary = Get-ChildItem -LiteralPath "$build/deps" -Filter 'pool_dispatch-*.exe' |
                Sort-Object LastWriteTime -Descending | Select-Object -First 1
            if (!$binary) { throw "Missing benchmark in $build" }
            & $binary.FullName --run --runtime $runtime |
                Set-Content -Encoding UTF8 "$OutputDirectory/windows-$runtime-$phase-$pass.csv"
            if ($LASTEXITCODE -ne 0) { throw "Benchmark failed: $runtime $phase" }
        }
        Write-Output "Completed allocation comparison: $runtime pass $pass"
    }
}
