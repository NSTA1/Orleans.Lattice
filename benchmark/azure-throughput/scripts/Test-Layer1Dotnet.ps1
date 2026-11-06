#!/usr/bin/env pwsh
#requires -Version 7.0
[CmdletBinding()] param()
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$tokens = $null; $errors = $null
$report = Join-Path $PSScriptRoot '..\..\performance-report.ps1'
$ast = [System.Management.Automation.Language.Parser]::ParseFile($report, [ref]$tokens, [ref]$errors)
if ($errors.Count -gt 0) { throw ($errors -join "`n") }
foreach ($fn in $ast.FindAll({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] }, $false)) {
    . ([scriptblock]::Create($fn.Extent.Text))
}
$passed = 0
function Assert-Case([string] $Name, [bool] $Condition) {
    if (-not $Condition) { throw "FAIL: $Name" }
    $script:passed++
    Write-Host "PASS: $Name"
}
function Resolve-BenchVmSsh { return @{ Target = 'test-vm'; SshOpts = @() } }
function ssh {
    $command = $args[-1]
    $script:commands.Add($command)
    $global:LASTEXITCODE = 0
    if ($command -match '--version$') { return $script:sdk }
    if ($command -match '--info$') { return "SDK $script:sdk; runtime $script:major.0" }
    if ($command -match '^cat .*runtimeconfig.json$') {
        return "{`"runtimeOptions`":{`"tfm`":`"net$script:compiledMajor.0`",`"framework`":{`"version`":`"$script:compiledMajor.0.0`"}}}"
    }
    return 'mock command output'
}
function scp {
    $global:LASTEXITCODE = 0
    if ($args[-1] -match '^test-vm:') { return }
    [IO.File]::WriteAllText($args[-1], '{"metrics":{"microbench_get_p50_ns":100}}')
}
$runRoot = Join-Path ([IO.Path]::GetTempPath()) ("layer1-dotnet-" + [guid]::NewGuid().ToString('N'))
[IO.Directory]::CreateDirectory($runRoot) | Out-Null
$parameters = Join-Path $runRoot 'parameters.ps1'
[IO.File]::WriteAllText($parameters, "@{ NamePrefix='test'; ResourceGroup='rg-test'; AdminUsername='test'; SshPublicKeyPath='test.pub' }")
$Layer1Rows = @(@{ WorkloadId = 'point-read'; BdnMethod = 'Get'; MetricSlug = 'get' })
$buildCommands = @{}
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..\..')).Path
try {
    foreach ($major in @(10, 11)) {
        $compiledMajor = $major
        $commands = [Collections.Generic.List[string]]::new()
        $sdk = if ($major -eq 11) { '11.0.100-rc.3.26522.148' } else { '10.0.100' }
        $cohorts = @(Invoke-Layer1Cohorts -Prefix "test$major" -WorkloadIds point-read -N 2 -ParametersFilePath $parameters -Fidelity quick -DotnetMajor $major)
        $tc = Get-Layer1Toolchain -DotnetMajor $major
        Assert-Case "net$major returns exactly the requested cohorts, not command output" ($cohorts.Count -eq 2 -and @($cohorts | Where-Object { $_ -isnot [hashtable] }).Count -eq 0)
        $buildCommands[$major] = @($commands | Where-Object { $_ -match ' build ' })[0]
        Assert-Case "net$major build retargets restore and the entire project graph" ($buildCommands[$major].Contains("-p:LatticeBenchmarkTargetFramework=net$major.0 ") -and $buildCommands[$major].Contains('-p:BeforeTargetFrameworkInferenceTargets=') -and $buildCommands[$major].Contains($tc.Dotnet))
        Assert-Case "net$major runs the selected framework and executable" (@($commands | Where-Object { $_ -match ' run ' -and $_ -match "--framework net$major.0" -and $_.Contains($tc.Dotnet) }).Count -eq 2)
        Assert-Case "net$major passes the target to the forking BDN toolchain" (@($commands | Where-Object { $_ -match ' run ' -and $_ -match "BENCH_MICROBENCH_TARGET_FRAMEWORK=net$major.0" }).Count -eq 2)
        Assert-Case "net$major stops workload hosts before measuring" ($commands[0] -eq 'sudo systemctl stop lattice-producer lattice-silo')
        Assert-Case "net$major retains toolchain provenance and console logs" ($cohorts[0].toolchain.sdkVersion -eq $sdk -and $cohorts[0].toolchain.targetFramework -eq "net$major.0" -and (Test-Path $cohorts[0].toolchain.dotnetInfoPath) -and (Test-Path $cohorts[0].consoleLog))
        Assert-Case "net$major records the threading instrumentation difference and retained memory diagnostics" ($cohorts[0].toolchain.threadingDiagnoserEnabled -eq ($major -eq 10) -and $cohorts[0].toolchain.memoryDiagnoserEnabled)
        Assert-Case "net$major install is opt-in" ((@($commands | Where-Object { $_ -match 'dotnet-install.sh --version 11.0.100-rc.3.26522.148' }).Count -eq 1) -eq ($major -eq 11))
    }
    $sdk = '11.0.100-rc.2'
    $commands = [Collections.Generic.List[string]]::new()
    $rejected = $false
    try {
        Invoke-Layer1Cohorts -Prefix wrongsdk -WorkloadIds point-read -N 1 -ParametersFilePath $parameters -DotnetMajor 11 | Out-Null
    } catch {
        if ($_.Exception.Message -notmatch 'wrong SDK') { throw }
        $rejected = $true
    }
    Assert-Case 'wrong RC fails before build or measurement' ($rejected -and @($commands | Where-Object { $_ -match ' build | run ' }).Count -eq 0)
    Assert-Case 'default toolchain remains .NET 10' ((Get-Layer1Toolchain).Dotnet -eq '/usr/bin/dotnet')
    $major = 11
    $sdk = '11.0.100-rc.3.26522.148'
    $compiledMajor = 10
    $commands = [Collections.Generic.List[string]]::new()
    $rejected = $false
    try {
        Invoke-Layer1Cohorts -Prefix wrongtarget -WorkloadIds point-read -N 1 -ParametersFilePath $parameters -DotnetMajor 11 | Out-Null
    } catch {
        if ($_.Exception.Message -notmatch 'compiled target mismatch') { throw }
        $rejected = $true
    }
    Assert-Case 'wrong compiled runtime fails before measurement' ($rejected -and @($commands | Where-Object { $_ -match ' run ' }).Count -eq 0)
    $probeDir = Join-Path $runRoot 'restore-probe'
    [IO.Directory]::CreateDirectory($probeDir) | Out-Null
    $childDir = Join-Path $probeDir 'child'
    [IO.Directory]::CreateDirectory($childDir) | Out-Null
    $probeProject = Join-Path $probeDir 'probe.csproj'
    [IO.File]::WriteAllText($probeProject, '<Project Sdk="Microsoft.NET.Sdk"><PropertyGroup><OutputType>Exe</OutputType><TargetFramework>netstandard2.1</TargetFramework><EnableDefaultCompileItems>false</EnableDefaultCompileItems></PropertyGroup><ItemGroup><Compile Include="Program.cs" /><ProjectReference Include="child/child.csproj" /></ItemGroup></Project>')
    [IO.File]::WriteAllText((Join-Path $childDir 'child.csproj'), '<Project Sdk="Microsoft.NET.Sdk"><PropertyGroup><TargetFramework>netstandard2.1</TargetFramework></PropertyGroup></Project>')
    [IO.File]::WriteAllText((Join-Path $childDir 'Marker.cs'), 'public class Marker {}')
    [IO.File]::WriteAllText((Join-Path $probeDir 'Program.cs'), @'
using System;
using System.Reflection;
using System.Runtime.Versioning;
public class Program
{
    public static void Main()
    {
        Console.WriteLine(typeof(Program).Assembly.GetCustomAttribute<TargetFrameworkAttribute>().FrameworkName);
        Console.WriteLine(typeof(Marker).Assembly.GetCustomAttribute<TargetFrameworkAttribute>().FrameworkName);
        Console.WriteLine(System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription);
    }
}
'@)
    $frameworkArgs = @([regex]::Matches($buildCommands[10], '-p:\S+') | ForEach-Object { $_.Value })
    $targetsPath = Join-Path $repoRoot 'benchmark\host\Bench.Microbench\TargetFrameworkOverride.targets'
    $frameworkArgs = @($frameworkArgs | ForEach-Object {
        if ($_ -like '-p:BeforeTargetFrameworkInferenceTargets=*') { "-p:BeforeTargetFrameworkInferenceTargets=$targetsPath" } else { $_ }
    })
    & dotnet build $probeProject @frameworkArgs --nologo /clp:ErrorsOnly | Out-Host
    Assert-Case 'real implicit restore and build honor the generated framework arguments' ($LASTEXITCODE -eq 0)
    foreach ($projectDir in @($probeDir, $childDir)) {
        $assets = Get-Content -Raw (Join-Path $projectDir 'obj\project.assets.json') | ConvertFrom-Json -AsHashtable
        Assert-Case "restore retargets $([IO.Path]::GetFileName($projectDir)) assets rather than the declared framework" ($assets.targets.ContainsKey('net10.0') -and -not $assets.targets.ContainsKey('.NETStandard,Version=v2.1'))
    }
    $runtimeOutput = @(& dotnet run --project $probeProject @frameworkArgs --framework net10.0 --no-build)
    Assert-Case 'real no-build run finds the retargeted executable' ($LASTEXITCODE -eq 0)
    Assert-Case 'root and referenced assembly metadata target .NET 10' ($runtimeOutput.Count -eq 3 -and $runtimeOutput[0] -eq '.NETCoreApp,Version=v10.0' -and $runtimeOutput[1] -eq '.NETCoreApp,Version=v10.0')
    Assert-Case 'the retargeted executable actually runs on .NET 10' ($runtimeOutput[2] -match '^\.NET 10\.')
    Write-Host "$passed cases passed."
} finally {
    Remove-Item $runRoot -Recurse -Force
}
