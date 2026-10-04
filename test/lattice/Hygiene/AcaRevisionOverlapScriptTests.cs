using System.Diagnostics;
using System.IO;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Regression fixture for issue #3588 (Layer 3): the Azure Container Apps
/// throughput rig must not hand a cohort its cluster while a superseded
/// revision is still draining replicas. The fixture dot-sources
/// <c>benchmark/azure-throughput/scripts/aca-common.ps1</c> into a PowerShell
/// process, replaces <c>az</c> and <c>Start-Sleep</c> with in-process stubs,
/// and drives <c>Set-AcaSiloCount</c> and the revision classifier against
/// scripted platform states. Nothing is provisioned and no Azure call is made.
/// </summary>
/// <remarks>
/// Set the <c>ACA_COMMON_SCRIPT_OVERRIDE</c> environment variable to an
/// alternative copy of the script to run the same assertions against it; that
/// is how the fixture's fail-first evidence was produced against the pre-fix
/// script.
/// </remarks>
[TestFixture]
public sealed class AcaRevisionOverlapScriptTests
{
    private const string OverrideVariable = "ACA_COMMON_SCRIPT_OVERRIDE";

    private const int DrainPolls = 3;

    private const string Harness = """
        param(
            [Parameter(Mandatory)][string] $Target,
            [Parameter(Mandatory)][string] $Scenario
        )
        $ErrorActionPreference = 'Stop'
        . $Target

        $OldName = 'bench-silo--0000001'
        $NewName = 'bench-silo--0000002'
        $global:DrainPolls = 0
        $global:ProbeFailures = 0
        $global:ListFailures = 0
        $global:Expected = 0
        $global:RevisionListPolls = 0
        $global:OldProbes = 0
        $global:Parked = $false
        $global:Unexpected = [System.Collections.Generic.List[string]]::new()

        function Start-Sleep { param([int] $Seconds, [int] $Milliseconds) [System.Threading.Thread]::Sleep(20) }

        function Out-Result([string] $Key, [object] $Value) {
            [Console]::Out.WriteLine("RESULT:$Key=$Value")
        }

        function Get-StubArg([object[]] $A, [string] $Flag) {
            for ($i = 0; $i -lt $A.Count - 1; $i++) {
                if ($A[$i] -eq $Flag) { return [string]$A[$i + 1] }
            }
            return $null
        }

        function Write-StubJson([object] $Value) {
            $global:LASTEXITCODE = 0
            ConvertTo-Json -InputObject $Value -Depth 6 -Compress
        }

        function New-StubRevision([string] $Name, [bool] $Active, [int] $Replicas, [string] $State, [object] $Created) {
            [pscustomobject]@{
                name       = $Name
                properties = [pscustomobject]@{ active = $Active; replicas = $Replicas; runningState = $State; createdTime = $Created }
            }
        }

        function New-StubReplicas([int] $Count) {
            $list = @()
            for ($i = 1; $i -le $Count; $i++) {
                $list += [pscustomobject]@{ name = "replica-$i"; properties = [pscustomobject]@{ runningState = 'Running' } }
            }
            return , $list
        }

        function az {
            $a = @($args | ForEach-Object { [string]$_ })
            $verb = (@($a | Select-Object -First 3)) -join ' '
            if ($verb -eq 'containerapp update --name') {
                $global:LASTEXITCODE = 0
                return
            }
            if ($verb -eq 'containerapp revision deactivate') {
                if ((Get-StubArg $a '--revision') -eq $NewName) { $global:Parked = $true }
                $global:LASTEXITCODE = 0
                return
            }
            if ($verb -eq 'containerapp revision list') {
                $query = Get-StubArg $a '--query'
                if ($query) {
                    $global:LASTEXITCODE = 0
                    if ($query -like '*createdTime*') { return "$NewName`t2024-01-02T00:00:00Z" }
                    if ($global:Parked) { return '' }
                    return $NewName
                }
                $global:RevisionListPolls++
                if ($global:RevisionListPolls -le $global:ListFailures) {
                    $global:LASTEXITCODE = 1
                    return 'ERROR: transient revision list failure'
                }
                $draining = $global:RevisionListPolls -le $global:DrainPolls
                $oldReplicas = 0
                $oldState = 'Stopped'
                if ($draining) { $oldReplicas = 2; $oldState = 'Running' }
                $old = New-StubRevision $OldName $false $oldReplicas $oldState '2024-01-01T00:00:00Z'
                if ($global:Parked) {
                    $new = New-StubRevision $NewName $false 0 'Stopped' '2024-01-02T00:00:00Z'
                } else {
                    $new = New-StubRevision $NewName $true 2 'Running' '2024-01-02T00:00:00Z'
                }
                return Write-StubJson @($old, $new)
            }
            if ($verb -eq 'containerapp replica list') {
                $rev = Get-StubArg $a '--revision'
                if ($rev) {
                    if ($rev -eq $OldName) {
                        $global:OldProbes++
                        if ($global:OldProbes -le $global:ProbeFailures) {
                            $global:LASTEXITCODE = 1
                            return 'ERROR: transient replica probe failure'
                        }
                        if ($global:RevisionListPolls -le $global:DrainPolls) { return Write-StubJson (New-StubReplicas 2) }
                    }
                    return Write-StubJson @()
                }
                if ($global:Parked) { return Write-StubJson @() }
                return Write-StubJson (New-StubReplicas $global:Expected)
            }
            $global:Unexpected.Add($a -join ' ')
            $global:LASTEXITCODE = 2
            return 'ERROR: unexpected az call'
        }

        function Write-Outcome {
            Out-Result 'polls' $global:RevisionListPolls
            Out-Result 'oldProbes' $global:OldProbes
            Out-Result 'retiredAtReturn' ($global:RevisionListPolls -gt $global:DrainPolls)
            Out-Result 'unexpected' ($global:Unexpected -join ' ; ')
        }

        $ctx = @{ siloApp = 'bench-silo'; resourceGroup = 'bench-rg' }

        switch ($Scenario) {
            'classify' {
                $cases = @(
                    @{ id = 'deactivated_with_replicas'; keep = 'k'; rc = @{}; revs = @((New-StubRevision 'a' $false 2 'Running' 1), (New-StubRevision 'k' $true 1 'Running' 2)) },
                    @{ id = 'deprovisioning_state'; keep = 'k'; rc = @{}; revs = @((New-StubRevision 'a' $false 0 'Deprovisioning' 1)) },
                    @{ id = 'probe_failed_is_fail_closed'; keep = $null; rc = @{ a = -1 }; revs = @((New-StubRevision 'a' $false 0 'Stopped' 1)) },
                    @{ id = 'probe_found_replicas'; keep = $null; rc = @{ a = 2 }; revs = @((New-StubRevision 'a' $false 0 'Stopped' 1)) },
                    @{ id = 'all_retired'; keep = 'k'; rc = @{ a = 0; b = 0 }; revs = @((New-StubRevision 'a' $false 0 'Stopped' 1), (New-StubRevision 'b' $false 0 'Deprovisioned' 2), (New-StubRevision 'k' $true 1 'Running' 3)) },
                    @{ id = 'park_active_revision_lingers'; keep = $null; rc = @{}; revs = @((New-StubRevision 'k' $true 0 'Stopped' 3)) },
                    @{ id = 'missing_properties'; keep = $null; rc = @{}; revs = @([pscustomobject]@{ name = 'x' }) }
                )
                foreach ($c in $cases) {
                    try {
                        $raw = Get-AcaLingeringRevisions -Revisions $c.revs -KeepRevision $c.keep -ReplicaCounts $c.rc
                        Out-Result $c.id ('{0}|{1}' -f ($raw -is [string[]]), (@($raw) -join ','))
                    } catch {
                        Out-Result $c.id ('error|' + ($_.Exception.Message -replace "`r?`n", ' '))
                    }
                }
                return
            }
            'scale' { $global:DrainPolls = [int]$env:ACA_TEST_DRAIN_POLLS; $global:Expected = 2 }
            'park' { $global:DrainPolls = [int]$env:ACA_TEST_DRAIN_POLLS; $global:Expected = 0 }
            'probefail' { $global:ProbeFailures = [int]$env:ACA_TEST_DRAIN_POLLS; $global:Expected = 2 }
            'listfail' { $global:ListFailures = [int]$env:ACA_TEST_DRAIN_POLLS; $global:Expected = 2 }
            'timeout' { $global:DrainPolls = [int]::MaxValue; $global:Expected = 2 }
            default { throw "Unknown scenario '$Scenario'." }
        }

        $timeout = 600
        if ($Scenario -eq 'timeout') { $timeout = 1 }
        try {
            $null = Set-AcaSiloCount -Context $ctx -Count $global:Expected -TimeoutSec $timeout
            Out-Result 'threw' 'False'
        } catch {
            Out-Result 'threw' 'True'
            Out-Result 'message' ($_.Exception.Message -replace "`r?`n", ' ')
        }
        Write-Outcome
        """;

    /// <summary>
    /// The superseded-revision classifier flags a revision as lingering
    /// whenever any signal says it may still hold replicas, and treats a failed
    /// replica probe as lingering (fail-closed).
    /// </summary>
    [Test]
    public void Get_AcaLingeringRevisions_classifies_every_retirement_signal()
    {
        var (results, output) = RunScenario("classify");

        Assert.Multiple(() =>
        {
            AssertCase(results, output, "deactivated_with_replicas", "True|a");
            AssertCase(results, output, "deprovisioning_state", "True|a");
            AssertCase(results, output, "probe_failed_is_fail_closed", "True|a");
            AssertCase(results, output, "probe_found_replicas", "True|a");
            AssertCase(results, output, "all_retired", "True|");
            AssertCase(results, output, "park_active_revision_lingers", "True|k");
            AssertCase(results, output, "missing_properties", "True|");
        });
    }

    /// <summary>
    /// Scaling to N does not return while the previous cohort's revision is
    /// still draining replicas.
    /// </summary>
    [Test]
    public void Set_AcaSiloCount_scale_waits_for_superseded_revision_to_retire()
    {
        var (results, output) = RunScenario("scale");

        AssertReturnedOnlyAfterRetirement(results, output);
    }

    /// <summary>
    /// Parking at zero does not return while a deactivated revision is still
    /// draining replicas.
    /// </summary>
    [Test]
    public void Set_AcaSiloCount_park_waits_for_deactivated_revision_to_retire()
    {
        var (results, output) = RunScenario("park");

        AssertReturnedOnlyAfterRetirement(results, output);
    }

    /// <summary>
    /// A failing per-revision replica probe counts as not retired, so the hand
    /// over waits until the probe succeeds and reports zero replicas.
    /// </summary>
    [Test]
    public void Set_AcaSiloCount_treats_failed_replica_probe_as_not_retired()
    {
        var (results, output) = RunScenario("probefail");

        Assert.Multiple(() =>
        {
            Assert.That(Get(results, "threw"), Is.EqualTo("False"), output);
            Assert.That(int.Parse(Get(results, "oldProbes")), Is.GreaterThan(DrainPolls), output);
            Assert.That(Get(results, "unexpected"), Is.Empty, output);
        });
    }

    /// <summary>
    /// A failing revision listing counts as not retired, so the hand over
    /// waits until the listing succeeds.
    /// </summary>
    [Test]
    public void Set_AcaSiloCount_treats_failed_revision_list_as_not_retired()
    {
        var (results, output) = RunScenario("listfail");

        Assert.Multiple(() =>
        {
            Assert.That(Get(results, "threw"), Is.EqualTo("False"), output);
            Assert.That(int.Parse(Get(results, "polls")), Is.GreaterThan(DrainPolls), output);
            Assert.That(Get(results, "unexpected"), Is.Empty, output);
        });
    }

    /// <summary>
    /// A revision that never retires fails the cohort with a timeout that
    /// names the lingering revision, rather than letting it be measured.
    /// </summary>
    [Test]
    public void Set_AcaSiloCount_times_out_naming_the_lingering_revision()
    {
        var (results, output) = RunScenario("timeout");

        Assert.Multiple(() =>
        {
            Assert.That(Get(results, "threw"), Is.EqualTo("True"), output);
            Assert.That(Get(results, "message"), Does.Contain("Timed out"), output);
            Assert.That(Get(results, "message"), Does.Contain("bench-silo--0000001"), output);
        });
    }

    private static void AssertReturnedOnlyAfterRetirement(Dictionary<string, string> results, string output)
    {
        Assert.Multiple(() =>
        {
            Assert.That(Get(results, "threw"), Is.EqualTo("False"), output);
            Assert.That(int.Parse(Get(results, "polls")), Is.GreaterThan(DrainPolls), output);
            Assert.That(Get(results, "retiredAtReturn"), Is.EqualTo("True"), output);
            Assert.That(Get(results, "unexpected"), Is.Empty, output);
        });
    }

    private static void AssertCase(Dictionary<string, string> results, string output, string id, string expected) =>
        Assert.That(Get(results, id), Is.EqualTo(expected), $"case {id}:{Environment.NewLine}{output}");

    private static string Get(Dictionary<string, string> results, string key) =>
        results.TryGetValue(key, out var value) ? value : "<missing>";

    private static (Dictionary<string, string> Results, string Output) RunScenario(string scenario)
    {
        var target = Environment.GetEnvironmentVariable(OverrideVariable);
        if (string.IsNullOrWhiteSpace(target))
        {
            target = Path.Combine(
                HygieneRepository.FindRepoRoot(),
                "benchmark", "azure-throughput", "scripts", "aca-common.ps1");
        }

        Assert.That(File.Exists(target), Is.True, $"aca-common.ps1 not found at {target}");

        var script = Path.Combine(Path.GetTempPath(), $"aca-revision-overlap-{Guid.NewGuid():N}.ps1");
        File.WriteAllText(script, Harness);
        try
        {
            var (exitCode, output) = RunShell(
                ["-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", script, "-Target", target, "-Scenario", scenario]);
            Assert.That(exitCode, Is.EqualTo(0), output);

            var results = new Dictionary<string, string>(StringComparer.Ordinal);
            foreach (var line in output.Split('\n'))
            {
                var trimmed = line.TrimEnd('\r');
                if (!trimmed.StartsWith("RESULT:", StringComparison.Ordinal))
                {
                    continue;
                }

                var separator = trimmed.IndexOf('=');
                if (separator > 7)
                {
                    results[trimmed[7..separator]] = trimmed[(separator + 1)..];
                }
            }

            return (results, output);
        }
        finally
        {
            File.Delete(script);
        }
    }

    private static (int ExitCode, string Output) RunShell(string[] arguments)
    {
        foreach (var shell in new[] { "pwsh", "powershell" })
        {
            var startInfo = new ProcessStartInfo(shell)
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            };
            foreach (var argument in arguments)
            {
                startInfo.ArgumentList.Add(argument);
            }

            startInfo.Environment["ACA_TEST_DRAIN_POLLS"] = DrainPolls.ToString(System.Globalization.CultureInfo.InvariantCulture);

            Process? process;
            try
            {
                process = Process.Start(startInfo);
            }
            catch (System.ComponentModel.Win32Exception)
            {
                continue;
            }

            if (process is null)
            {
                continue;
            }

            using (process)
            {
                var stdout = process.StandardOutput.ReadToEndAsync();
                var stderr = process.StandardError.ReadToEndAsync();
                if (!process.WaitForExit(120_000))
                {
                    try
                    {
                        process.Kill(entireProcessTree: true);
                    }
                    catch (InvalidOperationException)
                    {
                    }

                    Assert.Fail($"{shell} did not exit within 120 s.");
                }

                process.WaitForExit();
                return (process.ExitCode, stdout.Result + stderr.Result);
            }
        }

        RequireShell();
        return (-1, string.Empty);
    }

    private static void RequireShell()
    {
        if (string.Equals(Environment.GetEnvironmentVariable("GITHUB_ACTIONS"), "true", StringComparison.OrdinalIgnoreCase))
        {
            Assert.Fail("Neither pwsh nor powershell could be started on a CI runner.");
        }

        Assert.Ignore("Neither pwsh nor powershell is available on this machine.");
    }
}
