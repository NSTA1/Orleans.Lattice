using System.Diagnostics;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Regression coverage for issue #4211: a suite that outlives the bound handed to
/// <see cref="ScriptSuiteProcess.Run"/> must be reported AS a timeout naming the script,
/// and the child must not be left running.
/// <para>
/// Before the fix the helper discarded the <see langword="bool"/> from
/// <c>WaitForExit(int)</c> and read <c>Process.ExitCode</c> on a still-running child,
/// which throws <see cref="InvalidOperationException"/> ("Process must exit before
/// requested information can be determined.") - naming neither the timeout nor the script
/// - and the <c>using</c> block then disposed the handle without killing the child.
/// </para>
/// <para>
/// Deterministic: the bound is deliberately far shorter than the child's lifetime, so the
/// outcome does not depend on host load.
/// </para>
/// </summary>
[TestFixture]
public sealed class ScriptSuiteProcessTimeoutTests
{
    [Test]
    public void Run_suite_outlives_its_bound_throws_a_timeout_naming_the_script_and_kills_the_child()
    {
        var shell = ScriptSuiteProcess.FindExecutable("pwsh") ?? ScriptSuiteProcess.FindExecutable("powershell");
        if (shell is null)
        {
            Assert.Ignore("Neither pwsh nor powershell is available on this host.");
        }

        var directory = Path.Combine(Path.GetTempPath(), "suite-timeout-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        var pidFile = Path.Combine(directory, "child.pid");
        var script = Path.Combine(directory, "sleeps.ps1");

        // The child records its own PID first so the test can prove it was killed rather
        // than leaked, then sleeps far beyond the bound.
        File.WriteAllText(script, $"Set-Content -LiteralPath '{pidFile}' -Value $PID\nStart-Sleep -Seconds 120\nexit 0\n");

        int? childPid = null;
        try
        {
            Exception? thrown = null;
            try
            {
                ScriptSuiteProcess.Run(shell!, script, directory, timeoutMilliseconds: 10_000);
            }
            catch (Exception ex)
            {
                thrown = ex;
            }

            childPid = ReadPid(pidFile);

            Assert.Multiple(() =>
            {
                Assert.That(thrown, Is.InstanceOf<TimeoutException>(),
                    "a bound that is exceeded must fail AS a timeout. Anything else - in particular an "
                    + "InvalidOperationException from reading ExitCode on a running child - names the wrong "
                    + "condition and sends the reader to the fixture's process handling instead.");
                Assert.That(thrown?.Message, Does.Contain("sleeps.ps1"),
                    "the diagnosis must name the child it bounded.");

                // The PID file is written by the script's first line. A host so contended that
                // pwsh had not reached it inside the bound has no PID to check; the timeout
                // assertions above still hold in that case.
                if (childPid is { } pid)
                {
                    Assert.That(WaitUntilGone(pid, TimeSpan.FromSeconds(15)), Is.True,
                        $"the timed-out child (PID {pid}) is still running. A timeout must kill the process "
                        + "tree, or the hung suite keeps consuming the CPU whose contention caused the overrun.");
                }
            });
        }
        finally
        {
            if (childPid is { } pid)
            {
                KillIfAlive(pid);
            }

            try { Directory.Delete(directory, recursive: true); } catch { /* best effort */ }
        }
    }

    private static int? ReadPid(string pidFile)
    {
        try
        {
            return File.Exists(pidFile) && int.TryParse(File.ReadAllText(pidFile).Trim(), out var pid) ? pid : null;
        }
        catch (IOException)
        {
            return null;
        }
    }

    private static bool WaitUntilGone(int pid, TimeSpan within)
    {
        var deadline = Stopwatch.StartNew();
        while (deadline.Elapsed < within)
        {
            if (!IsAlive(pid))
            {
                return true;
            }

            Thread.Sleep(100);
        }

        return !IsAlive(pid);
    }

    private static bool IsAlive(int pid)
    {
        try
        {
            using var process = Process.GetProcessById(pid);
            return !process.HasExited;
        }
        catch (ArgumentException)
        {
            return false;
        }
        catch (InvalidOperationException)
        {
            return false;
        }
    }

    private static void KillIfAlive(int pid)
    {
        try
        {
            using var process = Process.GetProcessById(pid);
            process.Kill(entireProcessTree: true);
        }
        catch (ArgumentException)
        {
            // Already gone.
        }
        catch (InvalidOperationException)
        {
            // Exited between the lookup and the kill.
        }
    }
}
