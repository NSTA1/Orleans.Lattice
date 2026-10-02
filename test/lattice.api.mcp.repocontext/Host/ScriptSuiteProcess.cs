using System.Diagnostics;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Shared process plumbing for the fixtures that drive a PowerShell suite under
/// <c>src/lattice.api.mcp.repocontext/scripts/</c>.
/// </summary>
/// <remarks>
/// <para>
/// Five fixtures previously carried byte-identical private copies of the executable
/// probe and of the run block. The duplication was not the only cost: every copy of
/// the run block read the child's two pipes <em>sequentially</em>, which is the
/// documented deadlock this type exists to make unrepeatable.
/// </para>
/// </remarks>
internal static class ScriptSuiteProcess
{
    /// <summary>
    /// Resolves an executable by walking <c>PATH</c>, returning its full path or
    /// <see langword="null"/> when it is not present on this host.
    /// </summary>
    internal static string? FindExecutable(string name)
    {
        var extensions = OperatingSystem.IsWindows()
            ? new[] { ".exe", ".cmd", ".bat" }
            : new[] { string.Empty };

        foreach (var directory in (Environment.GetEnvironmentVariable("PATH") ?? string.Empty)
            .Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries))
        {
            foreach (var extension in extensions)
            {
                try
                {
                    var candidate = Path.Combine(directory.Trim('"'), name + extension);
                    if (File.Exists(candidate))
                    {
                        return candidate;
                    }
                }
                catch (ArgumentException)
                {
                    // A malformed PATH entry is not this fixture's problem.
                }
            }
        }

        return null;
    }

    /// <summary>
    /// Runs <paramref name="suitePath"/> under <paramref name="shell"/> and returns its
    /// exit code together with both captured streams.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Both pipes are drained CONCURRENTLY. Reading one to EOF while the other is
    /// unread deadlocks the moment the child fills the unread pipe's buffer, and it
    /// deadlocks <em>inside</em> the read - before <c>WaitForExit</c> is ever reached -
    /// so the timeout argument cannot bound it. These suites are verbose on both
    /// streams: each prints per-assertion progress and a tally to stdout while a
    /// terminating PowerShell error record goes to stderr, which is exactly the
    /// combination that reaches the deadlock. In CI it would present as a blame-hang
    /// abort naming no fixture and no assertion.
    /// </para>
    /// <para>
    /// The <see langword="bool"/> from <c>WaitForExit(int)</c> is honoured. When it is
    /// <see langword="false"/> the child is STILL RUNNING, so reading
    /// <c>Process.ExitCode</c> throws an <see cref="InvalidOperationException"/> whose
    /// message names neither the suite nor the timeout, and the <c>using</c> block
    /// disposes the handle without killing the child - leaving an orphaned PowerShell
    /// process holding the working directory for the rest of the run. The timeout branch
    /// therefore kills the whole tree, drains whatever both pipes captured before the
    /// kill, and throws a diagnosis naming the shell, the suite, the timeout, and that
    /// partial output.
    /// </para>
    /// </remarks>
    /// <exception cref="TimeoutException">
    /// <paramref name="suitePath"/> did not exit within <paramref name="timeoutMilliseconds"/>.
    /// </exception>
    internal static (int ExitCode, string StandardOutput, string StandardError) Run(
        string shell,
        string suitePath,
        string workingDirectory,
        int timeoutMilliseconds)
    {
        var psi = new ProcessStartInfo(shell, $"-NoProfile -File \"{suitePath}\"")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            WorkingDirectory = workingDirectory,
        };

        using var process = Process.Start(psi)!;
        var stdoutTask = process.StandardOutput.ReadToEndAsync();
        var stderrTask = process.StandardError.ReadToEndAsync();

        if (!process.WaitForExit(timeoutMilliseconds))
        {
            Kill(process);
            throw new TimeoutException(
                $"'{shell} -NoProfile -File \"{suitePath}\"' did not exit within {timeoutMilliseconds} ms "
                + $"(working directory '{workingDirectory}'). The process tree was killed."
                + Environment.NewLine + "Captured stdout: " + DrainOrDescribe(stdoutTask)
                + Environment.NewLine + "Captured stderr: " + DrainOrDescribe(stderrTask));
        }

        return (
            process.ExitCode,
            stdoutTask.GetAwaiter().GetResult(),
            stderrTask.GetAwaiter().GetResult());
    }

    private static void Kill(Process process)
    {
        try
        {
            process.Kill(entireProcessTree: true);
        }
        catch (InvalidOperationException)
        {
            // The child exited between the timeout and the kill.
        }
        catch (NotSupportedException)
        {
            // The platform cannot enumerate the tree; the direct child is already gone.
        }
    }

    /// <summary>
    /// Returns whatever a pipe captured before the kill, bounded so a drain that never
    /// reaches EOF cannot replace the timeout diagnosis with a hang of its own.
    /// </summary>
    private static string DrainOrDescribe(Task<string> pipe)
    {
        try
        {
            return pipe.Wait(TimeSpan.FromSeconds(5))
                ? pipe.GetAwaiter().GetResult()
                : "<not drained within 5 s of the kill>";
        }
        catch (Exception ex)
        {
            return $"<unreadable: {ex.GetType().Name}: {ex.Message}>";
        }
    }
}
