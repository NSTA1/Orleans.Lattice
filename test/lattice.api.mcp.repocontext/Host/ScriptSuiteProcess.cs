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
    /// </remarks>
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
        process.WaitForExit(timeoutMilliseconds);
        return (
            process.ExitCode,
            stdoutTask.GetAwaiter().GetResult(),
            stderrTask.GetAwaiter().GetResult());
    }
}
