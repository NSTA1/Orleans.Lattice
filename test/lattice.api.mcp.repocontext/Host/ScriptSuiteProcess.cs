using System.ComponentModel;
using System.Diagnostics;
using System.Text;

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
    /// <para>
    /// The diagnosis also says where the child was when it was killed (#5783): how long
    /// it had been since either pipe last produced a line (or that neither ever did), the
    /// child's CPU time, and, on Linux, the scheduler state of the child and of every
    /// descendant still running. A starved child (runnable, little CPU), a busy one
    /// (much CPU), a blocked one (sleeping in a git it spawned) and a stopped one read
    /// differently, so a timeout is never again an empty message.
    /// </para>
    /// <para>
    /// The child runs <c>-NonInteractive</c> with its standard input closed, so nothing it
    /// runs can wait on a prompt or a terminal that will never answer.
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
        var psi = new ProcessStartInfo(shell, $"-NoProfile -NonInteractive -File \"{suitePath}\"")
        {
            RedirectStandardInput = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            WorkingDirectory = workingDirectory,
        };

        var clock = Stopwatch.StartNew();
        using var process = Process.Start(psi)!;
        process.StandardInput.Close();
        var stdout = new PipeTail(process.StandardOutput, clock);
        var stderr = new PipeTail(process.StandardError, clock);

        if (!process.WaitForExit(timeoutMilliseconds))
        {
            var where = DescribeWhere(process, clock, stdout, stderr);
            Kill(process);
            throw new TimeoutException(
                $"'{shell} -NoProfile -NonInteractive -File \"{suitePath}\"' did not exit within {timeoutMilliseconds} ms "
                + $"(working directory '{workingDirectory}'). The process tree was killed."
                + Environment.NewLine + where
                + Environment.NewLine + "Captured stdout: " + stdout.DrainOrDescribe()
                + Environment.NewLine + "Captured stderr: " + stderr.DrainOrDescribe());
        }

        return (
            process.ExitCode,
            stdout.Completion.GetAwaiter().GetResult(),
            stderr.Completion.GetAwaiter().GetResult());
    }

    /// <summary>
    /// Where a timed-out child was before it was killed: how long since either pipe last
    /// produced a line, its CPU time, and (Linux) the state of it and its descendants.
    /// </summary>
    private static string DescribeWhere(Process process, Stopwatch clock, PipeTail stdout, PipeTail stderr)
    {
        var now = clock.ElapsedMilliseconds;
        var text = new StringBuilder();
        text.Append("At the kill, ").Append(now).Append(" ms after the start: ");
        var last = Math.Max(stdout.LastLineAtMilliseconds, stderr.LastLineAtMilliseconds);
        if (last < 0)
        {
            text.Append("neither pipe had produced a line");
        }
        else
        {
            text.Append("the last line arrived ").Append(now - last).Append(" ms before it");
        }

        try
        {
            process.Refresh();
            text.Append("; the child had used ")
                .Append((long)process.TotalProcessorTime.TotalMilliseconds)
                .Append(" ms of CPU");
        }
        catch (Exception ex) when (ex is InvalidOperationException or NotSupportedException or Win32Exception)
        {
            text.Append("; the child's CPU time was unreadable");
        }

        text.Append('.');
        if (OperatingSystem.IsLinux())
        {
            foreach (var line in DescribeLinuxTree(process.Id, depth: 0))
            {
                text.Append(Environment.NewLine).Append(line);
            }
        }

        return text.ToString();
    }

    /// <summary>
    /// One line per live process in the tree rooted at <paramref name="pid"/>, read from
    /// <c>/proc</c>: its name, scheduler state, and user and system CPU ticks. Best effort.
    /// </summary>
    private static IEnumerable<string> DescribeLinuxTree(int pid, int depth)
    {
        string stat;
        string children;
        try
        {
            stat = File.ReadAllText($"/proc/{pid}/stat");
            children = File.ReadAllText($"/proc/{pid}/task/{pid}/children");
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            yield break;
        }

        // "pid (comm) state ppid ... utime stime ...": comm may contain spaces, so split after
        // its closing parenthesis. utime and stime are the 12th and 13th fields from there.
        var open = stat.IndexOf('(');
        var close = stat.LastIndexOf(')');
        var comm = open >= 0 && close > open ? stat[(open + 1)..close] : "?";
        var fields = close > 0 && close + 2 < stat.Length ? stat[(close + 2)..].Split(' ') : [];
        var state = fields.Length > 0 ? fields[0] : "?";
        var ticks = fields.Length > 12 ? $"{fields[11]}+{fields[12]}" : "?";
        yield return $"{new string(' ', 2 * depth)}- pid {pid} {comm}: state {state}, cpu ticks user+sys {ticks}";

        if (depth >= 6)
        {
            yield break;
        }

        foreach (var child in children.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            if (int.TryParse(child, out var childPid))
            {
                foreach (var line in DescribeLinuxTree(childPid, depth + 1))
                {
                    yield return line;
                }
            }
        }
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
    /// Reads one of the child's pipes line by line as it arrives, keeping the whole text
    /// and the time its last line arrived, so a kill can say how long the child was silent.
    /// </summary>
    private sealed class PipeTail
    {
        private readonly StringBuilder _text = new();
        private readonly object _gate = new();
        private long _lastLineAt = -1;

        public PipeTail(StreamReader reader, Stopwatch clock)
        {
            Completion = ReadAsync(reader, clock);
        }

        /// <summary>The whole text, one <c>\n</c>-terminated line per line read, once the pipe reaches EOF.</summary>
        public Task<string> Completion { get; }

        /// <summary>Milliseconds after the start at which the last line arrived, or -1 if none has.</summary>
        public long LastLineAtMilliseconds => Interlocked.Read(ref _lastLineAt);

        /// <summary>
        /// Returns whatever the pipe captured before the kill, bounded so a drain that never
        /// reaches EOF cannot replace the timeout diagnosis with a hang of its own.
        /// </summary>
        public string DrainOrDescribe()
        {
            try
            {
                if (Completion.Wait(TimeSpan.FromSeconds(5)))
                {
                    return Completion.GetAwaiter().GetResult();
                }

                lock (_gate)
                {
                    return "<not drained within 5 s of the kill; read so far:>" + Environment.NewLine + _text;
                }
            }
            catch (Exception ex)
            {
                return $"<unreadable: {ex.GetType().Name}: {ex.Message}>";
            }
        }

        private async Task<string> ReadAsync(StreamReader reader, Stopwatch clock)
        {
            while (await reader.ReadLineAsync().ConfigureAwait(false) is { } line)
            {
                lock (_gate)
                {
                    _text.Append(line).Append('\n');
                }

                Interlocked.Exchange(ref _lastLineAt, clock.ElapsedMilliseconds);
            }

            lock (_gate)
            {
                return _text.ToString();
            }
        }
    }
}
