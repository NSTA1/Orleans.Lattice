using System.Diagnostics;
using System.Globalization;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.BPlusTree.TraceValidation;

/// <summary>
/// Checks a trace against <c>spec/atomic-commit/AtomicCommit.tla</c> with TLC.
/// <para>
/// The trace specification and its TLC configuration are not files under
/// <c>spec/</c>: any non-markdown file there is a TLC verdict input and would
/// be discovered as a module. They live as fenced blocks in
/// <c>spec/atomic-commit/TraceValidation.md</c>, so the write-up shows exactly
/// what runs, and are extracted here into a scratch directory together with
/// <c>AtomicCommit.tla</c> and the generated trace module. TLC runs with one
/// worker and its <c>states/</c> output stays in the scratch directory, which
/// is deleted afterwards.
/// </para>
/// </summary>
internal static partial class AtomicCommitTraceChecker
{
    /// <summary>The fenced-block info string carrying the trace specification.</summary>
    public const string SpecBlockInfo = "tla AtomicCommitTrace.tla";

    /// <summary>The fenced-block info string carrying the trace specification's TLC configuration.</summary>
    public const string ConfigBlockInfo = "text AtomicCommitTrace.cfg";

    /// <summary>The banner TLC prints when no invariant or property is violated.</summary>
    public const string CleanBanner = "Model checking completed. No error has been found.";

    /// <summary>The prefix of the message the trace specification asserts on a rejected event.</summary>
    public const string RejectionMarker = "TRACE REJECTED";

    /// <summary>Absolute path of the write-up that carries the trace specification.</summary>
    public static string WriteUpPath =>
        Path.Combine(HygieneRepository.FindRepoRoot(), "spec", "atomic-commit", "TraceValidation.md");

    /// <summary>
    /// Resolves the TLA+ toolchain from <c>TLA_TOOLS_JAR</c> (else
    /// <c>tools/tla2tools.jar</c>) and <c>JAVA_HOME</c> (else <c>PATH</c>), the
    /// same variables the Formal harness reads. Returns a reason when either is
    /// missing.
    /// </summary>
    public static (string Java, string Jar)? ResolveToolchain(out string? missing)
    {
        var jar = Environment.GetEnvironmentVariable("TLA_TOOLS_JAR");
        if (string.IsNullOrWhiteSpace(jar))
        {
            jar = Path.Combine(HygieneRepository.FindRepoRoot(), "tools", "tla2tools.jar");
        }

        var executable = OperatingSystem.IsWindows() ? "java.exe" : "java";
        string? java = null;
        var home = Environment.GetEnvironmentVariable("JAVA_HOME");
        if (!string.IsNullOrWhiteSpace(home) && File.Exists(Path.Combine(home, "bin", executable)))
        {
            java = Path.Combine(home, "bin", executable);
        }
        java ??= (Environment.GetEnvironmentVariable("PATH") ?? string.Empty)
            .Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries)
            .Select(directory => Path.Combine(directory.Trim(), executable))
            .FirstOrDefault(File.Exists);

        if (java is null || !File.Exists(jar))
        {
            missing = java is null ? "a Java runtime (JAVA_HOME or PATH)" : $"tla2tools.jar (looked in '{jar}')";
            return null;
        }

        missing = null;
        return (java, jar);
    }

    /// <summary>
    /// Extracts the single fenced block whose info string is
    /// <paramref name="info"/> from <paramref name="markdown"/>. Throws when
    /// there is not exactly one, so a renamed or duplicated block fails loudly
    /// instead of checking the wrong text.
    /// </summary>
    public static string ExtractBlock(string markdown, string info)
    {
        var lines = markdown.Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n');
        var blocks = new List<string>();
        for (var i = 0; i < lines.Length; i++)
        {
            if (lines[i].TrimEnd() != "```" + info)
            {
                continue;
            }

            var end = Array.FindIndex(lines, i + 1, line => line.TrimEnd() == "```");
            if (end < 0)
            {
                throw new InvalidOperationException($"the '```{info}' block in {WriteUpPath} is not closed.");
            }

            blocks.Add(string.Join("\n", lines[(i + 1)..end]) + "\n");
            i = end;
        }

        return blocks.Count == 1
            ? blocks[0]
            : throw new InvalidOperationException(
                $"{WriteUpPath} must carry exactly one '```{info}' block; found {blocks.Count}.");
    }

    /// <summary>
    /// Runs TLC over <paramref name="trace"/> and reports whether the
    /// specification accepted it.
    /// </summary>
    public static TraceCheckResult Check(string java, string jar, IReadOnlyList<SpecTraceEvent> trace)
    {
        var writeUp = File.ReadAllText(WriteUpPath);
        var scratch = Path.Combine(Path.GetTempPath(), "lattice-trace-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(scratch);
        try
        {
            File.Copy(
                Path.Combine(HygieneRepository.FindRepoRoot(), "spec", "atomic-commit", "AtomicCommit.tla"),
                Path.Combine(scratch, "AtomicCommit.tla"));
            File.WriteAllText(Path.Combine(scratch, "AtomicCommitTrace.tla"), ExtractBlock(writeUp, SpecBlockInfo));
            File.WriteAllText(Path.Combine(scratch, "AtomicCommitTrace.cfg"), ExtractBlock(writeUp, ConfigBlockInfo));
            File.WriteAllText(Path.Combine(scratch, "AtomicCommitTraceData.tla"), AtomicCommitTraceRecorder.RenderTraceModule(trace));

            var info = new ProcessStartInfo(java)
            {
                WorkingDirectory = scratch,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };
            foreach (var argument in new[]
            {
                "-XX:+UseParallelGC", "-cp", jar, "tlc2.TLC", "-workers", "1", "-nowarning",
                "-metadir", Path.Combine(scratch, "states"), "-config", "AtomicCommitTrace.cfg", "AtomicCommitTrace.tla",
            })
            {
                info.ArgumentList.Add(argument);
            }

            var stopwatch = Stopwatch.StartNew();
            using var process = Process.Start(info)
                ?? throw new InvalidOperationException("TLC did not start.");
            var stdout = process.StandardOutput.ReadToEndAsync();
            var stderr = process.StandardError.ReadToEndAsync();
            if (!process.WaitForExit(TimeSpan.FromMinutes(2)))
            {
                process.Kill(entireProcessTree: true);
                throw new TimeoutException("TLC did not finish checking the trace within two minutes.");
            }
            stopwatch.Stop();

            var output = stdout.Result + stderr.Result;
            var match = DistinctStates().Match(output);
            var distinct = match.Success ? long.Parse(match.Groups[1].Value, CultureInfo.InvariantCulture) : -1;
            return new TraceCheckResult(process.ExitCode, output, distinct, stopwatch.Elapsed, trace.Count);
        }
        finally
        {
            Directory.Delete(scratch, recursive: true);
        }
    }

    [GeneratedRegex(@"(\d+) distinct states found")]
    private static partial Regex DistinctStates();
}
