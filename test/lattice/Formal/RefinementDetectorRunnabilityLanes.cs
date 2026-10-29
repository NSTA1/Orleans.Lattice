using System.Diagnostics;
using System.Text.Json;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// The CI test lanes, as <c>plan-test-matrix.py</c> plans them for a full run,
/// so <see cref="RefinementDetectorRunnabilityTests"/> can ask whether any lane
/// selects a cited detector (#5154).
/// <para>
/// DERIVED, NOT DECLARED. The set of categories no lane selects is not written
/// down anywhere. It falls out of the planner's tier filters, its shard
/// partition and its TLC category shards, which change independently of the
/// refinement notes. A hand-kept list would agree with the planner on the day
/// it was written, and the first new tier or shard would make it wrong in the
/// direction nobody notices. So the planner itself is run, for a full plan with
/// no tier skipped, and its emitted filters are evaluated.
/// </para>
/// <para>
/// THE PACKAGES ARE THE ONES CI SELECTS. A package reaches the planner only
/// through <c>select-test-packages.sh</c>: every <c>src/&lt;package&gt;</c>
/// directory, plus its <c>TEST_ONLY_PACKAGES</c> allow-list. A test project
/// outside that set (a Playwright suite, a benchmark host) is run by no planned
/// lane, so a detector there is reported, not assumed.
/// </para>
/// </summary>
internal sealed class RefinementDetectorRunnabilityLanes
{
    /// <summary>One planned <c>dotnet test</c> invocation.</summary>
    /// <param name="Package">The package, the directory name under <c>test/</c>.</param>
    /// <param name="Label">The planner's label, for messages.</param>
    /// <param name="Tier">The planner's tier (<c>deterministic</c>, <c>coyote</c>, <c>chaos</c> or <c>all</c>).</param>
    /// <param name="Filter">The vstest filter, or null for an unfiltered run.</param>
    public sealed record Lane(string Package, string Label, string Tier, string? Filter);

    private const string WorkflowDirectory = ".github/workflows";

    private static readonly TimeSpan PlannerTimeout = TimeSpan.FromMinutes(2);

    private static readonly TimeSpan ProbeTimeout = TimeSpan.FromSeconds(30);

    private static readonly Regex TestOnlyPackages = new(
        @"^\s*TEST_ONLY_PACKAGES=\(([^)]*)\)",
        RegexOptions.Compiled | RegexOptions.Multiline | RegexOptions.CultureInvariant);

    private static readonly Lazy<RefinementDetectorRunnabilityLanes?> Repository = new(PlanRepository);

    private readonly List<(Lane Lane, Func<string, IReadOnlySet<string>, bool> Selects)> _lanes;

    /// <summary>Builds a lane set from explicit lanes.</summary>
    public RefinementDetectorRunnabilityLanes(IEnumerable<Lane> lanes)
    {
        ArgumentNullException.ThrowIfNull(lanes);
        _lanes = lanes.Select(lane => (lane, RefinementDetectorRunnabilityFilter.Compile(lane.Filter))).ToList();
    }

    /// <summary>Every lane, in planner order.</summary>
    public IReadOnlyList<Lane> All => _lanes.Select(entry => entry.Lane).ToArray();

    /// <summary>The packages at least one lane runs.</summary>
    public IReadOnlySet<string> Packages =>
        _lanes.Select(entry => entry.Lane.Package).ToHashSet(StringComparer.Ordinal);

    /// <summary>
    /// The lanes of <paramref name="package"/> whose filter selects a test of
    /// that full name and those categories.
    /// </summary>
    public IReadOnlyList<Lane> Selecting(string package, string fullName, IReadOnlySet<string> categories)
    {
        ArgumentNullException.ThrowIfNull(package);
        ArgumentNullException.ThrowIfNull(fullName);
        ArgumentNullException.ThrowIfNull(categories);

        return _lanes
            .Where(entry => string.Equals(entry.Lane.Package, package, StringComparison.Ordinal)
                && entry.Selects(fullName, categories))
            .Select(entry => entry.Lane)
            .ToArray();
    }

    /// <summary>
    /// The repository's full CI plan, built once per run. Null only off CI on
    /// a host with no Python interpreter; on a CI runner a missing interpreter
    /// throws, because a gate that cannot see the lanes there must not pass.
    /// </summary>
    public static RefinementDetectorRunnabilityLanes? ForRepository() => Repository.Value;

    /// <summary>
    /// The packages CI selects: every <c>src/</c> directory and the
    /// <c>TEST_ONLY_PACKAGES</c> allow-list of <c>select-test-packages.sh</c>,
    /// kept only where <c>test/&lt;package&gt;</c> holds a test project.
    /// </summary>
    public static IReadOnlyList<string> CiPackages(string repoRoot)
    {
        ArgumentNullException.ThrowIfNull(repoRoot);

        var selector = File.ReadAllText(Path.Combine(repoRoot, WorkflowDirectory, "select-test-packages.sh"));
        var testOnly = TestOnlyPackages.Match(selector);
        if (!testOnly.Success)
        {
            throw new InvalidOperationException(
                "select-test-packages.sh no longer declares TEST_ONLY_PACKAGES=(...), so the set of packages CI "
                + "selects cannot be derived. Update RefinementDetectorRunnabilityLanes.CiPackages.");
        }

        return Directory.EnumerateDirectories(Path.Combine(repoRoot, "src"))
            .Select(Path.GetFileName)
            .Concat(testOnly.Groups[1].Value.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries))
            .OfType<string>()
            .Where(package => Directory.Exists(Path.Combine(repoRoot, "test", package))
                && Directory.EnumerateFiles(Path.Combine(repoRoot, "test", package), "*.Tests.csproj").Any())
            .Distinct(StringComparer.Ordinal)
            .Order(StringComparer.Ordinal)
            .ToArray();
    }

    private static RefinementDetectorRunnabilityLanes? PlanRepository()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();
        var interpreter = FindPython();
        if (interpreter is null)
        {
            if (string.Equals(Environment.GetEnvironmentVariable("GITHUB_ACTIONS"), "true", StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException(
                    "no Python interpreter was found on the CI runner, so the planner's lanes cannot be derived");
            }

            return null;
        }

        var work = Directory.CreateTempSubdirectory("detector-lanes-");
        try
        {
            var packageFile = Path.Combine(work.FullName, "packages.txt");
            var matrixFile = Path.Combine(work.FullName, "matrix.json");
            File.WriteAllLines(packageFile, CiPackages(repoRoot));

            var start = new ProcessStartInfo(interpreter)
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                WorkingDirectory = repoRoot,
            };

            foreach (var argument in new[]
            {
                Path.Combine(repoRoot, WorkflowDirectory, "plan-test-matrix.py"),
                "--packages", packageFile,
                "--shards", Path.Combine(repoRoot, WorkflowDirectory, "test-shards.json"),
                "--durations", Path.Combine(repoRoot, WorkflowDirectory, "test-durations.tsv"),
                "--output-matrix", matrixFile,
                "--report-file", Path.Combine(work.FullName, "report.md"),
            })
            {
                start.ArgumentList.Add(argument);
            }

            var (exitCode, stdout, stderr) = Run(start, PlannerTimeout);

            if (exitCode != 0 || !File.Exists(matrixFile))
            {
                throw new InvalidOperationException(
                    $"plan-test-matrix.py exited {exitCode}: {stderr}{stdout}");
            }

            return new RefinementDetectorRunnabilityLanes(ParseMatrix(File.ReadAllText(matrixFile)));
        }
        finally
        {
            work.Delete(recursive: true);
        }
    }

    /// <summary>
    /// Reads the lanes out of a planner matrix. A policy-skipped item is
    /// refused: this is a full plan, and a skipped item would make a lane look
    /// present that runs nothing.
    /// </summary>
    public static IReadOnlyList<Lane> ParseMatrix(string matrix)
    {
        ArgumentNullException.ThrowIfNull(matrix);

        using var document = JsonDocument.Parse(matrix);
        var lanes = new List<Lane>();
        foreach (var item in document.RootElement.EnumerateArray().SelectMany(leg => leg.GetProperty("items").EnumerateArray()))
        {
            var label = item.GetProperty("label").GetString()!;
            if (item.TryGetProperty("skip", out _))
            {
                throw new InvalidOperationException($"the full plan skipped '{label}', so it is not a full plan");
            }

            var filter = item.GetProperty("filter");
            lanes.Add(new Lane(
                item.GetProperty("package").GetString()!,
                label,
                item.GetProperty("tier").GetString()!,
                filter.ValueKind == JsonValueKind.String ? filter.GetString() : null));
        }

        return lanes;
    }

    private static string? FindPython()
    {
        // `python3` on Windows is often a Store alias stub that prints an error,
        // so each candidate is probed rather than assumed.
        string[] candidates = OperatingSystem.IsWindows() ? ["python", "py", "python3"] : ["python3", "python"];

        foreach (var candidate in candidates)
        {
            try
            {
                var probe = new ProcessStartInfo(candidate)
                {
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false,
                };
                probe.ArgumentList.Add("--version");

                if (Run(probe, ProbeTimeout).ExitCode == 0)
                {
                    return candidate;
                }
            }
            catch (System.ComponentModel.Win32Exception)
            {
                // Not on PATH; try the next candidate.
            }
            catch (TimeoutException)
            {
                // A stub that never exits is not an interpreter; try the next candidate.
            }
        }

        return null;
    }

    /// <summary>
    /// Runs a child process with both pipes draining concurrently and a bounded
    /// wait. On overrun the process tree is killed and a
    /// <see cref="TimeoutException"/> names the child and the bound, so a hung
    /// child fails with a diagnosis rather than a blame-hang abort.
    /// </summary>
    private static (int ExitCode, string StandardOutput, string StandardError) Run(ProcessStartInfo start, TimeSpan timeout)
    {
        using var process = Process.Start(start)
            ?? throw new InvalidOperationException($"could not start {start.FileName}");
        var stdout = process.StandardOutput.ReadToEndAsync();
        var stderr = process.StandardError.ReadToEndAsync();

        if (!process.WaitForExit((int)timeout.TotalMilliseconds))
        {
            process.Kill(entireProcessTree: true);
            throw new TimeoutException(
                $"{start.FileName} {string.Join(' ', start.ArgumentList)} did not exit within {timeout.TotalSeconds:0} s and was killed.");
        }

        return (process.ExitCode, stdout.GetAwaiter().GetResult(), stderr.GetAwaiter().GetResult());
    }
}
