using System.Text;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// A benchmark environment variable whose header documentation gives <c>0</c> a
/// meaning (disable, infinite, run forever, a control arm) must not be read
/// through a <c>ReadInt</c> helper that discards <c>0</c> in favour of the
/// default.
/// </summary>
/// <remarks>
/// Regression for the azure-throughput rig: the silo read
/// <c>BENCH_WAL_APPEND_COALESCING_IN_FLIGHT_THRESHOLD</c> and the TCP producer
/// read <c>BENCH_DURATION_SEC</c> through <c>ReadInt</c>, which accepts only
/// positive values. Setting either to its documented <c>0</c> - the coalescing
/// sweep's control arm, and "run forever" - silently ran the default instead,
/// so a 0-versus-default sweep measured the default twice. Both programs carry a
/// <c>ReadIntAllowZero</c> helper for exactly this case; nothing compiles the
/// header prose against the call, so this gate does.
/// </remarks>
[TestFixture]
public sealed class BenchmarkZeroValuedEnvironmentHygieneTests
{
    private static readonly string[] RegressionTargets =
    [
        "benchmark/azure-throughput/Silo/Program.cs",
        "benchmark/azure-throughput/Producer/Program.cs",
    ];

    private static readonly Regex ZeroRejectingReadIntRegex = new(
        @"static\s+int\s+ReadInt\s*\(\s*string\s+\w+\s*,\s*int\s+@?\w+\s*\)\s*\{(?<body>[^}]*)\}",
        RegexOptions.Compiled);

    private static readonly Regex ReadIntCallRegex = new(
        @"\bReadInt\s*\(\s*""(?<name>BENCH_[A-Z0-9_]+)""\s*,\s*(?<default>[^)]*)\)",
        RegexOptions.Compiled);

    private static readonly Regex DocumentedVariableRegex = new(
        @"^//(?<indent>[ \t]{1,4})(?<name>BENCH_[A-Z0-9_]+)\b(?<rest>.*)$",
        RegexOptions.Compiled);

    private static readonly Regex ZeroMeaningRegex = new(
        @"(?<![\w.])0(?![\w.%])\s*(?:=|disables\b|explicitly\b|measures\b|for\b|to\b)|\bset\s+(?:it\s+)?(?:to\s+)?0(?![\w.])",
        RegexOptions.Compiled | RegexOptions.IgnoreCase);

    [Test]
    public void Zero_valued_benchmark_variables_are_not_read_through_a_zero_rejecting_helper()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();
        var violations = new List<string>();
        var examined = new Dictionary<string, AnalysisResult>(StringComparer.Ordinal);

        foreach (var path in HygieneRepository.EnumerateFiles(Path.Combine(repoRoot, "benchmark"), "*.cs"))
        {
            var relative = Path.GetRelativePath(repoRoot, path).Replace('\\', '/');
            var result = Analyse(File.ReadAllText(path, Encoding.UTF8));
            if (result is null)
            {
                continue;
            }

            examined[relative] = result;
            foreach (var name in result.ZeroDiscardingReads)
            {
                violations.Add($"{relative}: {name} documents a meaning for 0 but is read with ReadInt, which replaces 0 with the default");
            }
        }

        Assert.That(examined.Keys, Is.SupersetOf(RegressionTargets),
            "the regression targets of this gate must be examined; the scan is vacuous without them");
        Assert.Multiple(() =>
        {
            Assert.That(examined[RegressionTargets[0]].ZeroDocumented,
                Does.Contain("BENCH_WAL_APPEND_COALESCING_IN_FLIGHT_THRESHOLD"),
                "the silo header parse no longer finds the documented 0 control arm");
            Assert.That(examined[RegressionTargets[1]].ZeroDocumented,
                Does.Contain("BENCH_DURATION_SEC"),
                "the producer header parse no longer finds the documented 0 = run forever");
        });
        Assert.That(violations, Is.Empty,
            "Read a variable whose documented 0 means something through ReadIntAllowZero (or stop documenting a 0):\n"
            + string.Join("\n", violations));
    }

    [Test]
    public void Analyse_reports_a_documented_zero_read_through_ReadInt()
    {
        const string program = """
            // Environment variables:
            //   BENCH_THRESHOLD         Flush depth (default 4). 0
            //                           disables coalescing.
            //   BENCH_DURATION_SEC      run duration in seconds; 0 = run forever (default 300)
            //   BENCH_TICK_HZ           samples per second (default 5)

            var threshold = ReadInt("BENCH_THRESHOLD", 4);
            var duration = ReadInt("BENCH_DURATION_SEC", 300);
            var tick = ReadInt("BENCH_TICK_HZ", 5);

            static int ReadInt(string name, int @default)
            {
                var raw = Environment.GetEnvironmentVariable(name);
                return int.TryParse(raw, out var v) && v > 0 ? v : @default;
            }
            """;

        var result = Analyse(program);

        Assert.That(result, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(result!.ZeroDocumented, Is.EquivalentTo(new[] { "BENCH_THRESHOLD", "BENCH_DURATION_SEC" }));
            Assert.That(result.ZeroDiscardingReads, Is.EquivalentTo(new[] { "BENCH_THRESHOLD", "BENCH_DURATION_SEC" }));
        });
    }

    [Test]
    public void Analyse_accepts_a_zero_read_through_ReadIntAllowZero_or_with_a_zero_default()
    {
        const string program = """
            //   BENCH_BUDGET_SEC        Seconds to wait. Set 0 for infinite.
            //   BENCH_EXPECTED_SILOS    Silo count to wait for (default 0 = no gate).
            //   BENCH_RATIO             Ratio in the range [0.0, 1.0] (default 0.75).

            var budget = ReadIntAllowZero("BENCH_BUDGET_SEC", 30);
            var silos = ReadInt("BENCH_EXPECTED_SILOS", 0);
            var ratio = ReadInt("BENCH_RATIO", 1);

            static int ReadInt(string name, int @default)
            {
                var raw = Environment.GetEnvironmentVariable(name);
                return int.TryParse(raw, out var v) && v > 0 ? v : @default;
            }

            static int ReadIntAllowZero(string name, int @default)
            {
                var raw = Environment.GetEnvironmentVariable(name);
                return int.TryParse(raw, out var v) && v >= 0 ? v : @default;
            }
            """;

        var result = Analyse(program);

        Assert.That(result, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(result!.ZeroDocumented, Is.EquivalentTo(new[] { "BENCH_BUDGET_SEC", "BENCH_EXPECTED_SILOS" }));
            Assert.That(result.ZeroDiscardingReads, Is.Empty);
        });
    }

    [Test]
    public void Analyse_skips_a_file_whose_ReadInt_accepts_zero()
    {
        const string program = """
            //   BENCH_DURATION_SEC      0 = run forever

            var duration = ReadInt("BENCH_DURATION_SEC", 300);

            static int ReadInt(string name, int @default)
            {
                var raw = Environment.GetEnvironmentVariable(name);
                return int.TryParse(raw, out var v) && v >= 0 ? v : @default;
            }
            """;

        Assert.That(Analyse(program), Is.Null);
    }

    /// <summary>
    /// Returns the documented-zero variables of a file that declares a
    /// zero-rejecting <c>ReadInt</c> helper, and those of them it reads through
    /// that helper with a non-zero default; <see langword="null"/> when the file
    /// declares no such helper.
    /// </summary>
    private static AnalysisResult? Analyse(string text)
    {
        var helper = ZeroRejectingReadIntRegex.Match(text);
        if (!helper.Success || !helper.Groups["body"].Value.Contains("> 0", StringComparison.Ordinal))
        {
            return null;
        }

        var documented = DocumentedZeroVariables(text);
        var discarding = new SortedSet<string>(StringComparer.Ordinal);
        foreach (Match call in ReadIntCallRegex.Matches(text))
        {
            var name = call.Groups["name"].Value;
            var defaultValue = call.Groups["default"].Value.Trim();
            if (documented.Contains(name) && defaultValue != "0")
            {
                discarding.Add(name);
            }
        }

        return new AnalysisResult(documented.ToList(), discarding.ToList());
    }

    /// <summary>
    /// Parses the <c>//</c> comment lines documenting each <c>BENCH_*</c> variable
    /// (an entry starts at a shallowly indented variable name and runs through its
    /// more deeply indented continuation lines) and returns the variables whose
    /// entry gives <c>0</c> a meaning.
    /// </summary>
    private static SortedSet<string> DocumentedZeroVariables(string text)
    {
        var entries = new Dictionary<string, StringBuilder>(StringComparer.Ordinal);
        StringBuilder? current = null;
        foreach (var rawLine in text.Split('\n'))
        {
            var line = rawLine.TrimEnd('\r').TrimStart();
            if (!line.StartsWith("//", StringComparison.Ordinal))
            {
                current = null;
                continue;
            }

            var start = DocumentedVariableRegex.Match(line);
            if (start.Success)
            {
                var name = start.Groups["name"].Value;
                if (!entries.TryGetValue(name, out current))
                {
                    current = new StringBuilder();
                    entries[name] = current;
                }
                current.Append(' ').Append(start.Groups["rest"].Value);
                continue;
            }

            current?.Append(' ').Append(line[2..]);
        }

        var zero = new SortedSet<string>(StringComparer.Ordinal);
        foreach (var (name, entry) in entries)
        {
            var normalised = Regex.Replace(entry.ToString(), @"\s+", " ");
            if (ZeroMeaningRegex.IsMatch(normalised))
            {
                zero.Add(name);
            }
        }

        return zero;
    }

    private sealed record AnalysisResult(IReadOnlyList<string> ZeroDocumented, IReadOnlyList<string> ZeroDiscardingReads);
}
