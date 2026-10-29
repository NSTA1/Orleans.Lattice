using System.Text.Json;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Pins <c>.github/workflows/tlc-inputs.json</c> (contract C1 of #5148), the one
/// classification of the files that can affect a TLC run. <c>tier-scope.py</c>
/// runs exhaustive TLC only when a pull request changes a verdict input, so a
/// wrong classification is silent: a verdict-affecting file marked routing or
/// neutral merges a TLC regression nobody model-checked, and a new workflow
/// nobody classified is neither run nor reported. This fixture makes both loud
/// (#5149).
/// <para>
/// Three classes. <c>neutral</c> and <c>routing</c> are exclusions; a path is a
/// verdict input when it matches a <c>verdictInputs</c> glob and no exclusion.
/// Globs use only literal characters and <c>*</c>, which matches any run of
/// characters including <c>/</c>, exactly as Python's
/// <c>fnmatch.fnmatchcase</c> reads them in <c>tier-scope.py</c>.
/// </para>
/// </summary>
[TestFixture]
public sealed class TlcInputClassificationTests
{
    private const string InputsFile = ".github/workflows/tlc-inputs.json";
    private const string FormalDirectory = "test/lattice/Formal/";
    private static readonly string[] ClassifiedDirectories = [FormalDirectory, ".github/workflows/", ".github/actions/"];

    private static readonly Regex GlobAlphabet = new(@"^[A-Za-z0-9_./*-]+$", RegexOptions.CultureInvariant);

    private static readonly Regex Declaration = new(
        @"\b(?:class|record|struct|interface|enum)\s+(?:struct\s+|class\s+)?(?<name>[A-Za-z_][A-Za-z0-9_]*)",
        RegexOptions.CultureInvariant);

    private static readonly Regex CommentOrLiteral = new(
        "\"\"\"[\\s\\S]*?\"\"\""
            + "|(?:\\$@|@\\$|@)\"(?:[^\"]|\"\")*\""
            + "|\\$?\"(?:\\\\.|[^\"\\\\\\n])*\""
            + "|'(?:\\\\.|[^'\\\\\\n])'"
            + "|//[^\\n]*"
            + "|/\\*[\\s\\S]*?\\*/",
        RegexOptions.CultureInvariant);

    private static readonly Regex TlcCategory = new(@"Category\(\s*""Tlc", RegexOptions.CultureInvariant);

    /// <summary>
    /// Constructs a neutral file must not contain, because each makes the file
    /// affect a run without being referenced: a TLC case of its own, a category
    /// assignment, or code that runs for the whole assembly.
    /// </summary>
    private static readonly (Regex Pattern, string Reason)[] NeutralProhibitions =
    [
        (TlcCategory, "carries a TLC category"),
        (new(@"\bSetCategory\s*\(", RegexOptions.CultureInvariant), "assigns test categories"),
        (new(@"\bSetUpFixture\b", RegexOptions.CultureInvariant), "declares an assembly-wide set-up fixture"),
        (new(@"\bModuleInitializer\b", RegexOptions.CultureInvariant), "declares a module initializer"),
        (new(@"\[\s*assembly\s*:", RegexOptions.CultureInvariant), "declares an assembly attribute"),
    ];

    /// <summary>
    /// What a routing file must not contain: a TLA+ toolchain pin, whose change
    /// alters every verdict, or a TLC case of its own. Every pin lives in
    /// tools/tlc/toolchain.json and is applied by the toolchain action (#5151),
    /// both verdict inputs, which is what lets the workflows that call the
    /// action be routing.
    /// </summary>
    private static readonly (Regex Pattern, string Reason)[] RoutingProhibitions =
    [
        (new(@"releases/download/\S*tla2tools\.jar|\b[0-9a-f]{64}\s+\S*tla2tools\.jar", RegexOptions.CultureInvariant), "pins the TLA+ jar"),
        (new(@"uses:\s*actions/setup-java", RegexOptions.CultureInvariant), "provisions Java"),
        (new(@"\bjava-version\s*:", RegexOptions.CultureInvariant), "pins the Java version"),
        (new(@"\bpython3?\s+tools/tlc/build\.py", RegexOptions.CultureInvariant), "builds the TLC overlay"),
        (TlcCategory, "carries a TLC category"),
    ];

    private sealed record TlcInputs(string[] VerdictInputs, string[] Routing, string[] Neutral, string[] Toolchain)
    {
        public bool IsNeutral(string path) => Neutral.Any(glob => Matches(glob, path));

        public bool IsRouting(string path) => !IsNeutral(path) && Routing.Any(glob => Matches(glob, path));

        public bool IsVerdictInput(string path) =>
            VerdictInputs.Any(glob => Matches(glob, path)) && !IsNeutral(path) && !Routing.Any(glob => Matches(glob, path));

        public IEnumerable<string> Exclusions => Routing.Concat(Neutral);

        public TlcInputs WithNeutral(params string[] extra) => this with { Neutral = [.. Neutral, .. extra] };

        public TlcInputs WithRouting(params string[] extra) => this with { Routing = [.. Routing, .. extra] };

        public TlcInputs WithVerdictInputs(params string[] extra) => this with { VerdictInputs = [.. VerdictInputs, .. extra] };
    }

    private static string RepoRoot => HygieneRepository.FindRepoRoot();

    [Test]
    public void The_classification_file_parses_with_the_contract_shape()
    {
        var inputs = Load();

        Assert.Multiple(() =>
        {
            foreach (var (name, globs) in new[]
            {
                ("verdictInputs", inputs.VerdictInputs), ("routing", inputs.Routing),
                ("neutral", inputs.Neutral), ("toolchain", inputs.Toolchain),
            })
            {
                Assert.That(globs, Is.Not.Empty, name + " must not be empty");
                Assert.That(globs, Is.Unique, name + " must not repeat a glob");
                foreach (var glob in globs)
                {
                    Assert.That(GlobAlphabet.IsMatch(glob), Is.True,
                        $"{name} glob '{glob}' may use only literal characters and '*', so C# and Python fnmatch agree");
                }
            }
        });
    }

    [Test]
    public void Every_formal_workflow_and_action_file_is_classified_exactly_once()
    {
        var inputs = Load();
        var scoped = ScopedTrackedFiles();

        HygieneDenominator.RequireExamined(scoped.Count, nameof(TlcInputClassificationTests), "files", string.Join(", ", ClassifiedDirectories));
        Assert.That(scoped.Count(path => path.StartsWith(FormalDirectory, StringComparison.Ordinal)), Is.GreaterThan(0));
        Assert.That(scoped.Count(path => path.StartsWith(".github/workflows/", StringComparison.Ordinal)), Is.GreaterThan(0));
        Assert.That(ClassificationProblems(scoped, inputs), Is.Empty);
    }

    [Test]
    public void A_neutral_formal_file_declares_no_type_a_verdict_or_routing_formal_file_references()
    {
        var inputs = Load();
        var formal = FormalFiles();

        Assert.That(formal.Keys.Count(inputs.IsNeutral), Is.GreaterThan(0),
            "no Formal file is neutral, so this rule checks nothing");
        Assert.That(formal.Keys.Count(inputs.IsVerdictInput), Is.GreaterThan(0));
        Assert.That(NeutralRuleViolations(formal, inputs), Is.Empty);
    }

    [Test]
    public void A_routing_file_pins_no_toolchain_and_carries_no_TLC_case()
    {
        var inputs = Load();
        var routing = TrackedRelativeFiles()
            .Where(inputs.IsRouting)
            .ToDictionary(path => path, path => File.ReadAllText(Absolute(path)), StringComparer.Ordinal);

        Assert.That(routing.Keys, Has.Some.StartsWith(".github/workflows/").And.Some.StartsWith(FormalDirectory),
            "routing must cover both the workflow and the Formal routing files, or this rule checks less than it claims");
        Assert.That(RoutingRuleViolations(routing), Is.Empty);
    }

    [Test]
    public void Every_toolchain_glob_is_also_a_verdict_input_glob()
    {
        var inputs = Load();

        Assert.That(inputs.Toolchain, Is.SubsetOf(inputs.VerdictInputs),
            "a toolchain change must rerun TLC, and tier-scope.py gates on verdictInputs alone");
        Assert.That(inputs.Toolchain.Intersect(inputs.Exclusions), Is.Empty);
    }

    [Test]
    public void Every_exact_exclusion_entry_names_a_tracked_file()
    {
        var tracked = TrackedRelativeFiles().ToHashSet(StringComparer.Ordinal);
        var stale = Load().Exclusions.Where(glob => !glob.Contains('*', StringComparison.Ordinal) && !tracked.Contains(glob));

        Assert.That(stale, Is.Empty, "an exclusion entry naming no file is dead configuration; remove it");
    }

    [TestCase("test/lattice/Formal/TlcModelCheckTests.cs")]
    [TestCase("test/lattice/Formal/SpecModuleCases.cs")]
    [TestCase("test/lattice/Formal/RefinementNote.cs")]
    [TestCase("test/lattice/Formal/RefinementPropertyCoverageTests.cs")]
    [TestCase("test/shared/Orleans.Lattice.Testing/Hygiene/HygieneRepository.cs")]
    [TestCase(".github/actions/tla-toolchain/action.yml")]
    [TestCase("tools/tlc/toolchain.json")]
    [TestCase("spec/atomic-commit/AtomicCommit.tla")]
    public void A_file_that_can_change_a_verdict_is_a_verdict_input(string path)
    {
        Assert.That(Load().IsVerdictInput(path), Is.True, path);
    }

    [TestCase("test/lattice/Formal/TlcCiShard.cs")]
    [TestCase(".github/workflows/test-shards.json")]
    [TestCase(".github/workflows/plan-test-matrix.py")]
    [TestCase(".github/workflows/tier-scope.py")]
    [TestCase(".github/workflows/tlc-inputs.json")]
    [TestCase(".github/workflows/ci.yml")]
    [TestCase(".github/workflows/publish.yml")]
    [TestCase(".github/workflows/ci-serial-old.yml")]
    [TestCase(".github/workflows/coverage.yml")]
    [TestCase(".github/workflows/plan-coverage-matrix.py")]
    public void A_routing_file_is_routing_and_not_a_verdict_input(string path)
    {
        var inputs = Load();

        Assert.That(inputs.IsRouting(path), Is.True, path);
        Assert.That(inputs.IsVerdictInput(path), Is.False, path);
    }

    [TestCase("test/lattice/Formal/RefinementDetectorResolverTests.cs")]
    [TestCase(".github/workflows/coverage-catch-up.yml")]
    [TestCase("spec/shard-ownership/Refinement.md")]
    [TestCase("spec/atomic-commit/perturbations/Any.perturbation")]
    public void A_neutral_file_is_not_a_verdict_input(string path)
    {
        var inputs = Load();

        Assert.That(inputs.IsNeutral(path), Is.True, path);
        Assert.That(inputs.IsVerdictInput(path), Is.False, path);
    }

    [Test]
    public void Negative_control_a_neutral_file_whose_type_a_verdict_file_starts_referencing_fails()
    {
        var inputs = SyntheticInputs();
        var neutral = "namespace X;\n// class NotADeclaration\npublic sealed class RefinementProbeTests { var s = \"class AlsoNot\"; }\n";
        var clean = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [FormalDirectory + "RefinementProbeTests.cs"] = neutral,
            [FormalDirectory + "SpecModule.cs"] = "public sealed class SpecModule { /* NotADeclaration AlsoNot */ }\n",
            [FormalDirectory + "TlcCiShard.cs"] = "public static class TlcCiShard { }\n",
        };
        var verdictReferences = new Dictionary<string, string>(clean, StringComparer.Ordinal)
        {
            [FormalDirectory + "SpecModule.cs"] = "public sealed class SpecModule { string Name = nameof(RefinementProbeTests); }\n",
        };
        var routingReferences = new Dictionary<string, string>(clean, StringComparer.Ordinal)
        {
            [FormalDirectory + "TlcCiShard.cs"] = "public static class TlcCiShard { static RefinementProbeTests? probe; }\n",
        };

        Assert.Multiple(() =>
        {
            Assert.That(NeutralRuleViolations(clean, inputs), Is.Empty,
                "control: a neutral file nobody references, with type-like words only in comments and strings, is neutral");
            Assert.That(NeutralRuleViolations(verdictReferences, inputs),
                Has.Some.Contains("RefinementProbeTests").And.Some.Contains("SpecModule.cs"));
            Assert.That(NeutralRuleViolations(routingReferences, inputs),
                Has.Some.Contains("RefinementProbeTests").And.Some.Contains("TlcCiShard.cs"));
        });
    }

    [TestCase("[Category(\"Tlc\")]")]
    [TestCase("[SetUpFixture]")]
    [TestCase("[assembly: Parallelizable]")]
    [TestCase("[ModuleInitializer]")]
    [TestCase("data.SetCategory(\"TlcShardX\");")]
    public void Negative_control_a_neutral_file_that_affects_a_run_without_being_referenced_fails(string construct)
    {
        var files = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [FormalDirectory + "RefinementProbeTests.cs"] = "public sealed class RefinementProbeTests {\n" + construct + "\n}\n",
        };

        Assert.That(NeutralRuleViolations(files, SyntheticInputs()), Has.Some.Contains("RefinementProbeTests.cs"));
    }

    [TestCase("uses: actions/setup-java@v5")]
    [TestCase("java-version: '17'")]
    [TestCase("https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar")]
    [TestCase("echo \"936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88  tools/tla2tools.jar\"")]
    [TestCase("run: python3 tools/tlc/build.py --jar x")]
    [TestCase("[Category(\"Tlc\")]")]
    public void Negative_control_a_routing_file_that_pins_the_toolchain_or_is_a_TLC_case_fails(string construct)
    {
        var routing = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [".github/workflows/plan-test-matrix.py"] =
                "# plans legs; tla2tools.jar, setup-java, java-version and tools/tlc/build.py are provisioned elsewhere\nprint('ok')\n",
        };
        var pinned = new Dictionary<string, string>(routing, StringComparer.Ordinal)
        {
            [".github/workflows/plan-test-matrix.py"] = "# plans legs\n" + construct + "\n",
        };

        Assert.Multiple(() =>
        {
            Assert.That(RoutingRuleViolations(routing), Is.Empty, "control: a routing file that only mentions the toolchain in prose passes");
            Assert.That(RoutingRuleViolations(pinned), Has.Some.Contains("plan-test-matrix.py"));
        });
    }

    [TestCase(".github/workflows/new-lane.yml")]
    [TestCase(".github/workflows/new-helper.py")]
    [TestCase(".github/actions/new-action/action.yml")]
    public void Negative_control_an_unclassified_new_workflow_or_action_file_fails(string path)
    {
        var inputs = Load();
        var scoped = ScopedTrackedFiles();

        Assert.Multiple(() =>
        {
            Assert.That(ClassificationProblems(scoped, inputs), Is.Empty, "control: the real tree is classified");
            Assert.That(ClassificationProblems([.. scoped, path], inputs), Has.Some.Contains(path));
        });
    }

    [Test]
    public void Negative_control_a_file_both_named_as_a_verdict_input_and_excluded_fails()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ClassificationProblems(ScopedTrackedFiles(), Load().WithVerdictInputs(".github/workflows/ci.yml")),
                Has.Some.Contains(".github/workflows/ci.yml is named both as a verdict input and as routing"));
            Assert.That(ClassificationProblems(ScopedTrackedFiles(), Load().WithVerdictInputs(".github/workflows/coverage-catch-up.yml")),
                Has.Some.Contains(".github/workflows/coverage-catch-up.yml is named both as a verdict input and as neutral"));
        });
    }

    [Test]
    public void Negative_control_a_file_matched_by_two_exclusion_entries_fails()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ClassificationProblems(ScopedTrackedFiles(), Load().WithNeutral("test/lattice/Formal/TlcCiShardTests*")),
                Has.Some.Contains("TlcCiShardTests.cs"));
            Assert.That(ClassificationProblems(ScopedTrackedFiles(), Load().WithNeutral("test/lattice/Formal/TlcCiShard.cs")),
                Has.Some.Contains("TlcCiShard.cs"), "a file cannot be both routing and neutral");
        });
    }

    [TestCase("spec/*.md", "spec/README.md", true)]
    [TestCase("spec/*.md", "spec/wal/Refinement.md", true)]
    [TestCase("spec/**/perturbations/**", "spec/atomic-commit/perturbations/A.perturbation", true)]
    [TestCase("spec/**/perturbations/**", "spec/perturbations.md", false)]
    [TestCase(".github/workflows/ci.yml", ".github/workflows/ci.yml", true)]
    [TestCase(".github/workflows/ci.yml", ".github/workflows/ci.yml.bak", false)]
    [TestCase("tools/tla*", "tools/tlc/build.py", false)]
    [TestCase("Directory.Build.*", "test/Directory.Build.props", false)]
    public void Globs_match_as_fnmatch_does(string glob, string path, bool expected)
    {
        Assert.That(Matches(glob, path), Is.EqualTo(expected));
    }

    /// <summary>
    /// Each scoped file must be classified exactly once: matched by one
    /// exclusion entry (routing or neutral) and by no wildcard-free verdict
    /// entry, or by no exclusion entry and at least one verdict entry.
    /// </summary>
    private static List<string> ClassificationProblems(IEnumerable<string> scopedPaths, TlcInputs inputs)
    {
        var problems = new List<string>();
        foreach (var path in scopedPaths)
        {
            var excluded = inputs.Exclusions.Where(glob => Matches(glob, path)).ToArray();
            var verdict = inputs.VerdictInputs.Where(glob => Matches(glob, path)).ToArray();
            if (excluded.Length > 1)
            {
                problems.Add($"{path} is matched by {excluded.Length} routing/neutral entries ({string.Join(", ", excluded)}); name it once");
            }
            else if (excluded.Length == 1 && verdict.Any(glob => !glob.Contains('*', StringComparison.Ordinal)))
            {
                problems.Add($"{path} is named both as a verdict input and as {(inputs.IsNeutral(path) ? "neutral" : "routing")}");
            }
            else if (excluded.Length == 0 && verdict.Length == 0)
            {
                problems.Add($"{path} is unclassified: add it to {InputsFile} as a verdict input, routing or neutral");
            }
        }

        return problems;
    }

    /// <summary>
    /// A neutral Formal file declares no type that a verdict-affecting or
    /// routing Formal file mentions anywhere in its text (a conservative
    /// identifier check), and contains no construct that affects a run without
    /// being referenced.
    /// </summary>
    private static List<string> NeutralRuleViolations(IReadOnlyDictionary<string, string> formalFiles, TlcInputs inputs)
    {
        var violations = new List<string>();
        var onPath = formalFiles.Where(file => inputs.IsVerdictInput(file.Key) || inputs.IsRouting(file.Key)).ToArray();

        foreach (var (path, text) in formalFiles.Where(file => inputs.IsNeutral(file.Key)))
        {
            foreach (var (pattern, reason) in NeutralProhibitions)
            {
                if (pattern.IsMatch(text))
                {
                    violations.Add($"neutral {path} {reason}");
                }
            }

            var declared = Declaration.Matches(CommentOrLiteral.Replace(text, " "))
                .Select(match => match.Groups["name"].Value)
                .Distinct(StringComparer.Ordinal);
            foreach (var type in declared)
            {
                var word = new Regex(@"\b" + Regex.Escape(type) + @"\b", RegexOptions.CultureInvariant);
                foreach (var (otherPath, otherText) in onPath)
                {
                    if (word.IsMatch(otherText))
                    {
                        violations.Add($"neutral {path} declares {type}, which {otherPath} on the TLC path references; reclassify it");
                    }
                }
            }
        }

        return violations;
    }

    private static List<string> RoutingRuleViolations(IReadOnlyDictionary<string, string> routingFiles) =>
        routingFiles
            .SelectMany(file => RoutingProhibitions
                .Where(rule => rule.Pattern.IsMatch(file.Value))
                .Select(rule => $"routing {file.Key} {rule.Reason}; a routing file cannot change a verdict, so classify it as a verdict input"))
            .ToList();

    private static TlcInputs SyntheticInputs() => new(
        [FormalDirectory + "*"],
        [FormalDirectory + "TlcCiShard.cs"],
        [FormalDirectory + "RefinementProbeTests.cs"],
        ["tools/tlc/*"]);

    private static bool Matches(string glob, string path) =>
        Regex.IsMatch(path, "^" + string.Join(".*", glob.Split('*').Select(Regex.Escape)) + "$", RegexOptions.CultureInvariant);

    private static TlcInputs Load()
    {
        using var document = JsonDocument.Parse(File.ReadAllText(Absolute(InputsFile)));
        var root = document.RootElement;

        Assert.That(root.GetProperty("schema").GetInt32(), Is.EqualTo(1), "C1 schema");
        return new TlcInputs(Globs(root, "verdictInputs"), Globs(root, "routing"), Globs(root, "neutral"), Globs(root, "toolchain"));
    }

    private static string[] Globs(JsonElement root, string name) =>
        root.GetProperty(name).EnumerateArray().Select(element => element.GetString()!).ToArray();

    private static Dictionary<string, string> FormalFiles() =>
        ScopedTrackedFiles()
            .Where(path => path.StartsWith(FormalDirectory, StringComparison.Ordinal))
            .ToDictionary(path => path, path => File.ReadAllText(Absolute(path)), StringComparer.Ordinal);

    private static List<string> ScopedTrackedFiles() =>
        TrackedRelativeFiles()
            .Where(path => ClassifiedDirectories.Any(directory => path.StartsWith(directory, StringComparison.Ordinal)))
            .ToList();

    private static IEnumerable<string> TrackedRelativeFiles() =>
        HygieneRepository.TrackedFiles(RepoRoot)
            .Select(path => Path.GetRelativePath(RepoRoot, path).Replace('\\', '/'));

    private static string Absolute(string relative) =>
        Path.Combine(RepoRoot, relative.Replace('/', Path.DirectorySeparatorChar));
}
