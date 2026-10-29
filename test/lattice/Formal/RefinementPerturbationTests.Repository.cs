using System.Diagnostics;
using System.Text.Json;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// The repository half of the perturbation gate: every
/// <c>spec/&lt;area&gt;/perturbations/*.perturbation</c> is checked against the
/// code it edits, the note row it claims and the detectors it names, and the
/// runner's own parser is held to the same answer.
/// </summary>
public sealed partial class RefinementPerturbationTests
{
    private const string UnseededSection = "Unseeded rows";

    private const string RunnerPath = "tools/perturbations/run-perturbations.py";

    private static readonly TimeSpan RunnerTimeout = TimeSpan.FromMinutes(2);

    private static string RepoRoot => HygieneRepository.FindRepoRoot();

    /// <summary>
    /// Every tracked perturbation, as a forward-slash repository-relative path.
    /// Read from git's tracked set, so a scratch file left on disk is not
    /// gated and a committed one cannot be missed.
    /// </summary>
    public static IEnumerable<string> Files() =>
        HygieneRepository.EnumerateFiles(Path.Combine(RepoRoot, "spec"), "*" + RefinementPerturbation.Extension)
            .Select(Relative)
            .Order(StringComparer.Ordinal);

    private static string Relative(string path) =>
        Path.GetRelativePath(RepoRoot, path).Replace('\\', '/');

    private static RefinementPerturbation Load(string file) =>
        RefinementPerturbation.Parse(
            Path.GetFileNameWithoutExtension(file),
            File.ReadAllText(Path.Combine(RepoRoot, file)));

    private static SpecModule ModuleOf(RefinementPerturbation perturbation) =>
        SpecModuleCatalogue.Repository().SingleOrDefault(m => m.Name == perturbation.Module)
        ?? throw new InvalidOperationException(
            $"{perturbation.Name} names MODULE '{perturbation.Module}', which no spec module declares.");

    /// <summary>
    /// A row label as a perturbation writes it: the note's first cell with its
    /// backticks removed and its whitespace collapsed.
    /// </summary>
    private static string RowKey(string label) =>
        Regex.Replace(label.Replace("`", string.Empty, StringComparison.Ordinal), @"\s+", " ").Trim();

    /// <summary>
    /// The behaviour rows of a module's note - the action rows other than the
    /// non-behavioural ones, then the property rows - with the table each
    /// came from, which is the population the Detector column gates.
    /// </summary>
    private static List<(RefinementTable Table, RefinementRow Row)> BehaviourRows(SpecModule module)
    {
        var tables = RefinementNote.ReadTables(module);
        var nonBehavioural = RefinementNote.NonBehaviouralRowLabels(module);
        var rows = new List<(RefinementTable, RefinementRow)>();
        foreach (var section in new[] { RefinementNote.ActionSection, RefinementNote.PropertySection })
        {
            var table = tables[section];
            rows.AddRange(table.Rows
                .Where(r => !nonBehavioural.Contains(r.Label, StringComparer.Ordinal))
                .Select(r => (table, r)));
        }

        return rows;
    }

    /// <summary>
    /// Anti-vacuity: the gate reads a population, and the AtomicCommit seed
    /// set is in it. A discovery that silently found nothing would pass every
    /// per-file case below by having no cases.
    /// </summary>
    [Test]
    public void The_repository_holds_the_atomic_commit_seed_set()
    {
        var files = Files().ToList();

        Assert.Multiple(() =>
        {
            Assert.That(files, Is.Not.Empty, "no .perturbation file is tracked under spec/");
            Assert.That(
                files.Count(f => f.StartsWith("spec/atomic-commit/perturbations/", StringComparison.Ordinal)),
                Is.GreaterThanOrEqualTo(16),
                "the AtomicCommit seed set has one perturbation per seeded behaviour row");
        });
    }

    /// <summary>
    /// A perturbation lives at <c>spec/&lt;area&gt;/perturbations/&lt;Name&gt;.perturbation</c>,
    /// parses, and names a module whose directory is that area.
    /// </summary>
    [TestCaseSource(nameof(Files))]
    public void Every_perturbation_parses_and_belongs_to_its_area(string file)
    {
        var segments = file.Split('/');
        Assert.That(
            segments,
            Has.Length.EqualTo(4),
            $"{file} must sit at spec/<area>/{RefinementPerturbation.DirectoryName}/<Name>{RefinementPerturbation.Extension}");
        Assert.That(segments[2], Is.EqualTo(RefinementPerturbation.DirectoryName), file);

        var perturbation = Load(file);
        var module = ModuleOf(perturbation);

        Assert.That(
            Path.GetFileName(module.Directory.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)),
            Is.EqualTo(segments[1]),
            $"{file} names MODULE '{perturbation.Module}', which lives in {module.Describe(module.Directory)}, not spec/{segments[1]}/");
    }

    /// <summary>
    /// The anchor applies exactly once to the current <c>FILE</c>, which must
    /// be a tracked production file. This is the drift check: a refactor that
    /// moves the perturbed code fails here, on the pull request that moved it.
    /// </summary>
    [TestCaseSource(nameof(Files))]
    public void Every_perturbation_applies_exactly_once_to_its_file(string file)
    {
        var perturbation = Load(file);
        var target = Path.GetFullPath(Path.Combine(RepoRoot, perturbation.File));

        Assert.That(
            HygieneRepository.TrackedFiles(RepoRoot).Any(p => string.Equals(p, target, StringComparison.OrdinalIgnoreCase)),
            Is.True,
            $"{file} names FILE '{perturbation.File}', which is not a tracked file");
        Assert.That(
            () => perturbation.Apply(File.ReadAllText(target)),
            Throws.Nothing,
            $"{file} no longer applies to {perturbation.File}");
    }

    /// <summary>
    /// <c>ROW</c> names exactly one behaviour row of the module's note, and
    /// every <c>DETECTORS</c> entry is cited by that row's Detector cell and
    /// resolves to an NUnit test under <c>test/</c>. A perturbation therefore
    /// cannot claim a detector the note does not, and a renamed test fails here
    /// rather than at the weekly run.
    /// </summary>
    [TestCaseSource(nameof(Files))]
    public void Every_perturbation_names_a_behaviour_row_and_detectors_it_cites(string file)
    {
        var perturbation = Load(file);
        var module = ModuleOf(perturbation);

        var matches = BehaviourRows(module).Where(r => RowKey(r.Row.Label) == RowKey(perturbation.Row)).ToList();
        Assert.That(
            matches,
            Has.Count.EqualTo(1),
            $"{file} names ROW '{perturbation.Row}', which must match exactly one behaviour row of "
            + $"{module.Describe(module.RefinementNotePath)} (first cell, backticks removed). Behaviour rows: "
            + string.Join(", ", BehaviourRows(module).Select(r => RowKey(r.Row.Label))));

        var (table, row) = matches[0];
        var cited = RefinementCodeSymbols.ExtractDetectors([new RefinementTable(table.Section, table.Headers, [row])])
            .Select(d => d.Text)
            .ToHashSet(StringComparer.Ordinal);
        var resolver = RefinementDetectorResolver.ForRepository();

        Assert.Multiple(() =>
        {
            foreach (var detector in perturbation.Detectors)
            {
                Assert.That(
                    cited,
                    Does.Contain(detector),
                    $"{file}: '{detector}' is not cited by row '{perturbation.Row}' of {module.Describe(module.RefinementNotePath)}");

                var parts = detector.Split('.');
                Assert.That(
                    resolver.TestExists(parts[0], parts[1]),
                    Is.True,
                    $"{file}: {resolver.Explain(parts[0], parts[1])}");
            }
        });
    }

    /// <summary>
    /// Every behaviour row of a module that has any perturbation is either
    /// seeded or listed, with a reason, in the area README's
    /// <c>## Unseeded rows</c> table - never both and never neither. Coverage
    /// of a note therefore cannot shrink silently, and an unseeded row says
    /// why.
    /// </summary>
    [Test]
    public void Every_behaviour_row_of_a_perturbed_module_is_seeded_or_listed_unseeded()
    {
        var byArea = Files().GroupBy(f => f.Split('/')[1], StringComparer.Ordinal).ToList();
        Assert.That(byArea, Is.Not.Empty);

        Assert.Multiple(() =>
        {
            foreach (var area in byArea)
            {
                var readme = $"spec/{area.Key}/{RefinementPerturbation.DirectoryName}/{SpecModule.ReadmeFileName}";
                var unseeded = ReadUnseeded(readme);
                var seeded = area.Select(Load).ToList();

                foreach (var moduleName in seeded.Select(p => p.Module).Concat(unseeded.Select(u => u.Module)).Distinct(StringComparer.Ordinal))
                {
                    var module = SpecModuleCatalogue.Repository().SingleOrDefault(m => m.Name == moduleName);
                    Assert.That(module, Is.Not.Null, $"{readme} or a perturbation names unknown module '{moduleName}'");
                    if (module is null)
                    {
                        continue;
                    }

                    if (!seeded.Any(p => p.Module == moduleName))
                    {
                        Assert.Fail($"{readme} lists unseeded rows of {moduleName}, which has no perturbation in this area");
                        continue;
                    }

                    foreach (var (_, row) in BehaviourRows(module))
                    {
                        var key = RowKey(row.Label);
                        var isSeeded = seeded.Any(p => p.Module == moduleName && RowKey(p.Row) == key);
                        var listings = unseeded.Count(u => u.Module == moduleName && RowKey(u.Row) == key);
                        Assert.That(
                            (isSeeded ? 1 : 0) + listings,
                            Is.EqualTo(1),
                            $"{moduleName} row '{key}' must be seeded by a perturbation or listed once in {readme}, not "
                            + (isSeeded ? "both" : listings == 0 ? "neither" : "listed twice"));
                    }

                    foreach (var listing in unseeded.Where(u => u.Module == moduleName))
                    {
                        Assert.That(
                            BehaviourRows(module).Any(r => RowKey(r.Row.Label) == RowKey(listing.Row)),
                            Is.True,
                            $"{readme} lists '{listing.Row}', which is not a behaviour row of {moduleName}");
                    }
                }
            }
        });
    }

    /// <summary>
    /// A perturbations directory holds perturbations and its README and
    /// nothing else, so a misspelt extension cannot hide a perturbation from
    /// the gate.
    /// </summary>
    [Test]
    public void A_perturbations_directory_holds_only_perturbations_and_its_readme()
    {
        var marker = $"/{RefinementPerturbation.DirectoryName}/";
        var strays = HygieneRepository.EnumerateFiles(Path.Combine(RepoRoot, "spec"), "*")
            .Select(Relative)
            .Where(f => f.Contains(marker, StringComparison.Ordinal))
            .Where(f => !f.EndsWith(RefinementPerturbation.Extension, StringComparison.Ordinal)
                && !f.EndsWith(marker + SpecModule.ReadmeFileName, StringComparison.Ordinal))
            .ToList();

        Assert.That(strays, Is.Empty);
    }

    /// <summary>
    /// The runner carries its own parser of the same grammar. Its
    /// <c>--check --json</c> reading of every perturbation must equal this
    /// gate's, field for field and edit for edit, and it must report every one
    /// applicable; otherwise the gate and the experiment could be checking two
    /// different files.
    /// </summary>
    [Test]
    public void The_runner_reads_every_perturbation_as_the_gate_does()
    {
        var (exitCode, stdout, stderr) = RunRunner("--all", "--check", "--json");
        Assert.That(exitCode, Is.EqualTo(0), $"runner --check failed:{Environment.NewLine}{stdout}{stderr}");

        using var document = JsonDocument.Parse(stdout);
        var root = document.RootElement;
        Assert.That(root.GetProperty("schema").GetInt32(), Is.EqualTo(1));

        var byPath = root.GetProperty("perturbations").EnumerateArray()
            .ToDictionary(e => e.GetProperty("path").GetString()!, StringComparer.Ordinal);
        Assert.That(byPath.Keys.Order(StringComparer.Ordinal), Is.EqualTo(Files()), "the runner and the gate discover different files");

        Assert.Multiple(() =>
        {
            foreach (var file in Files())
            {
                var expected = Load(file);
                var actual = byPath[file];
                Assert.That(actual.GetProperty("verdict").GetString(), Is.EqualTo("valid"), file);
                Assert.That(actual.GetProperty("name").GetString(), Is.EqualTo(expected.Name), file);
                Assert.That(actual.GetProperty("module").GetString(), Is.EqualTo(expected.Module), file);
                Assert.That(actual.GetProperty("row").GetString(), Is.EqualTo(expected.Row), file);
                Assert.That(actual.GetProperty("file").GetString(), Is.EqualTo(expected.File), file);
                Assert.That(actual.GetProperty("summary").GetString(), Is.EqualTo(expected.Summary), file);
                Assert.That(
                    actual.GetProperty("detectors").EnumerateArray().Select(d => d.GetString()),
                    Is.EqualTo(expected.Detectors),
                    file);
                Assert.That(
                    actual.GetProperty("edits").EnumerateArray()
                        .Select(e => new SpecEdit(e.GetProperty("find").GetString()!, e.GetProperty("replace").GetString()!)),
                    Is.EqualTo(expected.Edits),
                    file);
            }
        });
    }

    /// <summary>
    /// The runner's built-in self-test - its parser's refusals, the TRX
    /// reading that keeps a prefix-sharing sibling test from being credited,
    /// and detector resolution - passes without dotnet or git.
    /// </summary>
    [Test]
    public void The_runner_self_test_passes()
    {
        var (exitCode, stdout, stderr) = RunRunner("--self-test");

        Assert.That(exitCode, Is.EqualTo(0), stdout + stderr);
        Assert.That(stdout, Does.Contain("run-perturbations self-test: passed"));
    }

    private static List<(string Module, string Row, string Reason)> ReadUnseeded(string readme)
    {
        var path = Path.Combine(RepoRoot, readme);
        Assert.That(File.Exists(path), Is.True, $"{readme} is missing; it holds the area's '## {UnseededSection}' table");

        var listings = new List<(string, string, string)>();
        var inSection = false;
        var sawTable = false;
        foreach (var raw in File.ReadAllText(path).ReplaceLineEndings("\n").Split('\n'))
        {
            var line = raw.Trim();
            if (line.StartsWith("## ", StringComparison.Ordinal))
            {
                inSection = line[3..].Trim() == UnseededSection;
                continue;
            }

            if (!inSection || !line.StartsWith('|'))
            {
                continue;
            }

            var cells = line.Trim('|').Split('|').Select(c => c.Trim()).ToArray();
            if (!sawTable)
            {
                Assert.That(cells, Is.EqualTo(new[] { "Module", "Row", "Reason" }), $"{readme} '## {UnseededSection}' header");
                sawTable = true;
                continue;
            }

            if (cells.All(c => Regex.IsMatch(c, "^:?-+:?$")))
            {
                continue;
            }

            Assert.That(cells, Has.Length.EqualTo(3), $"{readme}: '{line}'");
            Assert.That(cells[2], Is.Not.Empty, $"{readme}: an unseeded row needs a reason: '{line}'");
            listings.Add((cells[0], cells[1], cells[2]));
        }

        Assert.That(sawTable, Is.True, $"{readme} has no '## {UnseededSection}' table");
        return listings;
    }

    private static (int ExitCode, string StandardOutput, string StandardError) RunRunner(params string[] arguments)
    {
        var interpreter = FindPython();
        if (interpreter is null)
        {
            if (string.Equals(Environment.GetEnvironmentVariable("GITHUB_ACTIONS"), "true", StringComparison.OrdinalIgnoreCase))
            {
                Assert.Fail("no Python interpreter was found on the CI runner, so the perturbation runner cannot be checked");
            }

            Assert.Ignore("no Python interpreter is available on this host");
        }

        var start = new ProcessStartInfo(interpreter!)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            WorkingDirectory = RepoRoot,
        };
        start.ArgumentList.Add(Path.Combine(RepoRoot, RunnerPath));
        foreach (var argument in arguments)
        {
            start.ArgumentList.Add(argument);
        }

        return Run(start);
    }

    private static string? FindPython()
    {
        // `python3` on Windows is often a Store alias stub, so each candidate is probed.
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
                var (exitCode, stdout, stderr) = Run(probe);
                if (exitCode == 0 && (stdout + stderr).Contains("Python 3", StringComparison.Ordinal))
                {
                    return candidate;
                }
            }
            catch (System.ComponentModel.Win32Exception)
            {
                // Not on PATH.
            }
        }

        return null;
    }

    private static (int ExitCode, string StandardOutput, string StandardError) Run(ProcessStartInfo start)
    {
        using var process = Process.Start(start)!;
        var stdout = process.StandardOutput.ReadToEndAsync();
        var stderr = process.StandardError.ReadToEndAsync();
        if (!process.WaitForExit((int)RunnerTimeout.TotalMilliseconds))
        {
            process.Kill(entireProcessTree: true);
            var reaped = process.WaitForExit(10_000);
            Assert.Fail(
                $"'{start.FileName} {string.Join(' ', start.ArgumentList)}' did not exit within {RunnerTimeout}; "
                + (reaped ? "its process tree was killed." : "killing its process tree did not complete within 10 s."));
        }

        return (process.ExitCode, stdout.GetAwaiter().GetResult(), stderr.GetAwaiter().GetResult());
    }
}
