using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Text.Json;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Asserts that the settings table in
/// <c>docs/lattice.api.mcp.repocontext/local-deployment-runbook.md</c> enumerates
/// exactly the settings the tracked compose files actually resolve to, and that the
/// opt-in CPU pinning added by #2623 stays opt-in.
/// </summary>
/// <remarks>
/// <para>
/// <b>WHAT A GREEN RUN OF THIS FIXTURE DOES NOT ESTABLISH.</b> It establishes that
/// two tracked files agree with each other, and nothing else. It does not establish
/// that any container is running, that a running container was composed from these
/// files, that it is executing an image built from this checkout, or that any of
/// these limits are in force anywhere. <c>docker compose up</c> reads the compose
/// files in its own working directory, whichever checkout that is, and nothing in
/// its output names a branch or a commit - so a container can run indefinitely under
/// a different checkout's configuration while every file in this repository agrees
/// with every other. Epic #2368's gate runs 1 and 2 both failed exactly there. The
/// check that separates "the source does not carry the fix" from "the source carries
/// it and this container never received it" is the deployment-provenance assertion
/// added by #2590 and #2592,
/// <c>samples/RepoContextContainer/scripts/Assert-ContainerProvenance.ps1</c>, which
/// reads the answer out of the running container. This fixture is not a substitute
/// for it and cannot be made into one.
/// </para>
/// <para>
/// <b>Why it evaluates the resolved document.</b> The parity assertion runs
/// <c>docker compose config</c> over the two tracked files rather than reading them
/// as YAML, because compose merge and interpolation decide what a setting resolves
/// to. A raw-file comparison can be perfectly green about a value the merge
/// discards, which is the same class of self-consistent falsehood described above,
/// one level down. <c>Assert-RigComposeIsolation</c> in the cold-start rig is the
/// prior art for asserting over a resolved document rather than over the inputs.
/// </para>
/// <para>
/// <b>Why it lives in the core project under this name.</b> The files it guards are
/// a sample compose overlay and a package doc, and both are invisible to the test
/// matrix: <c>samples/**</c>, <c>docs/**</c> and <c>**/*.md</c> are all excluded
/// from the <c>nonSample</c> filter in <c>ci.yml</c>, so a pull request that only
/// adds a setting to the overlay - precisely the drift this guards - skips the
/// matrix entirely. The <c>hygiene</c> lane exists for that hole (#2443); it selects
/// by class name with <c>FullyQualifiedName~HygieneTests</c>, and it resolves both
/// <c>samples/**</c> and <c>docs/&lt;pkg&gt;/**</c> changes onto the core project.
/// Naming and placing this fixture anywhere else would leave it never running on the
/// changes it exists to catch, which is a worse outcome than not having it. It
/// derives from no scoped gate base, so it is outside the per-project replication
/// contract <c>CiContentHygieneLaneTests</c> enforces.
/// </para>
/// <para>
/// The Docker-free half runs everywhere and keeps the fixture from going vacuous
/// when the toolchain is absent. The parity half needs Docker: it is skipped
/// visibly on a developer machine without it, and fails under CI, where its absence
/// is a broken runner rather than a local convenience. It is never
/// <c>Assert.Inconclusive</c>, which NUnit counts as neither passed, failed, nor
/// skipped, and which therefore reads as a green run that checked nothing.
/// </para>
/// </remarks>
[TestFixture]
public sealed class LocalDeploymentRunbookHygieneTests
{
    private const string RunbookPath = "docs/lattice.api.mcp.repocontext/local-deployment-runbook.md";
    private const string ComposeDirectory = "samples/RepoContextContainer";
    private const string BaseComposeFile = "docker-compose.yml";
    private const string TuningComposeFile = "docker-compose.tuning.yml";
    private const string EnvExamplePath = "samples/RepoContextContainer/.env.example";

    private const string TableBegin = "<!-- compose-settings:begin -->";
    private const string TableEnd = "<!-- compose-settings:end -->";

    /// <summary>
    /// The settings the guard compares. Topology (networks, build, ports, volumes,
    /// depends_on) is deliberately out of scope: it is documented by
    /// <c>container.md</c> and the sample README, and the runbook links to both
    /// rather than restating them.
    /// </summary>
    private static readonly string[] ScalarSettings = ["image", "cpus", "mem_limit"];

    /// <summary>
    /// The variables that carry the opt-in CPU pinning added by #2623, and the
    /// service each one pins.
    /// </summary>
    private static readonly (string Service, string Variable)[] CpusetVariables =
    [
        ("repocontext", "REPOCONTEXT_CPUSET"),
        ("embedder", "EMBEDDER_CPUSET"),
    ];

    /// <summary>
    /// A <c>cpuset:</c> declaration in a compose file, capturing whatever it is set to.
    /// </summary>
    private static readonly Regex CpusetDeclaration = new(
        @"^\s*cpuset\s*:\s*""?(?<value>[^""#]*?)""?\s*$",
        RegexOptions.Compiled);

    /// <summary>
    /// A variable reference supplying an EMPTY default, which is the only form of
    /// <c>cpuset</c> this deployment permits in a tracked compose file.
    /// </summary>
    private static readonly Regex EmptyDefaultedVariable = new(
        @"^\$\{[A-Z_][A-Z0-9_]*:-\}$",
        RegexOptions.Compiled);

    /// <summary>
    /// The marker a value-redacted row must carry in the Value column, in place of the
    /// resolved value. A single backticked token, because that is what a table row's
    /// value cell is; the explanation belongs in the row's rationale column.
    /// </summary>
    private const string RedactionMarker = "redacted";

    /// <summary>
    /// Settings whose value is deliberately not reproduced in the runbook. This list is
    /// exhaustive and is meant to stay at one entry: every addition widens a hole.
    /// <para>
    /// The sole entry is a Blob connection string. It is the well-known public Azurite
    /// emulator account and is already tracked verbatim in <c>docker-compose.yml</c>, so
    /// nothing is concealed by omitting it. The reason to omit it is that copying a
    /// credential-shaped 222-character string into a second tracked file trains readers
    /// and scanners to treat such strings in documentation as normal. Presence parity
    /// still covers the key in both directions.
    /// </para>
    /// </summary>
    private static readonly HashSet<string> RedactedValues = new(StringComparer.Ordinal)
    {
        "repocontext.LATTICE_BACKUP_BLOB_CONNECTION_STRING",
    };

    private static readonly Regex TableRow = new(
        @"^\|\s*`(?<service>[^`]+)`\s*\|\s*`(?<setting>[^`]+)`\s*\|\s*`(?<value>[^`]*)`\s*\|(?<why>[^|]*)\|\s*$",
        RegexOptions.Compiled);

    /// <summary>A <c>KEY: "value"</c> or <c>KEY: value</c> line inside a compose file.</summary>
    private static readonly Regex YamlScalar = new(
        @"^\s{6,}(?<key>[A-Za-z_][A-Za-z0-9_.]*)\s*:\s*""?(?<value>[^""#]*?)""?\s*$",
        RegexOptions.Compiled);

    // ---------------------------------------------------------------------
    // Toolchain-free assertions. These run everywhere, including on a machine
    // with no Docker, so the fixture always asserts something.
    // ---------------------------------------------------------------------

    [Test]
    public void The_runbook_declares_a_well_formed_settings_table()
    {
        var rows = ParseTable();

        Assert.Multiple(() =>
        {
            Assert.That(
                rows,
                Has.Count.GreaterThanOrEqualTo(20),
                "expected the runbook's settings table to enumerate the whole resolved "
                + "document. Finding far fewer rows means the table was gutted and this "
                + "fixture is checking almost nothing.");

            Assert.That(
                rows.Select(row => (row.Service, row.Setting)),
                Is.Unique,
                "a setting is listed twice, so one of the two rows is unreachable and a "
                + "reader cannot tell which value is in force.");

            Assert.That(
                rows.Where(row => string.IsNullOrWhiteSpace(row.Why)).Select(row => row.Key),
                Is.Empty,
                "every row must say why the value is what it is. A table of values with no "
                + "rationale is the untracked override file again, in markdown.");
        });
    }

    /// <summary>
    /// Presence floor for the tuning overlay, evaluated without Docker. This is a
    /// textual scan, so it is strictly weaker than the resolved-document parity
    /// below - it exists so that adding a setting to the overlay and not the table
    /// still fails on a machine or runner with no Docker, rather than skipping.
    /// </summary>
    [Test]
    public void Every_setting_the_tuning_overlay_declares_appears_in_the_table()
    {
        var documented = ParseTable()
            .Select(row => row.Setting)
            .ToHashSet(StringComparer.Ordinal);

        var declared = DeclaredInTuningOverlay();

        Assert.That(
            declared,
            Has.Count.GreaterThanOrEqualTo(8),
            "expected to recognise the tuning overlay's settings. Finding far fewer means "
            + "the scan no longer parses that file and this assertion is vacuous.");

        Assert.That(
            declared.Where(setting => !documented.Contains(setting)).Order(StringComparer.Ordinal),
            Is.Empty,
            $"{TuningComposeFile} declares these settings and {RunbookPath} does not document "
            + "them. Undocumented tuning is what issue #2609 exists to close: a value with no "
            + "recorded rationale cannot be reviewed, reproduced, or safely changed.");
    }

    // ---------------------------------------------------------------------
    // The parity assertion, over the RESOLVED document.
    // ---------------------------------------------------------------------

    [Test]
    public void The_table_matches_the_resolved_compose_document()
    {
        var resolved = ResolveComposeDocument();

        var documented = ParseTable().ToDictionary(row => row.Key, row => row.Value, StringComparer.Ordinal);

        var undocumented = resolved.Keys
            .Where(key => !documented.ContainsKey(key))
            .Order(StringComparer.Ordinal)
            .ToArray();

        var phantom = documented.Keys
            .Where(key => !resolved.ContainsKey(key))
            .Order(StringComparer.Ordinal)
            .ToArray();

        var mismatched = documented
            .Where(entry => resolved.TryGetValue(entry.Key, out var actual)
                && !RedactedValues.Contains(entry.Key)
                && !Normalise(entry.Key, entry.Value).Equals(Normalise(entry.Key, actual), StringComparison.Ordinal))
            .Select(entry => $"{entry.Key}: runbook says '{entry.Value}', resolved document says "
                + $"'{resolved[entry.Key]}'")
            .Order(StringComparer.Ordinal)
            .ToArray();

        // The redaction seam is a deliberate, enumerated hole in value parity. Presence
        // parity above still applies to these keys in both directions; only the value
        // comparison is replaced, and it is replaced by two assertions rather than
        // dropped, so the row cannot quietly become decorative.
        var badRedaction = RedactedValues
            .Where(resolved.ContainsKey)
            .Where(key => !documented.TryGetValue(key, out var cell)
                || !cell.Equals(RedactionMarker, StringComparison.Ordinal)
                || string.IsNullOrWhiteSpace(resolved[key]))
            .Order(StringComparer.Ordinal)
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(
                undocumented,
                Is.Empty,
                $"the resolved compose document declares these settings and {RunbookPath} does "
                + "not list them.");

            Assert.That(
                phantom,
                Is.Empty,
                $"{RunbookPath} lists these settings and the resolved compose document does not "
                + "declare them. A row naming a setting the merge does not actually produce is "
                + "worse than a missing row: it reads as documentation of something in force.");

            Assert.That(
                mismatched,
                Is.Empty,
                "these documented values disagree with the resolved compose document.");

            Assert.That(
                badRedaction,
                Is.Empty,
                $"these settings are declared value-redacted, so {RunbookPath} must document "
                + $"them with the exact cell `{RedactionMarker}` and the resolved document must "
                + "still supply a non-empty value. A redacted row that stops matching the marker, "
                + "or whose real value has gone empty, is a hole in the guard rather than a "
                + "deliberate exception to it.");
        });
    }

    /// <summary>
    /// The workspace root the deployment points at is configured by an untracked
    /// <c>.env</c> file that Docker Compose auto-loads from the directory it is invoked
    /// from. It appears in neither compose file, so the parity test above cannot see it,
    /// and the resolved document reports only the unexpanded default. This test therefore
    /// guards the one thing that is checkable from the repository: that the runbook still
    /// explains the setting, the trap, and how to verify it, and that a tracked example
    /// still carries the value.
    /// <para>
    /// IT ESTABLISHES NOTHING ABOUT ANY MOUNT, ANY INDEXED ROOT, OR ANY RUNNING
    /// CONTAINER. It cannot: the correct value is an absolute host path that differs on
    /// every machine and does not exist in CI. A green run means the words are present.
    /// It does not mean a deployment is pointed anywhere in particular, and it does not
    /// prevent the omission from recurring, because nothing loads
    /// <c>.env.example</c> at <c>up</c> time. Observability of the wrong state is issue
    /// #2617; the running check is the indexed-root assertion described in the runbook.
    /// </para>
    /// </summary>
    [Test]
    public void The_runbook_documents_the_workspace_root_and_its_worktree_trap()
    {
        var root = HygieneRepository.FindRepoRoot();
        var runbook = File.ReadAllText(Path.Combine(
            root,
            RunbookPath.Replace('/', Path.DirectorySeparatorChar)));

        // Each entry is a fact the runbook must still carry, paired with why losing it
        // would matter. Substrings, not prose matching: this guards presence of the
        // mechanism's name, not the wording around it.
        var required = new (string Needle, string Why)[]
        {
            ("REPO_PATH", "the setting's own name"),
            (".env", "the mechanism that actually carries it, and the half that was lost"),
            (EnvExamplePath, "the tracked example a reader is told to copy"),
            ("${REPO_PATH:-../../..}", "the default whose meaning changes by invocation directory"),
            ("worktree", "the case in which that default is silently wrong"),
            ("docker inspect", "the mount verification command"),
            ("repocontext_changed", "the stronger indexed-root verification command"),
            ("#2617", "where prevention actually lives"),
        };

        var missing = required
            .Where(r => !runbook.Contains(r.Needle, StringComparison.Ordinal))
            .Select(r => $"{r.Needle} ({r.Why})")
            .ToList();

        var examplePath = Path.Combine(root, EnvExamplePath.Replace('/', Path.DirectorySeparatorChar));

        Assert.Multiple(() =>
        {
            Assert.That(
                missing,
                Is.Empty,
                $"{RunbookPath} no longer documents the workspace root. This deployment has "
                + "already been indexed against the wrong tree once, silently, for the whole "
                + "life of the container, because this setting lived only in an untracked file.");

            Assert.That(
                File.Exists(examplePath),
                Is.True,
                $"expected {EnvExamplePath} to exist. It is the only copy of REPO_PATH in "
                + "version control; the live .env is gitignored by design.");

            Assert.That(
                File.Exists(examplePath) ? File.ReadAllText(examplePath) : string.Empty,
                Does.Contain("REPO_PATH="),
                $"expected {EnvExamplePath} to assign REPO_PATH. An example that documents the "
                + "variable without assigning it cannot be copied to a working .env.");
        });
    }

    // ---------------------------------------------------------------------
    // The opt-in guarantee for CPU pinning (#2623).
    // ---------------------------------------------------------------------

    /// <summary>
    /// CPU pinning is opt-in, and this is the half of that guarantee which needs no
    /// Docker: every <c>cpuset</c> a TRACKED compose file declares must be driven by a
    /// variable with an EMPTY default. A literal range hard-coded here would make
    /// pinning a default that arrives with a <c>git pull</c> rather than a choice an
    /// operator made, and it would perturb precisely the measurement window the knob is
    /// kept unset for: epic #2368 voided a whole gate run to a service configuration
    /// that changed inside one.
    /// <para>
    /// It also fails if the declarations go MISSING, so deleting the knob while the
    /// runbook still documents it is caught rather than passing vacuously.
    /// </para>
    /// <para>
    /// A machine-local <c>docker-compose.override.yml</c> is deliberately excluded. It
    /// is gitignored, it is a legitimate personal escape hatch, and a hard-coded cpuset
    /// in one is the operator's business - the runbook asks only that it be recorded
    /// under local-only deltas. Scanning it would make this fixture's result depend on
    /// untracked state, which is the defect #2609 closed rather than one to reopen.
    /// </para>
    /// </summary>
    [Test]
    public void Every_tracked_cpuset_declaration_is_variable_driven_with_an_empty_default()
    {
        var composeFiles = TrackedComposeFiles();

        var declarations = composeFiles
            .SelectMany(file => File.ReadAllLines(file)
                .Where(line => !line.TrimStart().StartsWith('#'))
                .Select(line => CpusetDeclaration.Match(line))
                .Where(match => match.Success)
                .Select(match => (File: Path.GetFileName(file), Value: match.Groups["value"].Value.Trim())))
            .ToList();

        var literal = declarations
            .Where(d => !EmptyDefaultedVariable.IsMatch(d.Value))
            .Select(d => $"{d.File}: cpuset: \"{d.Value}\"")
            .Order(StringComparer.Ordinal)
            .ToArray();

        var declaredVariables = declarations
            .Select(d => d.Value)
            .ToHashSet(StringComparer.Ordinal);

        var missing = CpusetVariables
            .Where(v => !declaredVariables.Contains($"${{{v.Variable}:-}}"))
            .Select(v => $"{v.Variable} (pins {v.Service})")
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(
                composeFiles,
                Has.Count.GreaterThanOrEqualTo(3),
                "expected to find the tracked compose files to scan. Finding almost none "
                + "means this assertion is vacuous rather than satisfied.");

            Assert.That(
                literal,
                Is.Empty,
                "a tracked compose file pins a literal cpuset. Pinning must stay opt-in and "
                + "unset by default: a literal here changes what every reader of that "
                + "directory deploys from a plain `docker compose up`, and a configuration "
                + "change landing mid-measurement is what voided gate run 3 of epic #2368. "
                + "Drive it from a variable with an empty default instead, as "
                + "`${NAME:-}`, and put the value in an untracked .env.");

            Assert.That(
                missing,
                Is.Empty,
                "the opt-in CPU pinning variables are no longer declared by any tracked "
                + "compose file, while the runbook still documents them. Either the knob "
                + "was removed and its documentation left behind, or it was renamed and "
                + "this guard was not.");
        });
    }

    /// <summary>
    /// The other half of the opt-in guarantee, and the one that actually matters:
    /// with the variables unset the RESOLVED document must declare no <c>cpuset</c>
    /// at all.
    /// <para>
    /// This is not the same claim as the textual one above, and neither implies the
    /// other. Compose could perfectly well render an empty <c>cpuset: ""</c> key from
    /// an unset variable - that it instead omits the key entirely is a property of the
    /// merge, observed here rather than assumed, and it is what makes "byte-identical
    /// to before the knob existed" true rather than merely nearly true.
    /// </para>
    /// <para>
    /// Resolved with an EMPTY <c>--env-file</c> and with the two variables stripped from
    /// the child process environment, so that a developer who has legitimately enabled
    /// pinning on their own machine does not see this fixture fail. The question asked
    /// is what the tracked files resolve to for someone who has set nothing.
    /// </para>
    /// </summary>
    [Test]
    public void The_resolved_document_declares_no_cpuset_when_the_variables_are_unset()
    {
        var workingDirectory = Path.Combine(
            HygieneRepository.FindRepoRoot(),
            ComposeDirectory.Replace('/', Path.DirectorySeparatorChar));

        var emptyEnvFile = Path.Combine(Path.GetTempPath(), $"lattice-cpuset-{Guid.NewGuid():N}.env");
        File.WriteAllText(emptyEnvFile, string.Empty);

        try
        {
            var json = RunDockerCompose(workingDirectory, emptyEnvFile);

            using var document = JsonDocument.Parse(json);

            if (!document.RootElement.TryGetProperty("services", out var services))
            {
                Assert.Fail("`docker compose config` produced no `services` element.");
            }

            var pinned = services.EnumerateObject()
                .Where(service => service.Value.TryGetProperty("cpuset", out var cpuset)
                    && cpuset.ValueKind is not JsonValueKind.Null and not JsonValueKind.Undefined
                    && !string.IsNullOrEmpty(Stringify(cpuset)))
                .Select(service => $"{service.Name}: {Stringify(service.Value.GetProperty("cpuset"))}")
                .Order(StringComparer.Ordinal)
                .ToArray();

            Assert.Multiple(() =>
            {
                Assert.That(
                    services.EnumerateObject().Count(),
                    Is.GreaterThanOrEqualTo(2),
                    "the resolved document yielded almost no services, so the assertion "
                    + "below would be vacuous.");

                Assert.That(
                    pinned,
                    Is.Empty,
                    "the resolved compose document pins CPUs with no variable set. The "
                    + "default deployment must be byte-identical to one resolved before "
                    + "the pinning knob existed (#2623): enabling it has to be an act on "
                    + "the record, because a service configuration change inside a "
                    + "measurement window voids the measurement.");
            });
        }
        finally
        {
            try { File.Delete(emptyEnvFile); } catch (IOException) { /* best effort */ }
        }
    }

    /// <summary>
    /// The runbook must keep explaining the opt-in pinning knob, and in particular must
    /// keep carrying the two retractions attached to it. The throttling ratio was once
    /// quoted in this epic as evidence that a thread-pool sizing fix had taken effect;
    /// it measures CPU scatter instead, and has a high idle floor, so a residual is a
    /// FLOOR and not a REMAINDER. Losing that paragraph would leave the figures in the
    /// pool-sizing section reading as corroboration of something they cannot support.
    /// <para>
    /// Substrings, not prose matching: this guards presence of the mechanism's name and
    /// of each load-bearing caveat, not the wording around them.
    /// </para>
    /// </summary>
    [Test]
    public void The_runbook_documents_the_opt_in_pinning_knob_and_its_caveats()
    {
        var root = HygieneRepository.FindRepoRoot();
        var runbook = File.ReadAllText(Path.Combine(
            root,
            RunbookPath.Replace('/', Path.DirectorySeparatorChar)));

        var required = new (string Needle, string Why)[]
        {
            ("REPOCONTEXT_CPUSET", "the variable that pins the host service"),
            ("EMBEDDER_CPUSET", "the variable that pins the embedder"),
            ("cpuset", "the mechanism's own name"),
            ("reservation", "why quota is exhausted below the entitlement"),
            ("must not overlap", "the hazard of trading throttling for contention"),
            ("floor, not a remainder", "the retraction that stops a residual ratio being "
                + "read as remaining oversubscription"),
            ("T0", "the window in which the knob must not be switched on"),
        };

        var missing = required
            .Where(r => !runbook.Contains(r.Needle, StringComparison.Ordinal))
            .Select(r => $"{r.Needle} ({r.Why})")
            .ToList();

        Assert.That(
            missing,
            Is.Empty,
            $"{RunbookPath} no longer documents the opt-in CPU pinning knob or one of its "
            + "caveats. The knob is only safe because the conditions on using it are "
            + "written down: an operator who enables it mid-measurement voids the "
            + "measurement, and one who reads a residual throttle ratio as oversubscription "
            + "repeats a reading this epic has already withdrawn.");
    }

    // ---------------------------------------------------------------------
    // Helpers.
    // ---------------------------------------------------------------------

    /// <summary>
    /// The tracked compose files in the sample directory. Excludes
    /// <c>docker-compose.override.yml</c>, which is gitignored machine-local state.
    /// </summary>
    private static List<string> TrackedComposeFiles()
    {
        var directory = Path.Combine(
            HygieneRepository.FindRepoRoot(),
            ComposeDirectory.Replace('/', Path.DirectorySeparatorChar));

        Assert.That(
            Directory.Exists(directory),
            Is.True,
            $"expected the sample compose directory at {ComposeDirectory}.");

        return Directory.EnumerateFiles(directory, "docker-compose*.yml")
            .Where(file => !Path.GetFileName(file)
                .Equals("docker-compose.override.yml", StringComparison.OrdinalIgnoreCase))
            .Order(StringComparer.Ordinal)
            .ToList();
    }

    private sealed record Row(string Service, string Setting, string Value, string Why)
    {
        public string Key => $"{Service}.{Setting}";
    }

    private static List<Row> ParseTable()
    {
        var path = Path.Combine(
            HygieneRepository.FindRepoRoot(),
            RunbookPath.Replace('/', Path.DirectorySeparatorChar));

        Assert.That(File.Exists(path), Is.True, $"expected {RunbookPath} to exist.");

        var text = File.ReadAllText(path);
        var begin = text.IndexOf(TableBegin, StringComparison.Ordinal);
        var end = text.IndexOf(TableEnd, StringComparison.Ordinal);

        Assert.Multiple(() =>
        {
            Assert.That(
                begin,
                Is.GreaterThanOrEqualTo(0),
                $"expected {RunbookPath} to delimit its settings table with `{TableBegin}`. The "
                + "markers are how this fixture finds the table without guessing which of the "
                + "document's tables is the guarded one.");
            Assert.That(end, Is.GreaterThan(begin), $"expected `{TableEnd}` after `{TableBegin}`.");
        });

        var region = text[(begin + TableBegin.Length)..end];
        var rows = new List<Row>();

        foreach (var line in region.Split('\n'))
        {
            var match = TableRow.Match(line.TrimEnd('\r'));
            if (!match.Success) continue;

            rows.Add(new Row(
                match.Groups["service"].Value.Trim(),
                match.Groups["setting"].Value.Trim(),
                match.Groups["value"].Value.Trim(),
                match.Groups["why"].Value.Trim()));
        }

        return rows;
    }

    /// <summary>
    /// The settings the tuning overlay declares, read textually. Commented-out lines
    /// are excluded by construction: a diagnostic that ships commented out never
    /// enters the resolved document, so documenting it as a setting in force would
    /// be false.
    /// </summary>
    private static List<string> DeclaredInTuningOverlay()
    {
        var path = Path.Combine(
            HygieneRepository.FindRepoRoot(),
            ComposeDirectory.Replace('/', Path.DirectorySeparatorChar),
            TuningComposeFile);

        Assert.That(
            File.Exists(path),
            Is.True,
            $"expected the tracked tuning overlay at {ComposeDirectory}/{TuningComposeFile}. It "
            + "is what stops the deployment's configuration living only in an untracked, "
            + "gitignored file (#2609).");

        var declared = new List<string>();

        foreach (var raw in File.ReadAllLines(path))
        {
            var line = raw.TrimEnd();
            if (line.TrimStart().StartsWith('#')) continue;

            var match = YamlScalar.Match(line);
            if (!match.Success) continue;

            var key = match.Groups["key"].Value;
            if (key is "services" or "environment") continue;

            declared.Add(key);
        }

        foreach (var scalar in ScalarSettings)
        {
            if (File.ReadAllLines(path).Any(line =>
                    line.TrimStart().StartsWith(scalar + ":", StringComparison.Ordinal)))
            {
                declared.Add(scalar);
            }
        }

        return declared.Distinct(StringComparer.Ordinal).ToList();
    }

    /// <summary>
    /// Runs <c>docker compose config</c> over the two tracked files and flattens the
    /// result to <c>service.setting</c> keys.
    /// </summary>
    private static Dictionary<string, string> ResolveComposeDocument()
    {
        var workingDirectory = Path.Combine(
            HygieneRepository.FindRepoRoot(),
            ComposeDirectory.Replace('/', Path.DirectorySeparatorChar));

        var json = RunDockerCompose(workingDirectory);
        var settings = new Dictionary<string, string>(StringComparer.Ordinal);

        using var document = JsonDocument.Parse(json);

        if (!document.RootElement.TryGetProperty("services", out var services))
        {
            Assert.Fail("`docker compose config` produced no `services` element.");
        }

        foreach (var service in services.EnumerateObject())
        {
            if (service.Value.TryGetProperty("environment", out var environment)
                && environment.ValueKind == JsonValueKind.Object)
            {
                foreach (var variable in environment.EnumerateObject())
                {
                    settings[$"{service.Name}.{variable.Name}"] = Stringify(variable.Value);
                }
            }

            foreach (var scalar in ScalarSettings)
            {
                if (service.Value.TryGetProperty(scalar, out var value)
                    && value.ValueKind is not JsonValueKind.Null and not JsonValueKind.Undefined)
                {
                    settings[$"{service.Name}.{scalar}"] = Stringify(value);
                }
            }

            // Compose accepts limits under `deploy.resources.limits` as well as at
            // service level. Both reach the same place, so both are guarded; a
            // limit moved between the two forms must not silently escape the table.
            if (service.Value.TryGetProperty("deploy", out var deploy)
                && deploy.TryGetProperty("resources", out var resources)
                && resources.TryGetProperty("limits", out var limits)
                && limits.ValueKind == JsonValueKind.Object)
            {
                foreach (var limit in limits.EnumerateObject())
                {
                    settings[$"{service.Name}.{limit.Name}"] = Stringify(limit.Value);
                }
            }
        }

        Assert.That(
            settings,
            Has.Count.GreaterThanOrEqualTo(20),
            "the resolved compose document yielded almost no settings, so the parity "
            + "comparison below would be vacuous. Expected the base file and the tuning "
            + "overlay to merge into the full set.");

        return settings;
    }

    private static string Stringify(JsonElement element) => element.ValueKind switch
    {
        JsonValueKind.String => element.GetString() ?? string.Empty,
        JsonValueKind.Null => string.Empty,
        _ => element.GetRawText(),
    };

    /// <summary>
    /// Compose reports <c>mem_limit</c> in bytes and <c>cpus</c> as a bare number,
    /// while an operator writes <c>12g</c> and <c>6.0</c>. Normalise both sides of
    /// those two rather than forcing the runbook to spell out byte counts, which no
    /// reader would check.
    /// </summary>
    private static string Normalise(string key, string value)
    {
        var setting = key[(key.IndexOf('.') + 1)..];

        if (setting.Equals("mem_limit", StringComparison.Ordinal))
        {
            return ToBytes(value).ToString(CultureInfo.InvariantCulture);
        }

        if (setting.Equals("cpus", StringComparison.Ordinal)
            && decimal.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out var cpus))
        {
            return cpus.ToString("0.###", CultureInfo.InvariantCulture);
        }

        return value.Trim();
    }

    private static long ToBytes(string value)
    {
        var trimmed = value.Trim();
        var multiplier = 1L;

        if (trimmed.EndsWith('g') || trimmed.EndsWith('G')) multiplier = 1024L * 1024 * 1024;
        else if (trimmed.EndsWith('m') || trimmed.EndsWith('M')) multiplier = 1024L * 1024;
        else if (trimmed.EndsWith('k') || trimmed.EndsWith('K')) multiplier = 1024L;

        if (multiplier > 1) trimmed = trimmed[..^1];

        return decimal.TryParse(trimmed, NumberStyles.Any, CultureInfo.InvariantCulture, out var scalar)
            ? (long)(scalar * multiplier)
            : -1;
    }

    private static string RunDockerCompose(string workingDirectory, string? envFile = null)
    {
        var start = new ProcessStartInfo("docker")
        {
            WorkingDirectory = workingDirectory,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };

        start.ArgumentList.Add("compose");

        if (envFile is not null)
        {
            // Overrides the auto-loaded `.env`, which is gitignored machine-local
            // state. Paired with stripping the same variables from the child's own
            // environment below, because a shell variable outranks an env file.
            start.ArgumentList.Add("--env-file");
            start.ArgumentList.Add(envFile);

            foreach (var (_, variable) in CpusetVariables)
            {
                start.Environment.Remove(variable);
            }
        }

        start.ArgumentList.Add("-f");
        start.ArgumentList.Add(BaseComposeFile);
        start.ArgumentList.Add("-f");
        start.ArgumentList.Add(TuningComposeFile);
        start.ArgumentList.Add("config");
        start.ArgumentList.Add("--format");
        start.ArgumentList.Add("json");

        // ISSUE #2627. REPOCONTEXT_MEMORY_ARCHIVE_PATH is REQUIRED by the base compose
        // file - it deliberately has no default, because a relative one resolves against
        // whatever directory compose was invoked from and put the only working backup of
        // durable agent memory inside an ephemeral git worktree. An operator supplies it
        // through .env, which is gitignored and so absent in CI, and resolution is what
        // this fixture is here to do rather than a deployment. Any absolute path resolves
        // the document identically, so the value is irrelevant and only its presence
        // matters.
        start.Environment["REPOCONTEXT_MEMORY_ARCHIVE_PATH"] =
            Path.Combine(Path.GetTempPath(), "repocontext-memory-archive-hygiene");

        Process? process = null;
        try
        {
            process = Process.Start(start);
        }
        catch (Exception ex)
        {
            RequireToolchain($"could not start `docker`: {ex.Message}");
        }

        Assert.That(process, Is.Not.Null, "expected `docker` to start.");

        var stdout = process!.StandardOutput.ReadToEnd();
        var stderr = process.StandardError.ReadToEnd();

        if (!process.WaitForExit(milliseconds: 120_000))
        {
            try { process.Kill(entireProcessTree: true); } catch { /* already gone */ }
            Assert.Fail("`docker compose config` did not complete within two minutes.");
        }

        if (process.ExitCode != 0)
        {
            // A required variable that this fixture failed to supply is a fault in the
            // repository, not an absent toolchain, and routing it through
            // RequireToolchain would let it SKIP on a developer machine - a green run
            // over a document that was never resolved.
            if (stderr.Contains("REPOCONTEXT_MEMORY_ARCHIVE_PATH", StringComparison.Ordinal))
            {
                Assert.Fail(
                    "`docker compose config` refused because REPOCONTEXT_MEMORY_ARCHIVE_PATH was not "
                    + "supplied. That variable is required by design (issue #2627) and this fixture "
                    + $"is meant to set it: {stderr.Trim()}");
            }

            // A daemon that is not running, or a Docker CLI without the compose v2
            // plugin, is an absent toolchain rather than a failing assertion.
            RequireToolchain($"`docker compose config` exited {process.ExitCode}: {stderr.Trim()}");
        }

        return stdout;
    }

    /// <summary>
    /// Handles an absent Docker toolchain asymmetrically: visibly skipped on a
    /// developer machine, red in CI. Never <c>Assert.Inconclusive</c>, which NUnit
    /// counts as neither passed, failed, nor skipped, so a run that checked nothing
    /// still prints <c>Passed!</c> with <c>Skipped: 0</c> and only the total moves.
    /// </summary>
    private static void RequireToolchain(string detail)
    {
        var underCi = string.Equals(
            Environment.GetEnvironmentVariable("GITHUB_ACTIONS"),
            "true",
            StringComparison.OrdinalIgnoreCase);

        if (underCi)
        {
            Assert.Fail(
                "Docker is required to evaluate the resolved compose document, and it must be "
                + "present in CI: skipping here would let the runbook drift from the compose "
                + $"files behind a green check. {detail}");
        }

        Assert.Ignore(
            "Docker is unavailable, so the resolved-document parity assertion cannot run. The "
            + "toolchain-free assertions in this fixture still ran. "
            + $"{detail}");
    }
}
