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
/// exactly the settings the tracked compose files actually resolve to.
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
    // Helpers.
    // ---------------------------------------------------------------------

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

    private static string RunDockerCompose(string workingDirectory)
    {
        var start = new ProcessStartInfo("docker")
        {
            WorkingDirectory = workingDirectory,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };

        start.ArgumentList.Add("compose");
        start.ArgumentList.Add("-f");
        start.ArgumentList.Add(BaseComposeFile);
        start.ArgumentList.Add("-f");
        start.ArgumentList.Add(TuningComposeFile);
        start.ArgumentList.Add("config");
        start.ArgumentList.Add("--format");
        start.ArgumentList.Add("json");

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
