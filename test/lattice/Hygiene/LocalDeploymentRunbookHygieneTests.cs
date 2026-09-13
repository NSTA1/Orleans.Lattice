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

    /// <summary>
    /// The marker a DERIVED row must carry in the Value column. Distinct from
    /// <see cref="RedactionMarker"/> because the two express opposite things: a redacted
    /// value is one the runbook declines to reproduce, whereas a derived value is one the
    /// runbook <b>cannot</b> reproduce, because it is a property of the host and corpus
    /// the stack is deployed on rather than of the repository.
    /// </summary>
    private const string DerivedMarker = "derived";

    /// <summary>
    /// Settings whose value is derived per deployment (issue #2779) and therefore has no
    /// correct literal to document. Every resource knob in the tuning overlay was a
    /// transcription of one developer machine - 16 logical CPUs and 55.7 GiB of RAM - so
    /// the two <c>mem_limit</c> values alone summed to 17 GiB and the stack could not
    /// start at all on a 16 GiB host.
    /// <para>
    /// Unlike <see cref="RedactedValues"/>, this set does NOT widen a hole, because the
    /// value comparison it replaces is exchanged for a STRICTER property rather than a
    /// weaker one. Value parity asks whether a literal is currently correct; the
    /// companion test
    /// <see cref="Every_derived_resource_setting_is_declared_as_a_variable_reference"/>
    /// asks whether the setting is capable of being a literal at all, which is the
    /// property actually wanted and which no value comparison can express.
    /// </para>
    /// </summary>
    private static readonly HashSet<string> DerivedValues = new(StringComparer.Ordinal)
    {
        "repocontext.cpus",
        "repocontext.mem_limit",
        "repocontext.DOTNET_GCHeapCount",
        "repocontext.LATTICE_WAL_MAX_CONCURRENT_REPLAYS",
        "embedder.cpus",
        "embedder.mem_limit",
        "embedder.EMBED_INTRA_THREADS",
    };

    /// <summary>
    /// Values supplied to <c>docker compose config</c> purely so the document RESOLVES.
    /// </summary>
    /// <remarks>
    /// <para>
    /// These are not derived values and are not a recommendation. Every setting listed in
    /// <see cref="DerivedValues"/> is a <c>${VAR:?}</c> reference with no default, so
    /// compose refuses to resolve the document until each is supplied. This fixture
    /// resolves a document; it does not deploy one, so the values need only PARSE.
    /// New-TuningEnv.ps1 is what derives real ones.
    /// </para>
    /// <para>
    /// Deliberately ODD values, not the reference host's 6/12g/4/5g. If they matched, a
    /// regression that dropped a setting back to a hard-coded literal would still satisfy
    /// the parity assertions by coincidence, and the guard would read green for the wrong
    /// reason. Distinctive values mean the resolved document can only agree with these if
    /// interpolation actually happened.
    /// </para>
    /// </remarks>
    private static readonly (string Name, string Value)[] ToolchainProbeVariables =
    [
        ("REPOCONTEXT_CPUS", "3"),
        ("REPOCONTEXT_MEM_LIMIT", "7168m"),
        ("REPOCONTEXT_GC_HEAP_COUNT", "3"),
        ("REPOCONTEXT_MAX_CONCURRENT_REPLAYS", "3"),
        ("EMBEDDER_CPUS", "2"),
        ("EMBEDDER_MEM_LIMIT", "5120m"),
        ("EMBEDDER_INTRA_THREADS", "2"),
    ];

    /// <summary>
    /// A required-variable reference - <c>${NAME:?message}</c>. The <c>:?</c> form is the
    /// one this deployment uses for a value that must not have a default (issue #2627
    /// established it for the memory archive path, and #2779 extends it to every resource
    /// knob). A bare <c>${NAME}</c> does not qualify: compose refuses an empty
    /// <c>mem_limit</c> or <c>cpus</c> either way, so the difference is not silent
    /// breakage but the message - <c>invalid size: ''</c> sends an operator to the YAML,
    /// whereas this form names the variable and the script that derives it.
    /// </summary>
    private static readonly Regex RequiredVariableReference = new(
        @"^\$\{[A-Z_][A-Z0-9_]*:\?[^}]+\}$",
        RegexOptions.Compiled);

    private static readonly Regex TableRow = new(
        @"^\|\s*`(?<service>[^`]+)`\s*\|\s*`(?<setting>[^`]+)`\s*\|\s*`(?<value>[^`]*)`\s*\|(?<why>[^|]*)\|\s*$",
        RegexOptions.Compiled);

    /// <summary>The compose service whose image this repository builds (#2707).</summary>
    private const string BuiltService = "repocontext";

    /// <summary>
    /// The OCI label that carries the built commit. It is the only provenance channel
    /// that travels with the image; the <c>candidate-&lt;sha&gt;</c> tag is assigned by a
    /// person afterwards and can be moved.
    /// </summary>
    private const string ProvenanceLabel = "org.opencontainers.image.revision";

    /// <summary>
    /// The untracked directory an earlier revision of the runbook told operators to
    /// build from. Named here so the runbook can be asserted to still warn about it.
    /// </summary>
    private const string UntrackedBuildDirectory = ".deploy/";

    /// <summary>
    /// How far after a <c>docker build</c> to look for its own build-args. Comfortably
    /// longer than the multi-line command and far shorter than the gap to the next one,
    /// so the assertion is per-command rather than document-wide.
    /// </summary>
    private const int BuildCommandWindow = 400;

    /// <summary>
    /// A <c>docker build -f &lt;dockerfile&gt;</c> invocation in the runbook, capturing
    /// the build input it names.
    /// </summary>
    private static readonly Regex DockerBuildCommand = new(
        @"docker build\s+-f\s+(?<dockerfile>\S+)",
        RegexOptions.Compiled);

    /// <summary>
    /// A <c>docker inspect ... --format</c> that reads the provenance label back out of
    /// an image, in either argument order.
    /// </summary>
    private static readonly Regex LabelReadBack = new(
        @"docker inspect[^\r\n]*(\r?\n[^\r\n]*)?" + Regex.Escape(ProvenanceLabel),
        RegexOptions.Compiled);

    /// <summary>A top-level service declaration in a compose file.</summary>
    private static readonly Regex ServiceDeclaration = new(
        @"^\s{2}(?<name>[A-Za-z0-9_.-]+):\s*$",
        RegexOptions.Compiled);

    /// <summary>A <c>context:</c> or <c>dockerfile:</c> key inside a compose build stanza.</summary>
    private static readonly Regex BuildKey = new(
        @"^\s{4,}(?<key>context|dockerfile)\s*:\s*""?(?<value>[^""#]*?)""?\s*$",
        RegexOptions.Compiled);

    /// <summary>A <c>KEY: "value"</c> or <c>KEY: value</c> line inside a compose file.</summary>
    private static readonly Regex YamlScalar = new(
        @"^\s{6,}(?<key>[A-Za-z_][A-Za-z0-9_.]*)\s*:\s*"
        + @"(?:""(?<value>[^""]*)""|(?<value>[^#]*?))\s*(?:#.*)?$",
        RegexOptions.Compiled);

    /// <summary>
    /// A service-level scalar such as <c>cpus:</c> or <c>mem_limit:</c>, which sits at
    /// indent 4 rather than the indent 6+ an environment entry sits at.
    /// </summary>
    private static readonly Regex ScalarDeclaration = new(
        @"^\s{4}(?<key>[A-Za-z_][A-Za-z0-9_.]*)\s*:\s*"
        + @"(?:""(?<value>[^""]*)""|(?<value>[^#]*?))\s*(?:#.*)?$",
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
                && !DerivedValues.Contains(entry.Key)
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

        // The derived seam is the same shape as the redaction seam above, and for the
        // same reason: the value comparison is REPLACED by two assertions rather than
        // dropped. Presence parity in both directions still applies unchanged, and the
        // form assertion that makes the setting incapable of being a literal lives in
        // Every_derived_resource_setting_is_declared_as_a_variable_reference.
        var badDerivation = DerivedValues
            .Where(resolved.ContainsKey)
            .Where(key => !documented.TryGetValue(key, out var cell)
                || !cell.Equals(DerivedMarker, StringComparison.Ordinal)
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

            Assert.That(
                badDerivation,
                Is.Empty,
                $"these settings are derived per deployment (#2779), so {RunbookPath} must "
                + $"document them with the exact cell `{DerivedMarker}` and the resolved "
                + "document must still supply a non-empty value. Writing a host's actual "
                + "number into the table is how the transcription comes back: the table is "
                + "read as the value to use, and the next operator copies a figure measured "
                + "on 16 logical CPUs and 55.7 GiB of RAM onto a machine that has neither.");
        });
    }

    /// <summary>
    /// Every derived resource knob must be declared in the tracked overlay as a
    /// <c>${NAME:?...}</c> reference, so the file is INCAPABLE of carrying a literal.
    /// <para>
    /// This is the assertion that makes "adaptive" enforced rather than documented, and
    /// it is deliberately separate from the parity test above. Parity compares values,
    /// and a value comparison cannot distinguish a derived setting from a literal that
    /// happens to be correct on the machine the suite is running on - which is precisely
    /// the state issue #2779 describes, where every knob was right for one host and wrong
    /// for every other. Only a check on the setting's FORM can express that.
    /// </para>
    /// <para>
    /// It is also toolchain-free on purpose. The parity half needs Docker and is skipped
    /// on a developer machine without it, so putting this property there would let the
    /// one assertion that cannot be satisfied by luck be the one that silently does not
    /// run. Reading the tracked file as text needs nothing.
    /// </para>
    /// </summary>
    [Test]
    public void Every_derived_resource_setting_is_declared_as_a_variable_reference()
    {
        var path = Path.Combine(
            HygieneRepository.FindRepoRoot(),
            ComposeDirectory.Replace('/', Path.DirectorySeparatorChar),
            TuningComposeFile);

        var declared = new Dictionary<string, string>(StringComparer.Ordinal);
        var service = string.Empty;

        foreach (var line in File.ReadAllLines(path))
        {
            var serviceMatch = ServiceDeclaration.Match(line);

            if (serviceMatch.Success)
            {
                service = serviceMatch.Groups["name"].Value;
                continue;
            }

            if (service.Length == 0)
            {
                continue;
            }

            // Environment entries sit at indent 6+; the service-level scalars this guard
            // cares about (cpus, mem_limit) sit at indent 4. YamlScalar covers the first,
            // ScalarDeclaration the second, and a key matched by neither is not a setting.
            var match = YamlScalar.Match(line);

            if (!match.Success)
            {
                match = ScalarDeclaration.Match(line);
            }

            if (match.Success)
            {
                declared[$"{service}.{match.Groups["key"].Value}"] = match.Groups["value"].Value.Trim();
            }
        }

        // ANCHOR DerivedValues TO AN INDEPENDENT ARTEFACT.
        //
        // The presence assertion below has the form filter(declared, by: DerivedValues)
        // == DerivedValues, which holds for EVERY value of DerivedValues - including one
        // quietly shrunk so a setting can go back to a hard-coded literal. A perturbation
        // of that constant therefore cannot redden it: the list is both the thing checked
        // and the thing checking. Anchoring it to the runbook's `derived` cells - a
        // separate file, edited by a separate hand - is what makes shrinking it
        // observable, and unlike the parity gate it needs no toolchain to do so.
        var documentedAsDerived = ParseTable()
            .Where(row => row.Value.Equals(DerivedMarker, StringComparison.Ordinal))
            .Select(row => row.Key)
            .Order(StringComparer.Ordinal)
            .ToArray();

        Assert.That(
            documentedAsDerived,
            Is.EquivalentTo(DerivedValues.Order(StringComparer.Ordinal)),
            $"the settings this fixture treats as derived must be exactly those the runbook "
            + $"marks `{DerivedMarker}`. Without this, dropping an entry from DerivedValues "
            + "would let the matching setting revert to a literal while every assertion "
            + "keyed on that list stayed green - the list would be grading its own work.");

        // Guards against the scan silently ceasing to parse the file, which would make
        // every assertion below vacuously true while still reporting green.
        Assert.That(
            declared.Keys.Where(DerivedValues.Contains).Order(StringComparer.Ordinal),
            Is.EquivalentTo(DerivedValues.Order(StringComparer.Ordinal)),
            $"expected to find every derived setting in {TuningComposeFile}. A derived "
            + "setting that has vanished from the overlay is not thereby adaptive - it has "
            + "fallen back to whatever the base file or the Docker default supplies, "
            + "unreviewed.");

        var literals = DerivedValues
            .Where(declared.ContainsKey)
            .Where(key => !RequiredVariableReference.IsMatch(declared[key]))
            .Select(key => $"{key} is declared as `{declared[key]}`, which is a literal. It "
                + $"must be a `${{{SuggestVariableName(key)}:?...}}` reference instead.")
            .Order(StringComparer.Ordinal)
            .ToArray();

        Assert.That(
            literals,
            Is.Empty,
            $"{TuningComposeFile} declares a derived resource knob as a literal value. Every "
            + "resource knob in this overlay was once a transcription of one developer "
            + "machine (16 logical CPUs, 55.7 GiB RAM), which is issue #2779: the two "
            + "mem_limit values summed to 17 GiB, so the stack could not start on a 16 GiB "
            + "host, and DOTNET_PROCESSOR_COUNT: \"16\" held the WAL replay concurrency gate "
            + "at 16 permits against a 6.0-CPU quota - a 2.67x oversubscription measured on "
            + "the live deployment, and the deployment half of the root cause in #2692. "
            + "Derive it with scripts/New-TuningEnv.ps1 rather than writing the number here.");
    }

    /// <summary>
    /// The variable name a derived setting is expected to read from, so the failure above
    /// names the fix rather than only the fault.
    /// </summary>
    private static string SuggestVariableName(string key)
    {
        var separator = key.IndexOf('.');
        var service = key[..separator];
        var setting = key[(separator + 1)..];

        var prefix = service.ToUpperInvariant();

        return setting switch
        {
            "cpus" => $"{prefix}_CPUS",
            "mem_limit" => $"{prefix}_MEM_LIMIT",
            "DOTNET_GCHeapCount" => $"{prefix}_GC_HEAP_COUNT",
            "LATTICE_WAL_MAX_CONCURRENT_REPLAYS" => $"{prefix}_MAX_CONCURRENT_REPLAYS",
            "EMBED_INTRA_THREADS" => $"{prefix}_INTRA_THREADS",
            _ => $"{prefix}_{setting.ToUpperInvariant()}",
        };
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
    // Build-input parity between the runbook and the compose build stanza (#2707).
    // ---------------------------------------------------------------------

    /// <summary>
    /// Every <c>docker build -f</c> the runbook instructs an operator to run must name
    /// the same Dockerfile <c>docker-compose.yml</c>'s <c>repocontext</c> build stanza
    /// declares, and that Dockerfile must exist under the context the stanza resolves
    /// to.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>The drift this catches actually happened.</b> #2690 moved the tracked build
    /// onto <c>apps/repocontext/Dockerfile</c> and added the <c>GIT_COMMIT</c> arg that
    /// stamps <c>org.opencontainers.image.revision</c>. The runbook kept pointing at
    /// <c>.deploy/Dockerfile</c>, which is <b>untracked</b> - <c>git ls-files .deploy</c>
    /// returns nothing - and which declares no such <c>ARG</c>, so an operator following
    /// the document built a different image from the one the tracked path produces, and
    /// one provenance refuses. Nothing detected it: the fix was reachable in source and
    /// unreachable in practice, which is the recurring shape epic #2368 exists to close
    /// (#2707).
    /// </para>
    /// <para>
    /// It asserts over the runbook's <i>build commands</i> rather than over every
    /// mention of the word, on purpose. The runbook now names <c>.deploy/Dockerfile</c>
    /// in prose, to tell a reader who has one that it is not a build input; a needle
    /// test for that string would forbid the very warning the fix adds.
    /// </para>
    /// </remarks>
    [Test]
    public void Every_runbook_build_command_names_the_dockerfile_the_compose_stanza_declares()
    {
        var root = HygieneRepository.FindRepoRoot();
        var runbook = File.ReadAllText(Path.Combine(
            root,
            RunbookPath.Replace('/', Path.DirectorySeparatorChar)));

        var (context, dockerfile) = ComposeBuildStanza();

        var commands = DockerBuildCommand.Matches(runbook)
            .Select(m => m.Groups["dockerfile"].Value)
            .ToList();

        var composeDirectory = Path.Combine(
            root,
            ComposeDirectory.Replace('/', Path.DirectorySeparatorChar));

        var resolvedContext = Path.GetFullPath(Path.Combine(
            composeDirectory,
            context.Replace('/', Path.DirectorySeparatorChar)));

        var resolvedDockerfile = Path.Combine(
            resolvedContext,
            dockerfile.Replace('/', Path.DirectorySeparatorChar));

        Assert.Multiple(() =>
        {
            Assert.That(
                commands,
                Is.Not.Empty,
                $"{RunbookPath} declares no `docker build -f` command at all. This guard "
                + "compares the runbook's build input against the compose stanza's; with no "
                + "command to read it would pass vacuously, which is worse than absent.");

            Assert.That(
                commands,
                Is.All.EqualTo(dockerfile),
                $"{RunbookPath} tells an operator to build a Dockerfile that "
                + $"{ComposeDirectory}/{BaseComposeFile} does not declare. Compose builds "
                + $"`{dockerfile}`; the runbook names {string.Join(", ", commands.Distinct())}. "
                + "An operator who follows the document therefore produces a different image "
                + "from the tracked build path, which is exactly the divergence #2707 found: "
                + "the runbook still pointed at an UNTRACKED `.deploy/Dockerfile` that cannot "
                + "stamp `org.opencontainers.image.revision`, so the provenance gate refused "
                + "the result.");

            Assert.That(
                Path.GetFullPath(resolvedContext).TrimEnd(Path.DirectorySeparatorChar),
                Is.EqualTo(Path.GetFullPath(root).TrimEnd(Path.DirectorySeparatorChar)),
                $"the `context: {context}` in {ComposeDirectory}/{BaseComposeFile} no longer "
                + "resolves to the repository root. The runbook's build command is written to "
                + "be run FROM the repository root with `.` as its context, so the two agree "
                + "only while this holds.");

            Assert.That(
                File.Exists(resolvedDockerfile),
                Is.True,
                $"`{dockerfile}` does not exist under the resolved build context "
                + $"({resolvedContext}). Both the runbook and compose name a build input that "
                + "is not there.");
        });
    }

    /// <summary>
    /// The runbook must pass <c>GIT_COMMIT</c> into every build it documents, and must
    /// tell the operator to read the resulting label back out of the image.
    /// </summary>
    /// <remarks>
    /// <para>
    /// There are two independent ways to end up with an unprovenanced image, and
    /// correcting the <c>-f</c> path only closes one of them. <c>ARG GIT_COMMIT=""</c> in
    /// <c>apps/repocontext/Dockerfile</c> means a build that does not supply the arg
    /// <b>succeeds</b>, exits 0, and yields an image whose revision label names no
    /// commit. That is the failure that demonstrably occurred: the image running on the
    /// deployment host resolves no revision, so
    /// <c>Assert-ContainerProvenance.ps1</c> falls back to a movable tag.
    /// </para>
    /// <para>
    /// The read-back is the only step that turns that silence into a failure, so this
    /// asserts the runbook still carries one. It checks for the label name inside a
    /// <c>docker inspect</c> rather than for any particular script, because what matters
    /// is that the document instructs the operator to interrogate the artefact.
    /// </para>
    /// </remarks>
    [Test]
    public void The_runbook_stamps_the_commit_into_every_build_and_reads_the_label_back()
    {
        var root = HygieneRepository.FindRepoRoot();
        var runbook = File.ReadAllText(Path.Combine(
            root,
            RunbookPath.Replace('/', Path.DirectorySeparatorChar)));

        // Each build command, paired with the text that follows it, so "passes the arg"
        // is asserted per command rather than anywhere in the document.
        var unstamped = DockerBuildCommand.Matches(runbook)
            .Where(m => !runbook
                .Substring(m.Index, Math.Min(BuildCommandWindow, runbook.Length - m.Index))
                .Contains("--build-arg GIT_COMMIT=", StringComparison.Ordinal))
            .Select(m => m.Groups["dockerfile"].Value)
            .ToList();

        Assert.Multiple(() =>
        {
            Assert.That(
                unstamped,
                Is.Empty,
                $"a `docker build` in {RunbookPath} does not pass `--build-arg GIT_COMMIT=`. "
                + "The Dockerfile defaults that arg to the empty string, so the build still "
                + "succeeds and still produces an image - one whose revision label names no "
                + "commit, which Assert-ContainerProvenance.ps1 treats as unresolved.");

            Assert.That(
                runbook,
                Does.Contain(ProvenanceLabel),
                $"{RunbookPath} no longer names `{ProvenanceLabel}`. Redirecting the build "
                + "path only closes the case where the label CANNOT be stamped; the label "
                + "read-back is what closes the case where it simply was not.");

            Assert.That(
                LabelReadBack.IsMatch(runbook),
                Is.True,
                $"{RunbookPath} no longer instructs the operator to read "
                + $"`{ProvenanceLabel}` back out of the built image with `docker inspect`. "
                + "Without that step an unstamped build is indistinguishable from a good one "
                + "until the deployment gate refuses it, which is after it has been tagged, "
                + "deployed, and measured against (#2686).");

            Assert.That(
                runbook,
                Does.Contain(UntrackedBuildDirectory),
                $"{RunbookPath} no longer states that `{UntrackedBuildDirectory}` is not a "
                + "build input. The directory is untracked, so it exists on some machines and "
                + "not others; a reader who has one needs to be told it is not the build "
                + "input, or this drift simply recurs.");
        });
    }

    /// <summary>
    /// The <c>build.context</c> and <c>build.dockerfile</c> the <c>repocontext</c>
    /// service declares in the base compose file, read by walking the service block
    /// rather than by taking the first match: the <c>embedder</c> service declares its
    /// own, different, pair earlier in the same file.
    /// </summary>
    private static (string Context, string Dockerfile) ComposeBuildStanza()
    {
        var root = HygieneRepository.FindRepoRoot();
        var composePath = Path.Combine(
            root,
            ComposeDirectory.Replace('/', Path.DirectorySeparatorChar),
            BaseComposeFile);

        string? service = null;
        string? context = null;
        string? dockerfile = null;

        foreach (var line in File.ReadAllLines(composePath))
        {
            var serviceMatch = ServiceDeclaration.Match(line);
            if (serviceMatch.Success)
            {
                service = serviceMatch.Groups["name"].Value;
                continue;
            }

            if (!string.Equals(service, BuiltService, StringComparison.Ordinal))
            {
                continue;
            }

            var keyMatch = BuildKey.Match(line);
            if (!keyMatch.Success)
            {
                continue;
            }

            if (keyMatch.Groups["key"].Value == "context")
            {
                context ??= keyMatch.Groups["value"].Value;
            }
            else
            {
                dockerfile ??= keyMatch.Groups["value"].Value;
            }
        }

        Assert.That(
            context,
            Is.Not.Null,
            $"the `{BuiltService}` service in {ComposeDirectory}/{BaseComposeFile} declares "
            + "no `build.context`. This guard reads the build input out of that stanza; with "
            + "no stanza it would have nothing to compare the runbook against.");

        Assert.That(
            dockerfile,
            Is.Not.Null,
            $"the `{BuiltService}` service in {ComposeDirectory}/{BaseComposeFile} declares "
            + "no `build.dockerfile`.");

        return (context!, dockerfile!);
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

    /// <summary>
    /// The corpus count that feeds the memory grant must EXCLUDE build and VCS
    /// directories, because that is the unit its slope was fitted against.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the arm that makes the exclusion falsifiable. Reverting Measure-Corpus to a
    /// bare <c>Get-ChildItem -Recurse -Force</c> reddens it, which matters because that
    /// regression does not present as a wrong number - it presents as the shortfall
    /// warning saying THE HOST IS TOO SMALL. On the reference repository a raw walk returns
    /// about 3.15x the tracked count, which requests roughly 35 GiB, clamps to the ceiling
    /// and blames the machine. The operator's obvious responses (raise the host share, buy
    /// RAM) both appear to work, which confirms the wrong model.
    /// </para>
    /// <para>
    /// It also pins the separator-anchored match. A naive <c>-match 'bin'</c> would
    /// exclude a directory called <c>binaries</c> and a file called <c>bin.txt</c>, so the
    /// fixture contains both and requires them COUNTED. Without them the assertion would
    /// pass just as well for an over-broad filter, which fails low and silently.
    /// </para>
    /// </remarks>
    [Test]
    public void The_corpus_count_excludes_build_and_vcs_directories()
    {
        var script = Path.Combine(
            HygieneRepository.FindRepoRoot(),
            ComposeDirectory.Replace('/', Path.DirectorySeparatorChar),
            "scripts",
            "New-TuningEnv.ps1");

        Assert.That(File.Exists(script), Is.True, $"expected the derivation script at {script}.");

        var sandbox = Path.Combine(Path.GetTempPath(), "lattice-corpus-" + Guid.NewGuid().ToString("N"));

        try
        {
            // 5 ordinary source files, plus two decoys that MUST be counted.
            Directory.CreateDirectory(sandbox);

            for (var i = 0; i < 5; i++)
            {
                File.WriteAllText(Path.Combine(sandbox, $"source{i}.cs"), "//");
            }

            File.WriteAllText(Path.Combine(sandbox, "bin.txt"), "not a directory");
            Directory.CreateDirectory(Path.Combine(sandbox, "binaries"));
            File.WriteAllText(Path.Combine(sandbox, "binaries", "keep.cs"), "//");

            // 12 files that must NOT be counted.
            foreach (var (directory, count) in new[] { (".git", 7), ("bin", 3), ("obj", 2) })
            {
                Directory.CreateDirectory(Path.Combine(sandbox, directory));

                for (var i = 0; i < count; i++)
                {
                    File.WriteAllText(Path.Combine(sandbox, directory, $"f{i}.dat"), "x");
                }
            }

            var json = RunCorpusOnly(script, sandbox);

            var raw = int.Parse(Regex.Match(json, @"""Raw""\s*:\s*(\d+)").Groups[1].Value);
            var counted = int.Parse(Regex.Match(json, @"""Counted""\s*:\s*(\d+)").Groups[1].Value);

            Assert.Multiple(() =>
            {
                Assert.That(raw, Is.EqualTo(19), "the sandbox should contain 19 files on disk.");
                Assert.That(
                    counted,
                    Is.EqualTo(7),
                    "the corpus count must exclude .git, bin and obj (12 files) while KEEPING "
                    + "`binaries/keep.cs` and `bin.txt`. A count of 19 means the exclusion was "
                    + "removed and the grant will be derived from a raw tree walk, which "
                    + "over-requests by roughly 3x and then blames the host. A count below 7 "
                    + "means the match is over-broad and will silently under-grant.");
            });
        }
        finally
        {
            try { Directory.Delete(sandbox, recursive: true); } catch { /* best effort */ }
        }
    }

    private static string RunCorpusOnly(string script, string workspace)
    {
        foreach (var shell in new[] { "pwsh", "powershell" })
        {
            var start = new ProcessStartInfo(shell)
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };

            start.ArgumentList.Add("-NoProfile");
            start.ArgumentList.Add("-File");
            start.ArgumentList.Add(script);
            start.ArgumentList.Add("-WorkspacePath");
            start.ArgumentList.Add(workspace);
            start.ArgumentList.Add("-CorpusOnly");

            Process? process;

            try
            {
                process = Process.Start(start);
            }
            catch
            {
                continue;
            }

            if (process is null)
            {
                continue;
            }

            var stdout = process.StandardOutput.ReadToEnd();
            var stderr = process.StandardError.ReadToEnd();
            process.WaitForExit(milliseconds: 120_000);

            if (process.ExitCode != 0)
            {
                Assert.Fail($"`{shell} New-TuningEnv.ps1 -CorpusOnly` exited {process.ExitCode}: {stderr.Trim()}");
            }

            return stdout;
        }

        RequireToolchain("neither `pwsh` nor `powershell` could be started.");
        return string.Empty;
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

        // ISSUE #2779, and the same reasoning one step further. The tuning overlay's
        // resource knobs are `${VAR:?...}` with no defaults, so compose refuses to
        // resolve the document at all until every one is supplied. These values only
        // have to PARSE - this fixture resolves a document, it does not deploy one - so
        // they are deliberately not the derived values for this host and must not be
        // read as a recommendation. New-TuningEnv.ps1 derives those.
        //
        // Supplying them here is what keeps the parity gate RUNNING. Without it the
        // refusal below reaches RequireToolchain and the two strongest assertions in
        // this file skip on a developer box: a green run over a document that was never
        // resolved, caused by the very change that was meant to harden it.
        foreach (var (name, value) in ToolchainProbeVariables)
        {
            start.Environment[name] = value;
        }

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
            //
            // This now keys on the CLASS of failure rather than on one variable name.
            // The #2627 guard below named REPOCONTEXT_MEMORY_ARCHIVE_PATH specifically,
            // which was correct for the only no-default variable that then existed;
            // #2779 added seven more, and a guard that enumerates names silently stops
            // covering the next one somebody adds. Compose emits "required variable X is
            // missing a value" for every `${VAR:?}`, so matching that phrase covers all
            // of them, including ones added after this comment was written.
            if (stderr.Contains("required variable", StringComparison.OrdinalIgnoreCase))
            {
                Assert.Fail(
                    "`docker compose config` refused because a variable with no default was not "
                    + "supplied. Those are required by design (issues #2627 and #2779) and this "
                    + "fixture is meant to set every one of them - see ToolchainProbeVariables. "
                    + $"This is a repository fault, not an absent toolchain: {stderr.Trim()}");
            }

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
