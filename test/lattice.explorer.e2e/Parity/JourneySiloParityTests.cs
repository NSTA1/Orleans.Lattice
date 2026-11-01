using System.Text.RegularExpressions;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Parity;

/// <summary>
/// The journey silo stays a reference-architecture silo (#5359): every Lattice or Azure
/// storage registration the reference silo makes - an <c>AddLattice*</c>, <c>MapLattice*</c>,
/// <c>ConfigureLattice*</c> or <c>UseAzure*</c>/<c>AddAzure*</c> call - the journey silo makes
/// too. A registration added to the reference silo without its journey counterpart fails
/// here, so the journeys cannot quietly drift from what a deployment runs. The journey silo
/// may register more (Apps, tenancy, replication status); only the Entra provider, the one
/// CI cannot reach, is the disclosed exemption, and its calls are not in this pattern. The
/// journey hosts' test-only diagnostic and fault endpoints are disclosed here too.
/// </summary>
[TestFixture]
[Category("EndToEnd")]
public sealed class JourneySiloParityTests
{
    private static readonly Regex Registration = new(
        @"\.(?<call>(?:Add|Map|Use|Configure|Replicate)\w*(?:Lattice|Azure)\w*)\s*[<(]",
        RegexOptions.CultureInvariant);

    /// <summary>The journey hosts' disclosed diagnostic and fault endpoints.</summary>
    private static readonly string[] DisclosedDiagnosticSeams =
    [
        "GET /e2e/activation",  // which silo hosts an activation
        "GET /e2e/activations", // the activations whose key contains a fragment
        "POST /e2e/orphan",     // the state a crash mid-split leaves (#5359)
        "POST /e2e/memory",     // the live heap after a forced collection, on silos and the Explorer head (#5475)
        "POST /e2e/backup-writes", // hold backup artifact writes so A6 can cancel a running capture (#5377)
        "POST /e2e/wal-target", // hold a WAL move's writes to its target, so a stop lands before the flip (#5382)
        "POST /e2e/wal-pin",    // plant a WAL materialiser pin of a named kind on a tree (#5382)
        "DELETE /e2e/wal-pin",  // release a planted WAL pin (#5382)
    ];

    /// <summary>Every registration call of the reference silo appears in the journey silo.</summary>
    [Test]
    public void The_journey_silo_makes_every_registration_the_reference_silo_makes()
    {
        var reference = Calls(Path.Combine(JourneyPaths.RepositoryRoot, "reference-architecture", "hosts", "Silo", "Program.cs"));
        var journey = Calls(Path.Combine(JourneyPaths.ProjectDirectory, "Hosts", "JourneySilo", "Program.cs"));

        Assert.That(reference, Has.Count.GreaterThan(20), "the scan found too few reference registrations; the pattern is broken");
        Assert.That(reference, Does.Contain("AddLatticeStateApi").And.Contain("UseAzureStorageClustering").And.Contain("MapLatticeTreeAdminApiGrpc"),
            "the scan no longer reads the calls it exists for");

        var missing = reference.Except(journey, StringComparer.Ordinal).Order(StringComparer.Ordinal).ToArray();
        Assert.That(missing, Is.Empty,
            "The reference silo registers these and the journey silo does not; add each to "
            + "test/lattice.explorer.e2e/Hosts/JourneySilo/Program.cs: " + string.Join(", ", missing));
    }

    /// <summary>
    /// The journey silo's disclosed deviations from the reference silo's configuration, each
    /// with its reason. A deviation is a deliberate operator choice the story depends on; this
    /// list keeps each one visible, and fails when the source no longer carries it.
    /// </summary>
    private static readonly (string Source, string Reason)[] Deviations =
    [
        ("AddLatticeSchemaEnforcement(options => options.StrictIngest = true)", "dead-letter feature coverage (#5389)"),
        ("AddLatticeWalStorageProvider(JourneyHostEnvironment.SecondaryWalProviderKey", "a WAL move target for the Cluster area's WAL tools (A8, A12)"),
        ("JourneyBackupWriteHold.Decorate(silo.Services)", "fault seam, only with LATTICE_E2E_FAULTS=1: the harness can hold backup artifact writes so a capture is still running when A6 cancels it (#5377)"),
        ("JourneyHostEnvironment.FaultsEnabled ? new JourneyGatedWalStorageProvider(provider) : provider", "with the fault seams on, the move target's writes can be held per tree so a stop lands before the flip (A12)"),
        ("options.AccessAdministrationDelegationEnabled = true", "delegated access administration for the server-authority journey A9 (#5373)"),
    ];

    /// <summary>Every disclosed deviation is still made by the journey silo, so the list cannot rot into fiction.</summary>
    [Test]
    public void Every_disclosed_deviation_is_still_made_by_the_journey_silo()
    {
        var journey = File.ReadAllText(Path.Combine(JourneyPaths.ProjectDirectory, "Hosts", "JourneySilo", "Program.cs"));
        var absent = Deviations.Where(deviation => !journey.Contains(deviation.Source, StringComparison.Ordinal))
            .Select(deviation => $"{deviation.Source} ({deviation.Reason})")
            .ToArray();
        Assert.That(absent, Is.Empty, "Disclosed deviations the journey silo no longer makes; remove them from the list: " + string.Join("; ", absent));
    }

    /// <summary>The pattern matches the call shapes the hosts use, and not a lookalike in prose.</summary>
    [Test]
    public void The_pattern_reads_registration_calls_only()
    {
        Assert.That(Calls(string.Empty, "silo.AddLatticeBackup();\nbuilder.Services.AddLatticeDataApiGrpc(o => {});\nsilo.UseAzureTableReminderService(o => {});"),
            Is.EquivalentTo(new[] { "AddLatticeBackup", "AddLatticeDataApiGrpc", "UseAzureTableReminderService" }));
        Assert.That(Calls(string.Empty, "// AddLatticeBackup registers the engine\nsilo.AddEntraCredentialAuthenticator(o => {});"), Is.Empty);
    }

    /// <summary>
    /// The journey hosts' diagnostic and fault endpoints are a disclosed, test-only deviation
    /// from the reference heads: each is listed in <see cref="DisclosedDiagnosticSeams"/>,
    /// served under <c>/e2e</c> (on a silo, its internal port only; the Explorer head listens
    /// on loopback only), and mapped only when
    /// <c>LATTICE_E2E_FAULTS=1</c>, and no reference head serves any of them. A new seam fails
    /// here until it is disclosed.
    /// </summary>
    [Test]
    public void The_journey_hosts_diagnostic_seams_are_disclosed_and_mapped_only_with_the_fault_seams_on()
    {
        var hosts = Path.Combine(JourneyPaths.ProjectDirectory, "Hosts");
        var faultEndpoints = File.ReadAllText(Path.Combine(hosts, "JourneySilo", "JourneyFaultEndpoints.cs"));
        Assert.That(faultEndpoints, Does.Contain("public const string Prefix = \"/e2e\";"), "the silo's seams live under /e2e");
        var seams = DiagnosticSeams(faultEndpoints)
            .Concat(DiagnosticSeams(File.ReadAllText(Path.Combine(hosts, "Shared", "JourneyMemoryProbe.cs"))))
            .ToArray();
        Assert.That(seams, Is.EquivalentTo(DisclosedDiagnosticSeams), "every /e2e endpoint a journey host maps is disclosed here");

        foreach (var (host, mapping) in new[] { ("JourneySilo", "MapJourneyFaultEndpoints"), ("JourneyExplorer", "MapJourneyMemoryProbe") })
        {
            var program = File.ReadAllText(Path.Combine(hosts, host, "Program.cs"));
            var calls = Regex.Matches(program, @"\b" + mapping + @"\(\)").Count;
            var guarded = Regex.Matches(program, @"if \(JourneyHostEnvironment\.FaultsEnabled\)\s*\{\s*app\." + mapping + @"\(\);\s*\}").Count;
            Assert.That((calls, guarded), Is.EqualTo((1, 1)), $"{host} maps its diagnostic seams once, and only when the fault seams are enabled");
        }

        Assert.That(File.ReadAllText(Path.Combine(hosts, "JourneySilo", "Program.cs")),
            Does.Contain("UseInternalPortEndpointGuard(httpPort, \"/metrics\", \"/lattice/scale\", JourneyFaultEndpoints.Prefix)"),
            "the silo serves its /e2e seams on its internal port only");

        foreach (var reference in new[] { "Silo", "Explorer" })
        {
            Assert.That(File.ReadAllText(Path.Combine(JourneyPaths.RepositoryRoot, "reference-architecture", "hosts", reference, "Program.cs")),
                Does.Not.Contain("/e2e"), $"the reference {reference} serves no journey diagnostic seam");
        }
    }

    private static IEnumerable<string> DiagnosticSeams(string source) =>
        Regex.Matches(source, @"\.Map(?<verb>Get|Post|Put|Delete|Patch)\(\s*(?:Prefix\s*\+\s*""(?<suffix>[^""]+)""|(?<path>Path)\b)")
            .Select(match => match.Groups["verb"].Value.ToUpperInvariant() + " "
                + (match.Groups["path"].Success ? HostContract.JourneyMemoryProbe.Path : "/e2e" + match.Groups["suffix"].Value));

    private static HashSet<string> Calls(string path, string? source = null) =>
        Registration.Matches(source ?? File.ReadAllText(path))
            .Select(match => match.Groups["call"].Value)
            .ToHashSet(StringComparer.Ordinal);
}
