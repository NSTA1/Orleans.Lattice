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
/// CI cannot reach, is the disclosed exemption, and its calls are not in this pattern.
/// </summary>
[TestFixture]
[Category("EndToEnd")]
public sealed class JourneySiloParityTests
{
    private static readonly Regex Registration = new(
        @"\.(?<call>(?:Add|Map|Use|Configure|Replicate)\w*(?:Lattice|Azure)\w*)\s*[<(]",
        RegexOptions.CultureInvariant);

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

    private static HashSet<string> Calls(string path, string? source = null) =>
        Registration.Matches(source ?? File.ReadAllText(path))
            .Select(match => match.Groups["call"].Value)
            .ToHashSet(StringComparer.Ordinal);
}
