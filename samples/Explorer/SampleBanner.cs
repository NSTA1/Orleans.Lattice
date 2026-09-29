using Orleans.Lattice.Membership;
using Orleans.Lattice.Samples.Explorer.TaskBoard;

namespace Orleans.Lattice.Samples.Explorer;

/// <summary>The console output a started sample prints: every URL, every sample identity and how to drive the estate.</summary>
internal static class SampleBanner
{
    /// <summary>Writes the banner for a started <paramref name="sample"/>.</summary>
    /// <param name="writer">Where the banner goes.</param>
    /// <param name="sample">The started sample.</param>
    /// <param name="startup">How long the start took.</param>
    public static void Write(TextWriter writer, ExplorerSample sample, TimeSpan startup)
    {
        ArgumentNullException.ThrowIfNull(writer);
        ArgumentNullException.ThrowIfNull(sample);

        var estate = sample.West is not null;
        writer.WriteLine();
        writer.WriteLine(estate
            ? "Orleans.Lattice Explorer sample - two regions, tenancy and replication in one process"
            : "Orleans.Lattice Explorer sample - one region (--minimal)");
        writer.WriteLine($"Started in {startup.TotalSeconds:0.0}s.");
        writer.WriteLine();

        writer.WriteLine("URLs");
        writer.WriteLine($"  Explorer console   {sample.Console.Url}   (connected to region '{sample.ConsoleRegion.Id}')");
        foreach (var region in sample.Regions)
        {
            writer.WriteLine($"  Region {region.Id,-11} gRPC {region.Plan.GrpcEndpoint}   (silo port {region.Plan.SiloPort}, gateway port {region.Plan.GatewayPort})");
        }

        writer.WriteLine();
        writer.WriteLine(sample.Console.SignInAs is { } signInAs
            ? $"Sample identities (any password signs in; the console signs in as '{signInAs}' automatically - {ExplorerSampleOptions.SignInAsSwitch} picks another)"
            : $"Sample identities (any password signs in; the console starts signed out - use its Sign in dialog)");
        writer.WriteLine($"  {SampleIdentities.Administrator,-15} bootstrap administrator and platform operator: every area");
        if (estate)
        {
            writer.WriteLine($"  {SampleIdentities.AcmeAdmin,-15} tenant admin of '{SampleIdentities.AcmeTenant}': the tenant-scoped view at /t/{SampleIdentities.AcmeTenant}");
            writer.WriteLine($"  {SampleIdentities.GlobexAdmin,-15} tenant admin of '{SampleIdentities.GlobexTenant}': the tenant-scoped view at /t/{SampleIdentities.GlobexTenant}");
        }

        writer.WriteLine($"  {SampleIdentities.Alice,-15} groups '{SampleIdentities.OperatorsGroup}' and '{SampleIdentities.TaskEditorsGroup}'");
        writer.WriteLine($"  {SampleIdentities.Bob,-15} group '{SampleIdentities.TaskViewersGroup}'");
        writer.WriteLine($"  {SampleIdentities.Carol,-15} group '{SampleIdentities.VisitorsGroup}'");
        writer.WriteLine(sample.Options.Entra is null
            ? "  Identity directory: static in-memory roster - the Access create form fails closed on any id not in it."
            : "  Identity directory: Microsoft Entra (Graph) - the Access picker and validated create use your tenant.");
        writer.WriteLine(sample.Options.GroupMergeMode == SubjectGroupMergeMode.TokenOnly
            ? "  Group-merge mode: TokenOnly - local membership is inert, so Access renders group editing disabled."
            : $"  Group-merge mode: {sample.Options.GroupMergeMode} - set LATTICE_MEMBERSHIP_MERGE_MODE=TokenOnly to see merge-mode gating.");

        writer.WriteLine();
        writer.WriteLine("Seeded");
        foreach (var line in sample.SeedLog)
        {
            writer.WriteLine("  " + line);
        }

        writer.WriteLine();
        writer.WriteLine($"The '{TaskBoardApp.Slug}' app is in Apps > Catalogue; see samples/Explorer/Apps/TaskBoard/README.md.");
        writer.WriteLine("Telemetry is hidden: it needs a Prometheus-compatible metrics backend, which this sample does not run.");
        if (estate)
        {
            writer.WriteLine($"A background writer updates '{SampleIdentities.FactoryFloorTree}' in both regions every {sample.Options.WriterInterval.TotalSeconds:0.#}s.");
            writer.WriteLine(sample.PeerLink.IsPaused
                ? "The peer link is PAUSED (--peer-paused): links go Lagging, then Stalled. Press P to resume it."
                : "Press P to pause the peer link (links go Lagging, then Stalled) and P again to resume it.");
            writer.WriteLine($"Run with {ExplorerSampleOptions.ExplorerRegionSwitch} {SampleIdentities.WestRegion} to point the console at the west region, or {ExplorerSampleOptions.MinimalSwitch} for one region.");
        }

        writer.WriteLine("Press Ctrl+C to stop.");
    }
}
