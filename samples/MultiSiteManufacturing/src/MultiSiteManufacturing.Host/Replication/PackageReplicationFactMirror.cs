using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Federation;
using MultiSiteManufacturing.Host.Lattice;
using Orleans.Lattice;

namespace MultiSiteManufacturing.Host.Replication;

/// <summary>
/// Migration-step-1 fact mirror: subscribes to
/// <see cref="FederationRouter.FactRouted"/> and writes the same fact
/// payload into a brand-new lattice tree (<see cref="MirrorTreeId"/>)
/// that <c>Orleans.Lattice.Replication</c> ships across clusters.
/// </summary>
/// <remarks>
/// <para>
/// The host-rolled replication pipeline ships from <c>mfg-facts</c>;
/// the package ships from <c>mfg-facts-v2</c>. Mirroring every fact
/// into both trees lets the two pipelines run side by side under the
/// same workload so the package's
/// <c>orleans.lattice.replication</c> meter starts emitting and the
/// shipped Grafana <b>Replication</b> dashboard renders real data
/// without disturbing any of the existing tests, dashboard panels,
/// or chaos tiers that target the host-rolled <c>_replog__mfg-facts</c>.
/// </para>
/// <para>
/// This service is deleted at migration step 2 (cut <c>mfg-facts</c>
/// over to the package and remove the host-rolled outbound for that
/// tree). See <c>../../migration.md</c>.
/// </para>
/// <para>
/// Errors writing to <see cref="MirrorTreeId"/> are logged at
/// Warning and swallowed: the mirror is a passive observer of the
/// canonical fact stream, and a transient lattice / storage failure
/// must not propagate to the operator UI or the gRPC ingress path.
/// </para>
/// </remarks>
internal sealed class PackageReplicationFactMirror(
    FederationRouter router,
    IGrainFactory grains,
    ILogger<PackageReplicationFactMirror> logger) : IHostedService
{
    /// <summary>
    /// Lattice tree id observed by the package
    /// (<c>Orleans.Lattice.Replication</c>) under
    /// <see cref="ReplicationMode.LwwRegister"/>. Disjoint from
    /// every tree declared in <c>ReplicationTopology.ReplicatedTrees</c>
    /// so the host-rolled pipeline never appends to its replog for
    /// these writes.
    /// </summary>
    public const string MirrorTreeId = "mfg-facts-v2";

    public Task StartAsync(CancellationToken cancellationToken)
    {
        router.FactRouted += OnFactRouted;
        logger.LogInformation(
            "PackageReplicationFactMirror started; mirroring FederationRouter.FactRouted to lattice tree {Tree}",
            MirrorTreeId);
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        router.FactRouted -= OnFactRouted;
        return Task.CompletedTask;
    }

    private void OnFactRouted(object? sender, Fact fact)
    {
        // Fire-and-forget: the FactRouted handler chain is synchronous
        // and must not block the router's fan-out. The grain call is
        // routed through Orleans which already handles per-grain
        // serialisation; concurrent FactRouted callbacks for distinct
        // facts are independent activations on the destination tree
        // grain.
        _ = MirrorAsync(fact);
    }

    private async Task MirrorAsync(Fact fact)
    {
        try
        {
            var key = KeyFor(fact);
            var payload = FactJsonCodec.Encode(fact);
            var tree = grains.GetGrain<ILattice>(MirrorTreeId);
            await tree.SetAsync(key, payload).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex,
                "PackageReplicationFactMirror: failed to mirror fact {FactId} (serial {Serial}) into {Tree}",
                fact.FactId, fact.Serial.Value, MirrorTreeId);
        }
    }

    /// <summary>
    /// Same lex-ordered key shape <see cref="LatticeFactBackend"/>
    /// uses on <c>mfg-facts</c> so a future operator inspecting both
    /// trees side by side can compare entries by key directly.
    /// </summary>
    private static string KeyFor(Fact fact) =>
        string.Create(System.Globalization.CultureInfo.InvariantCulture,
            $"{fact.Serial.Value}/{fact.Hlc.WallClockTicks:D20}/{fact.Hlc.Counter:D10}/{fact.FactId:N}");
}
