using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Regression coverage for the sender-side enrollment gate on the
/// cross-cluster bootstrap export path.
/// <para>
/// The gate is the mirror of the receiver-side classification
/// <c>ReplicationApplier.ClassifyInboundTree</c> already performs. Before it
/// existed, <see cref="LatticeRemoteSnapshotService.GetMetadataAsync"/> and
/// <see cref="LatticeRemoteSnapshotService.RequestSnapshotAsync"/> passed the
/// peer-supplied tree name straight into
/// <see cref="ISnapshotProvider.ExportAsync(string, HybridLogicalClock, CancellationToken)"/>,
/// so any peer that cleared the shared-secret interceptor could stream out an
/// arbitrary tree on the silo - including the <c>sys-</c>-prefixed
/// authorization and identity trees, and other tenants' trees, that a cluster
/// deliberately keeps local by never enrolling them for replication.
/// </para>
/// <para>
/// Each test asserts both halves of the contract: the refusal itself, and that
/// the provider was <b>never called</b>, since a gate that refuses only after
/// the export has begun still leaks the cut-point metadata.
/// </para>
/// </summary>
[TestFixture]
public class LatticeRemoteSnapshotServiceEnrollmentTests
{
    private const string EnrolledTree = "rsee-enrolled";
    private const string UnenrolledTree = "rsee-not-enrolled";
    private const string SystemTree = "sys-auth-policies";
    private const string Source = "site-a";

    /// <summary>
    /// Dictionary-backed <see cref="ILatticeReplicationContext"/> reporting a
    /// merge mode for exactly the enrolled trees, mirroring what the production
    /// <c>ConfiguredLatticeReplicationContext</c> resolves for a cluster that
    /// enrolled only a subset of its trees.
    /// </summary>
    private sealed class MapReplicationContext(IReadOnlyDictionary<string, LatticeMergeMode> modes)
        : ILatticeReplicationContext
    {
        public bool IsReplicationEnabled => true;

        public string LocalReplicaId => Source;

        public LatticeMergeMode? ResolveMergeMode(string treeId) =>
            modes.TryGetValue(treeId, out var mode) ? mode : null;
    }

    private static ILatticeReplicationContext EnrolledOnly() =>
        new MapReplicationContext(new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal)
        {
            [EnrolledTree] = LatticeMergeMode.LwwRegister,
        });

    private static async IAsyncEnumerable<SnapshotEntry> EmptyEntries()
    {
        await Task.CompletedTask;
        yield break;
    }

    private static ISnapshotProvider ProviderReturningEmptyStream(string treeName)
    {
        var provider = Substitute.For<ISnapshotProvider>();
        provider
            .ExportAsync(Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new SnapshotStream(
                treeName,
                new HybridLogicalClock { WallClockTicks = 100, Counter = 0 },
                new VersionVector(),
                EmptyEntries())));
        return provider;
    }

    private static async Task<int> DrainAsync(IAsyncEnumerable<SnapshotEntry> entries)
    {
        var count = 0;
        await foreach (var _ in entries.ConfigureAwait(false))
        {
            count++;
        }

        return count;
    }

    [Test]
    public void GetMetadataAsync_refuses_a_tree_that_is_not_enrolled_for_replication()
    {
        var provider = ProviderReturningEmptyStream(UnenrolledTree);
        var service = new LatticeRemoteSnapshotService(
            provider,
            EnrolledOnly(),
            NullLogger<LatticeRemoteSnapshotService>.Instance);

        Assert.That(
            () => service.GetMetadataAsync(UnenrolledTree, Source, HybridLogicalClock.Zero),
            Throws.InstanceOf<UnauthorizedAccessException>());

        provider.DidNotReceive().ExportAsync(
            Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void RequestSnapshotAsync_refuses_a_tree_that_is_not_enrolled_for_replication()
    {
        var provider = ProviderReturningEmptyStream(UnenrolledTree);
        var service = new LatticeRemoteSnapshotService(
            provider,
            EnrolledOnly(),
            NullLogger<LatticeRemoteSnapshotService>.Instance);

        Assert.That(
            async () => await DrainAsync(
                service.RequestSnapshotAsync(UnenrolledTree, Source, HybridLogicalClock.Zero)),
            Throws.InstanceOf<UnauthorizedAccessException>());

        provider.DidNotReceive().ExportAsync(
            Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void RequestSnapshotAsync_refuses_a_system_tree_the_cluster_never_enrolled()
    {
        // The sys- prefixed authorization and identity trees are the highest
        // value target of the gap this gate closes: they are never enrolled for
        // replication, so the enrollment resolver reports no mode for them.
        var provider = ProviderReturningEmptyStream(SystemTree);
        var service = new LatticeRemoteSnapshotService(
            provider,
            EnrolledOnly(),
            NullLogger<LatticeRemoteSnapshotService>.Instance);

        Assert.That(
            async () => await DrainAsync(
                service.RequestSnapshotAsync(SystemTree, Source, HybridLogicalClock.Zero)),
            Throws.InstanceOf<UnauthorizedAccessException>());

        provider.DidNotReceive().ExportAsync(
            Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task An_enrolled_tree_still_exports_normally()
    {
        var provider = ProviderReturningEmptyStream(EnrolledTree);
        var service = new LatticeRemoteSnapshotService(
            provider,
            EnrolledOnly(),
            NullLogger<LatticeRemoteSnapshotService>.Instance);

        var metadata = await service.GetMetadataAsync(EnrolledTree, Source, HybridLogicalClock.Zero);

        Assert.Multiple(() =>
        {
            Assert.That(metadata.TreeName, Is.EqualTo(EnrolledTree));
            Assert.That(metadata.SourceClusterId, Is.EqualTo(Source));
            Assert.That(metadata.AsOfHlc.WallClockTicks, Is.EqualTo(100));
        });

        var drained = await DrainAsync(
            service.RequestSnapshotAsync(EnrolledTree, Source, HybridLogicalClock.Zero));

        Assert.That(drained, Is.Zero);
    }

    [Test]
    public void A_service_built_without_an_enrollment_source_fails_closed()
    {
        // Fail-closed on ambiguity: with no ILatticeReplicationContext the gate
        // has no enrollment signal at all, so it denies rather than falling
        // through to allow - the same arm ReplicationApplier takes as
        // InboundTreeAdmission.RejectNoEnrollmentSource.
        var provider = ProviderReturningEmptyStream(EnrolledTree);
        var service = new LatticeRemoteSnapshotService(
            provider,
            NullLogger<LatticeRemoteSnapshotService>.Instance);

        Assert.That(
            () => service.GetMetadataAsync(EnrolledTree, Source, HybridLogicalClock.Zero),
            Throws.InstanceOf<UnauthorizedAccessException>());

        provider.DidNotReceive().ExportAsync(
            Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void Constructor_throws_when_the_replication_context_is_null()
    {
        Assert.That(
            () => new LatticeRemoteSnapshotService(
                Substitute.For<ISnapshotProvider>(),
                (ILatticeReplicationContext)null!,
                NullLogger<LatticeRemoteSnapshotService>.Instance),
            Throws.InstanceOf<ArgumentNullException>());
    }
}
