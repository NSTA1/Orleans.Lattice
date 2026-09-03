using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeReplicationConfigAuthority"/>: the runtime
/// enable/disable authoring seam. They drive the authority against an in-memory
/// config store (mimicking OR-Map convergence via
/// <see cref="LatticeReplicationConfigEntry.MergeFrom"/>) plus substituted
/// context, bootstrap admin, and tree-content probe, so every case is
/// deterministic with no cluster.
/// </summary>
[TestFixture]
public sealed partial class LatticeReplicationConfigAuthorityTests
{
    private const string LocalReplica = "site-a";
    private const string Tree = "orders";

    private static LatticeReplicationConfigAuthority CreateAuthority(
        InMemoryConfigStore store,
        string localReplicaId = LocalReplica,
        ILatticeReplicationAdmin? admin = null,
        ILatticeTreeContentProbe? probe = null,
        IReadOnlyDictionary<string, LatticeMergeMode>? staticTrees = null)
    {
        var context = Substitute.For<ILatticeReplicationContext>();
        context.LocalReplicaId.Returns(localReplicaId);
        context.IsReplicationEnabled.Returns(!string.IsNullOrEmpty(localReplicaId));

        var preconditions = new LatticeReplicationPreconditionValidator(context);

        admin ??= Substitute.For<ILatticeReplicationAdmin>();
        probe ??= FixedProbe(hasContent: false);

        return new LatticeReplicationConfigAuthority(
            store, preconditions, context, admin, probe, Monitor(staticTrees));
    }

    /// <summary>
    /// An options monitor exposing <paramref name="staticTrees"/> as the host's
    /// static deployment-time replicated-tree map, which the authority now
    /// reconciles into its status projections.
    /// </summary>
    private static IOptionsMonitor<LatticeReplicationOptions> Monitor(
        IReadOnlyDictionary<string, LatticeMergeMode>? staticTrees)
    {
        var options = new LatticeReplicationOptions { ClusterId = "x", ReplicatedTrees = staticTrees };
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        monitor.OnChange(Arg.Any<Action<LatticeReplicationOptions, string?>>())
            .Returns(Substitute.For<IDisposable>());
        return monitor;
    }

    private static ILatticeTreeContentProbe FixedProbe(bool hasContent)
    {
        var probe = Substitute.For<ILatticeTreeContentProbe>();
        probe.HasContentAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult(hasContent));
        return probe;
    }

    private static SnapshotLatticeMergeModeResolver ResolverOver(
        InMemoryConfigStore store,
        IReadOnlyDictionary<string, LatticeMergeMode>? staticFallback = null)
    {
        var maintainer = new CompiledReplicationConfigSnapshotMaintainer(
            store, Microsoft.Extensions.Logging.Abstractions.NullLogger<CompiledReplicationConfigSnapshotMaintainer>.Instance);
        maintainer.EnsureWarmAsync().GetAwaiter().GetResult();

        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(new LatticeReplicationOptions
        {
            ClusterId = "x",
            ReplicatedTrees = staticFallback,
        });
        monitor.OnChange(Arg.Any<Action<LatticeReplicationOptions, string?>>())
            .Returns(Substitute.For<IDisposable>());
        var fallback = new ConfiguredLatticeMergeModeResolver(monitor);
        return new SnapshotLatticeMergeModeResolver(maintainer, fallback);
    }

    [Test]
    public async Task EnableReplicationAsync_sets_mode_and_flag_reflected_after_snapshot_rebuild()
    {
        var store = new InMemoryConfigStore();
        var authority = CreateAuthority(store);

        var result = await authority.EnableReplicationAsync(Tree, LatticeMergeMode.OrSet);

        Assert.That(result.AlreadyEnabled, Is.False);
        Assert.That(result.Mode, Is.EqualTo(LatticeMergeMode.OrSet));

        var entry = await store.ReadEntryAsync(Tree);
        Assert.That(entry, Is.Not.Null);
        Assert.That(entry!.IsEnabled, Is.True);
        Assert.That(entry.TryGetMode(out var mode), Is.True);
        Assert.That(mode, Is.EqualTo(LatticeMergeMode.OrSet));

        // A snapshot rebuilt over the store now resolves the enabled tree's mode.
        var resolver = ResolverOver(store);
        Assert.That(resolver.Resolve(Tree), Is.EqualTo(LatticeMergeMode.OrSet));
    }

    [Test]
    public void EnableReplicationAsync_flag_mode_without_replica_id_throws_precondition()
    {
        var store = new InMemoryConfigStore();
        var authority = CreateAuthority(store, localReplicaId: string.Empty);

        Assert.That(
            async () => await authority.EnableReplicationAsync(Tree, LatticeMergeMode.RwFlag),
            Throws.TypeOf<LatticeReplicationPreconditionFailedException>());
    }

    [Test]
    public void EnableReplicationAsync_non_flag_mode_without_replica_id_throws_precondition()
    {
        var store = new InMemoryConfigStore();
        var authority = CreateAuthority(store, localReplicaId: string.Empty);

        // Even a non-flag mode needs a replica id for the config entry's own flag.
        Assert.That(
            async () => await authority.EnableReplicationAsync(Tree, LatticeMergeMode.OrSet),
            Throws.TypeOf<LatticeReplicationPreconditionFailedException>());
    }

    [Test]
    public async Task EnableReplicationAsync_same_mode_is_idempotent()
    {
        var store = new InMemoryConfigStore();
        var authority = CreateAuthority(store);

        await authority.EnableReplicationAsync(Tree, LatticeMergeMode.OrSet);
        var before = (await store.ReadEntryAsync(Tree))!.Clone();

        var result = await authority.EnableReplicationAsync(Tree, LatticeMergeMode.OrSet);

        Assert.That(result.AlreadyEnabled, Is.True);
        Assert.That(result.Mode, Is.EqualTo(LatticeMergeMode.OrSet));
        Assert.That(result.BootstrapRequested, Is.False);

        // No new dot authored: the entry's flag state is unchanged.
        var after = (await store.ReadEntryAsync(Tree))!;
        Assert.That(after.Enabled.Enables.Count, Is.EqualTo(before.Enabled.Enables.Count));
    }

    [Test]
    public async Task EnableReplicationAsync_different_mode_throws_mode_change_rejected()
    {
        var store = new InMemoryConfigStore();
        var authority = CreateAuthority(store);
        await authority.EnableReplicationAsync(Tree, LatticeMergeMode.OrSet);

        var ex = Assert.ThrowsAsync<LatticeReplicationModeChangeRejectedException>(
            async () => await authority.EnableReplicationAsync(Tree, LatticeMergeMode.LwwRegister));

        Assert.That(ex!.CurrentMode, Is.EqualTo(LatticeMergeMode.OrSet));
        Assert.That(ex.RequestedMode, Is.EqualTo(LatticeMergeMode.LwwRegister));
        Assert.That(ex.CurrentModeAmbiguous, Is.False);
    }

    [Test]
    public void EnableReplicationAsync_ambiguous_mode_throws_mode_change_rejected()
    {
        var store = new InMemoryConfigStore();
        store.Seed(Tree, ReplicationConfigSnapshotTestHelpers.AmbiguousEnabled());
        var authority = CreateAuthority(store);

        var ex = Assert.ThrowsAsync<LatticeReplicationModeChangeRejectedException>(
            async () => await authority.EnableReplicationAsync(Tree, LatticeMergeMode.OrSet));

        Assert.That(ex!.CurrentModeAmbiguous, Is.True);
    }

    [Test]
    public async Task DisableReplicationAsync_stops_shipping_without_removing_entry()
    {
        var store = new InMemoryConfigStore();
        var authority = CreateAuthority(store);
        await authority.EnableReplicationAsync(Tree, LatticeMergeMode.OrSet);

        var result = await authority.DisableReplicationAsync(Tree);

        Assert.That(result.AlreadyDisabled, Is.False);

        // The entry is kept, with its fixed mode, but is no longer enabled.
        var entry = await store.ReadEntryAsync(Tree);
        Assert.That(entry, Is.Not.Null);
        Assert.That(entry!.IsEnabled, Is.False);
        Assert.That(entry.TryGetMode(out var mode), Is.True);
        Assert.That(mode, Is.EqualTo(LatticeMergeMode.OrSet));

        // Shipping pauses: a snapshot over the store resolves null (no static
        // fallback), so the receiver applies nothing new.
        var resolver = ResolverOver(store);
        Assert.That(resolver.Resolve(Tree), Is.Null);
    }

    [Test]
    public async Task DisableReplicationAsync_absent_tree_is_idempotent()
    {
        var store = new InMemoryConfigStore();
        var authority = CreateAuthority(store, localReplicaId: string.Empty);

        var result = await authority.DisableReplicationAsync(Tree);

        Assert.That(result.AlreadyDisabled, Is.True);
        Assert.That(await store.ReadEntryAsync(Tree), Is.Null);
    }

    [Test]
    public async Task EnableReplicationAsync_on_non_empty_tree_requests_bootstrap()
    {
        var store = new InMemoryConfigStore();
        var admin = Substitute.For<ILatticeReplicationAdmin>();
        var authority = CreateAuthority(store, admin: admin, probe: FixedProbe(hasContent: true));

        var result = await authority.EnableReplicationAsync(
            Tree, LatticeMergeMode.OrSet, bootstrapSourceClusterId: "site-b");

        Assert.That(result.BootstrapRequested, Is.True);
        await admin.Received(1).RequestSnapshotAsync(Tree, "site-b", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task EnableReplicationAsync_on_empty_tree_does_not_request_bootstrap()
    {
        var store = new InMemoryConfigStore();
        var admin = Substitute.For<ILatticeReplicationAdmin>();
        var authority = CreateAuthority(store, admin: admin, probe: FixedProbe(hasContent: false));

        var result = await authority.EnableReplicationAsync(
            Tree, LatticeMergeMode.OrSet, bootstrapSourceClusterId: "site-b");

        Assert.That(result.BootstrapRequested, Is.False);
        await admin.DidNotReceive().RequestSnapshotAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task EnableReplicationAsync_without_source_does_not_request_bootstrap()
    {
        var store = new InMemoryConfigStore();
        var admin = Substitute.For<ILatticeReplicationAdmin>();
        var authority = CreateAuthority(store, admin: admin, probe: FixedProbe(hasContent: true));

        var result = await authority.EnableReplicationAsync(Tree, LatticeMergeMode.OrSet);

        Assert.That(result.BootstrapRequested, Is.False);
        await admin.DidNotReceive().RequestSnapshotAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetTreeStatusAsync_returns_null_when_absent()
    {
        var store = new InMemoryConfigStore();
        var authority = CreateAuthority(store);

        Assert.That(await authority.GetTreeStatusAsync(Tree), Is.Null);
    }

    [Test]
    public async Task GetTreeStatusAsync_reports_enabled_mode_and_ambiguity()
    {
        var store = new InMemoryConfigStore();
        var authority = CreateAuthority(store);
        await authority.EnableReplicationAsync(Tree, LatticeMergeMode.OrSet);

        var status = await authority.GetTreeStatusAsync(Tree);

        Assert.That(status, Is.Not.Null);
        Assert.That(status!.Value.Enabled, Is.True);
        Assert.That(status.Value.Mode, Is.EqualTo(LatticeMergeMode.OrSet));
        Assert.That(status.Value.Ambiguous, Is.False);
    }

    /// <summary>
    /// A minimal in-memory <see cref="ILatticeReplicationConfigStore"/> that
    /// converges writes through <see cref="LatticeReplicationConfigEntry.MergeFrom"/>
    /// exactly as the real OR-Map would, so authoring read-modify-write cycles
    /// behave the same without a cluster.
    /// </summary>
    private sealed class InMemoryConfigStore : ILatticeReplicationConfigStore
    {
        private readonly Dictionary<string, LatticeReplicationConfigEntry> _entries =
            new(StringComparer.Ordinal);

        public void Seed(string treeId, LatticeReplicationConfigEntry entry) => _entries[treeId] = entry;

        public Task<IReadOnlyDictionary<string, LatticeReplicationConfigEntry>> ReadEntriesAsync(
            CancellationToken cancellationToken = default)
        {
            var snapshot = new Dictionary<string, LatticeReplicationConfigEntry>(StringComparer.Ordinal);
            foreach (var pair in _entries)
            {
                snapshot[pair.Key] = pair.Value.Clone();
            }

            return Task.FromResult<IReadOnlyDictionary<string, LatticeReplicationConfigEntry>>(snapshot);
        }

        public Task<LatticeReplicationConfigEntry?> ReadEntryAsync(
            string treeId,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(_entries.TryGetValue(treeId, out var entry) ? entry.Clone() : null);

        public Task WriteEntryAsync(
            string treeId,
            string replicaId,
            LatticeReplicationConfigEntry entry,
            CancellationToken cancellationToken = default)
        {
            if (_entries.TryGetValue(treeId, out var existing))
            {
                existing.MergeFrom(entry);
            }
            else
            {
                _entries[treeId] = entry.Clone();
            }

            return Task.CompletedTask;
        }
    }
}
