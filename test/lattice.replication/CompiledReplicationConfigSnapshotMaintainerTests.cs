using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit tests for <see cref="CompiledReplicationConfigSnapshotMaintainer"/> and
/// the <see cref="CompiledReplicationConfig"/> projection it produces, over an
/// in-memory fake config store (no cluster). Covers the warm-up build, the
/// monotonic epoch, change-feed-driven rebuilds filtered to the reserved config
/// tree, the atomic snapshot swap, and the per-tree
/// enabled/disabled/ambiguous/absent projection.
/// </summary>
[TestFixture]
public sealed class CompiledReplicationConfigSnapshotMaintainerTests
{
    private static CompiledReplicationConfigSnapshotMaintainer CreateMaintainer(FakeConfigStore store) =>
        new(store, NullLogger<CompiledReplicationConfigSnapshotMaintainer>.Instance);

    private static LatticeReplicationConfigEntry Enabled(LatticeMergeMode mode)
    {
        var entry = new LatticeReplicationConfigEntry();
        entry.Enable("site-a", 1);
        entry.SetMode("site-a", mode);
        return entry;
    }

    private static LatticeReplicationConfigEntry DisabledWithMode(LatticeMergeMode mode)
    {
        var entry = new LatticeReplicationConfigEntry();
        entry.SetMode("site-a", mode);
        // No enable dot minted, so the RwFlag is not enabled.
        return entry;
    }

    private static LatticeReplicationConfigEntry AmbiguousEnabled()
    {
        var a = new LatticeReplicationConfigEntry();
        a.Enable("site-a", 1);
        a.SetMode("site-a", LatticeMergeMode.LwwRegister);

        var b = new LatticeReplicationConfigEntry();
        b.Enable("site-b", 1);
        b.SetMode("site-b", LatticeMergeMode.OrSet);

        // Concurrent divergent mode assignments from two replicas that never
        // observed one another survive the merge as two live values.
        a.MergeFrom(b);
        return a;
    }

    private static async Task<bool> WaitForEpochAtLeast(
        CompiledReplicationConfigSnapshotMaintainer maintainer, long target, int timeoutMs = 5000)
    {
        var start = Environment.TickCount64;
        while (Environment.TickCount64 - start < timeoutMs)
        {
            if (maintainer.CurrentEpoch >= target)
            {
                return true;
            }

            await Task.Delay(20);
        }

        return maintainer.CurrentEpoch >= target;
    }

    [Test]
    public void Fresh_maintainer_starts_at_epoch_zero_with_empty_snapshot()
    {
        var maintainer = CreateMaintainer(new FakeConfigStore());

        Assert.That(maintainer.CurrentEpoch, Is.EqualTo(0));
        Assert.That(maintainer.Current.TreeCount, Is.EqualTo(0));
    }

    [Test]
    public async Task EnsureWarmAsync_builds_the_snapshot_and_advances_the_epoch_once()
    {
        var store = new FakeConfigStore();
        store.Entries["orders"] = Enabled(LatticeMergeMode.LwwRegister);
        var maintainer = CreateMaintainer(store);

        await maintainer.EnsureWarmAsync();

        Assert.That(maintainer.CurrentEpoch, Is.EqualTo(1));
        Assert.That(maintainer.Current.TryGetTree("orders", out _), Is.True);

        // Second warm is a no-op (idempotent) once warm.
        await maintainer.EnsureWarmAsync();
        Assert.That(maintainer.CurrentEpoch, Is.EqualTo(1));
    }

    [Test]
    public async Task Compile_projects_enabled_tree_with_its_mode()
    {
        var store = new FakeConfigStore();
        store.Entries["orders"] = Enabled(LatticeMergeMode.OrSet);
        var maintainer = CreateMaintainer(store);

        await maintainer.EnsureWarmAsync();

        Assert.That(maintainer.Current.TryGetTree("orders", out var projection), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(projection.Enabled, Is.True);
            Assert.That(projection.Ambiguous, Is.False);
            Assert.That(projection.Mode, Is.EqualTo(LatticeMergeMode.OrSet));
        });
        Assert.That(maintainer.Current.EnabledTrees, Does.Contain("orders"));
    }

    [Test]
    public async Task Compile_projects_disabled_tree_as_not_enabled_but_keeps_mode()
    {
        var store = new FakeConfigStore();
        store.Entries["orders"] = DisabledWithMode(LatticeMergeMode.LwwRegister);
        var maintainer = CreateMaintainer(store);

        await maintainer.EnsureWarmAsync();

        Assert.That(maintainer.Current.TryGetTree("orders", out var projection), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(projection.Enabled, Is.False);
            Assert.That(projection.Ambiguous, Is.False);
            Assert.That(projection.Mode, Is.EqualTo(LatticeMergeMode.LwwRegister));
        });
        Assert.That(maintainer.Current.EnabledTrees, Does.Not.Contain("orders"));
    }

    [Test]
    public async Task Compile_projects_ambiguous_tree_with_null_mode_and_flag_set()
    {
        var store = new FakeConfigStore();
        store.Entries["orders"] = AmbiguousEnabled();
        var maintainer = CreateMaintainer(store);

        await maintainer.EnsureWarmAsync();

        Assert.That(maintainer.Current.TryGetTree("orders", out var projection), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(projection.Ambiguous, Is.True, "two live modes must project as ambiguous");
            Assert.That(projection.Mode, Is.Null, "an ambiguous mode must never resolve to a single value");
            Assert.That(projection.Enabled, Is.True);
        });
    }

    [Test]
    public async Task Compile_reports_absent_tree_as_not_configured()
    {
        var store = new FakeConfigStore();
        store.Entries["orders"] = Enabled(LatticeMergeMode.LwwRegister);
        var maintainer = CreateMaintainer(store);

        await maintainer.EnsureWarmAsync();

        Assert.That(maintainer.Current.TryGetTree("inventory", out _), Is.False);
    }

    [Test]
    public async Task OnMutationAsync_on_the_config_tree_rebuilds_and_advances_the_epoch()
    {
        var store = new FakeConfigStore();
        var maintainer = CreateMaintainer(store);
        await maintainer.EnsureWarmAsync();
        var epochBefore = maintainer.CurrentEpoch;

        store.Entries["orders"] = Enabled(LatticeMergeMode.LwwRegister);
        await maintainer.OnMutationAsync(
            new LatticeMutation { TreeId = LatticeSystemTreeNames.ReplicationConfig }, CancellationToken.None);

        Assert.That(await WaitForEpochAtLeast(maintainer, epochBefore + 1), Is.True,
            "a config-tree mutation must trigger a rebuild");
        Assert.That(maintainer.Current.TryGetTree("orders", out _), Is.True,
            "the rebuilt snapshot must reflect the new entry");
    }

    [Test]
    public async Task OnMutationAsync_on_an_unrelated_tree_does_not_rebuild()
    {
        var store = new FakeConfigStore();
        var maintainer = CreateMaintainer(store);
        await maintainer.EnsureWarmAsync();
        var epochBefore = maintainer.CurrentEpoch;

        await maintainer.OnMutationAsync(
            new LatticeMutation { TreeId = "some-app-tree" }, CancellationToken.None);
        await Task.Delay(200);

        Assert.That(maintainer.CurrentEpoch, Is.EqualTo(epochBefore),
            "an unrelated tree mutation must not rebuild the config snapshot");
    }

    [Test]
    public async Task RebuildNowAsync_reflects_the_latest_store_state()
    {
        var store = new FakeConfigStore();
        var maintainer = CreateMaintainer(store);
        await maintainer.RebuildNowAsync();
        Assert.That(maintainer.Current.TreeCount, Is.EqualTo(0));

        store.Entries["orders"] = Enabled(LatticeMergeMode.LwwRegister);
        var epoch = await maintainer.RebuildNowAsync();

        Assert.That(epoch, Is.EqualTo(2));
        Assert.That(maintainer.Current.TryGetTree("orders", out _), Is.True);
    }

    [Test]
    public async Task EnsureWarmStarted_kicks_a_background_rebuild_once_when_cold()
    {
        var store = new FakeConfigStore();
        store.Entries["orders"] = Enabled(LatticeMergeMode.LwwRegister);
        var maintainer = CreateMaintainer(store);

        Assert.That(maintainer.CurrentEpoch, Is.EqualTo(0));

        maintainer.EnsureWarmStarted();

        Assert.That(await WaitForEpochAtLeast(maintainer, 1), Is.True,
            "the cold-start warm-up must build the snapshot in the background");
        Assert.That(maintainer.Current.TryGetTree("orders", out _), Is.True);
    }

    /// <summary>
    /// A minimal in-memory <see cref="ILatticeReplicationConfigStore"/> whose
    /// backing dictionary a test edits between rebuilds.
    /// </summary>
    private sealed class FakeConfigStore : ILatticeReplicationConfigStore
    {
        public Dictionary<string, LatticeReplicationConfigEntry> Entries { get; } =
            new(StringComparer.Ordinal);

        public Task<IReadOnlyDictionary<string, LatticeReplicationConfigEntry>> ReadEntriesAsync(
            CancellationToken cancellationToken = default)
        {
            // Snapshot to avoid mutation-during-enumeration when a test edits the
            // dictionary between rebuilds.
            var copy = new Dictionary<string, LatticeReplicationConfigEntry>(Entries, StringComparer.Ordinal);
            return Task.FromResult<IReadOnlyDictionary<string, LatticeReplicationConfigEntry>>(copy);
        }
    }
}
