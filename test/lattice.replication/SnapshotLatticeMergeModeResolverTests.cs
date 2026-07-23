using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit tests for <see cref="SnapshotLatticeMergeModeResolver"/>: snapshot-first
/// precedence, fallback to the options-backed
/// <see cref="ConfiguredLatticeMergeModeResolver"/>, and the fail-closed
/// <see langword="null"/> for an ambiguous runtime mode.
/// </summary>
[TestFixture]
public sealed class SnapshotLatticeMergeModeResolverTests
{
    private static ConfiguredLatticeMergeModeResolver Fallback(
        IReadOnlyDictionary<string, LatticeMergeMode>? staticTrees)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(new LatticeReplicationOptions
        {
            ClusterId = "x",
            ReplicatedTrees = staticTrees,
        });
        monitor.OnChange(Arg.Any<Action<LatticeReplicationOptions, string?>>())
            .Returns(Substitute.For<IDisposable>());
        return new ConfiguredLatticeMergeModeResolver(monitor);
    }

    [Test]
    public async Task Resolve_returns_the_snapshot_mode_when_enabled_and_unambiguous()
    {
        var maintainer = await ReplicationConfigSnapshotTestHelpers.WarmMaintainerAsync(
            new Dictionary<string, LatticeReplicationConfigEntry>
            {
                ["orders"] = ReplicationConfigSnapshotTestHelpers.Enabled(LatticeMergeMode.OrSet),
            });
        // Static seed says LWW; snapshot must win.
        using var fallback = Fallback(new Dictionary<string, LatticeMergeMode>
        {
            ["orders"] = LatticeMergeMode.LwwRegister,
        });
        var resolver = new SnapshotLatticeMergeModeResolver(maintainer, fallback);

        Assert.That(resolver.Resolve("orders"), Is.EqualTo(LatticeMergeMode.OrSet));
    }

    [Test]
    public async Task Resolve_falls_back_to_options_when_tree_absent_from_snapshot()
    {
        var maintainer = await ReplicationConfigSnapshotTestHelpers.WarmMaintainerAsync(
            new Dictionary<string, LatticeReplicationConfigEntry>());
        using var fallback = Fallback(new Dictionary<string, LatticeMergeMode>
        {
            ["orders"] = LatticeMergeMode.LwwRegister,
        });
        var resolver = new SnapshotLatticeMergeModeResolver(maintainer, fallback);

        Assert.That(resolver.Resolve("orders"), Is.EqualTo(LatticeMergeMode.LwwRegister));
    }

    [Test]
    public async Task Resolve_falls_back_when_tree_present_but_not_enabled()
    {
        var maintainer = await ReplicationConfigSnapshotTestHelpers.WarmMaintainerAsync(
            new Dictionary<string, LatticeReplicationConfigEntry>
            {
                ["orders"] = ReplicationConfigSnapshotTestHelpers.DisabledWithMode(LatticeMergeMode.OrSet),
            });
        using var fallback = Fallback(new Dictionary<string, LatticeMergeMode>
        {
            ["orders"] = LatticeMergeMode.LwwRegister,
        });
        var resolver = new SnapshotLatticeMergeModeResolver(maintainer, fallback);

        Assert.That(resolver.Resolve("orders"), Is.EqualTo(LatticeMergeMode.LwwRegister));
    }

    [Test]
    public async Task Resolve_returns_null_when_snapshot_mode_is_ambiguous()
    {
        var maintainer = await ReplicationConfigSnapshotTestHelpers.WarmMaintainerAsync(
            new Dictionary<string, LatticeReplicationConfigEntry>
            {
                ["orders"] = ReplicationConfigSnapshotTestHelpers.AmbiguousEnabled(),
            });
        // Even though the static seed offers a mode, ambiguity must fail closed.
        using var fallback = Fallback(new Dictionary<string, LatticeMergeMode>
        {
            ["orders"] = LatticeMergeMode.LwwRegister,
        });
        var resolver = new SnapshotLatticeMergeModeResolver(maintainer, fallback);

        Assert.That(resolver.Resolve("orders"), Is.Null,
            "an ambiguous runtime mode must pause shipping, never silently pick a mode");
    }

    [Test]
    public async Task Resolve_returns_null_when_neither_source_configures_the_tree()
    {
        var maintainer = await ReplicationConfigSnapshotTestHelpers.WarmMaintainerAsync(
            new Dictionary<string, LatticeReplicationConfigEntry>());
        using var fallback = Fallback(null);
        var resolver = new SnapshotLatticeMergeModeResolver(maintainer, fallback);

        Assert.That(resolver.Resolve("orders"), Is.Null);
    }

    [Test]
    public async Task Resolve_throws_on_null_tree_id()
    {
        var maintainer = await ReplicationConfigSnapshotTestHelpers.WarmMaintainerAsync(
            new Dictionary<string, LatticeReplicationConfigEntry>());
        using var fallback = Fallback(null);
        var resolver = new SnapshotLatticeMergeModeResolver(maintainer, fallback);

        Assert.That(() => resolver.Resolve(null!), Throws.ArgumentNullException);
    }
}
