using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Unit tests for the per-tree runtime <see cref="LatticeOptions.WalMaxRetainedBytes"/>
/// override folded into <see cref="LatticeOptionsResolver"/> (both the full
/// <see cref="LatticeOptionsResolver.ResolveAsync(string)"/> record and the
/// lightweight <see cref="LatticeOptionsResolver.GetWalMaxRetainedBytesAsync(string)"/>
/// fast path the WAL garbage collector drives on every pass). The override on
/// <see cref="TreeRegistryEntry.WalMaxRetainedBytes"/> wins when pinned; an
/// absent override falls back to the silo-wide static option byte-for-byte,
/// exactly mirroring the neighbouring
/// <see cref="TreeRegistryEntry.MaxCacheValueBytes"/> precedence (issue #3333).
/// </summary>
[TestFixture]
public class LatticeOptionsResolverWalRetainedBytesTests
{
    [SetUp]
    public void Setup()
    {
        // Keep the shared "warned-latched-trees" memo clean between tests so a
        // resolver constructed here is independent of sibling fixtures.
        LatticeOptionsResolver.ResetWarnedLatchedTreesForTests();
    }

    private static (LatticeOptionsResolver Resolver, ILatticeRegistry Registry) Build(
        LatticeOptions? options = null)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options ?? new LatticeOptions());

        var factory = Substitute.For<IGrainFactory>();
        var registry = Substitute.For<ILatticeRegistry>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);

        // Default to a structurally-pinned entry with NO ceiling override so the
        // fall-back branch is the baseline. Individual tests overwrite the
        // response to pin an override.
        registry.GetEntryAsync(Arg.Any<string>()).Returns(_ => Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry
            {
                MaxLeafKeys = LatticeConstants.DefaultMaxLeafKeys,
                MaxInternalChildren = LatticeConstants.DefaultMaxInternalChildren,
                ShardCount = LatticeConstants.DefaultShardCount,
            }));

        return (new LatticeOptionsResolver(factory, monitor), registry);
    }

    private static TreeRegistryEntry EntryWithOverride(long? walMaxRetainedBytes) =>
        new()
        {
            MaxLeafKeys = LatticeConstants.DefaultMaxLeafKeys,
            MaxInternalChildren = LatticeConstants.DefaultMaxInternalChildren,
            ShardCount = LatticeConstants.DefaultShardCount,
            WalMaxRetainedBytes = walMaxRetainedBytes,
        };

    // ---- ResolveAsync (full record) ----

    [Test]
    public async Task ResolveAsync_no_override_null_static_resolves_null()
    {
        // Byte-for-byte baseline: the default leaves the advisory byte-pressure
        // policy disabled (null), so the resolved value must remain null.
        var (resolver, _) = Build(new LatticeOptions { WalMaxRetainedBytes = null });

        var resolved = await resolver.ResolveAsync("user-tree");

        Assert.That(resolved.WalMaxRetainedBytes, Is.Null);
    }

    [Test]
    public async Task ResolveAsync_no_override_falls_back_to_static_value()
    {
        var (resolver, _) = Build(new LatticeOptions { WalMaxRetainedBytes = 8589934592 });

        var resolved = await resolver.ResolveAsync("user-tree");

        Assert.That(resolved.WalMaxRetainedBytes, Is.EqualTo(8589934592),
            "With no per-tree override pinned, the resolved ceiling must equal the static option exactly.");
    }

    [Test]
    public async Task ResolveAsync_override_wins_over_static_value()
    {
        var (resolver, registry) = Build(new LatticeOptions { WalMaxRetainedBytes = 8589934592 });
        registry.GetEntryAsync("user-tree").Returns(_ => Task.FromResult<TreeRegistryEntry?>(
            EntryWithOverride(21474836480)));

        var resolved = await resolver.ResolveAsync("user-tree");

        Assert.That(resolved.WalMaxRetainedBytes, Is.EqualTo(21474836480),
            "A per-tree runtime override must win over the silo-wide static option.");
    }

    [Test]
    public async Task ResolveAsync_override_wins_even_when_static_is_null()
    {
        var (resolver, registry) = Build(new LatticeOptions { WalMaxRetainedBytes = null });
        registry.GetEntryAsync("user-tree").Returns(_ => Task.FromResult<TreeRegistryEntry?>(
            EntryWithOverride(1048576)));

        var resolved = await resolver.ResolveAsync("user-tree");

        Assert.That(resolved.WalMaxRetainedBytes, Is.EqualTo(1048576),
            "An override arms the advisory policy on a tree the static option leaves unbounded.");
    }

    [Test]
    public async Task ResolveAsync_null_override_falls_back_to_static_value()
    {
        var (resolver, registry) = Build(new LatticeOptions { WalMaxRetainedBytes = 2048 });
        registry.GetEntryAsync("user-tree").Returns(_ => Task.FromResult<TreeRegistryEntry?>(
            EntryWithOverride(null)));

        var resolved = await resolver.ResolveAsync("user-tree");

        Assert.That(resolved.WalMaxRetainedBytes, Is.EqualTo(2048),
            "A cleared (null) override must fall back to the static option, not to policy-disabled.");
    }

    [Test]
    public async Task ResolveAsync_system_tree_uses_static_option_and_bypasses_registry()
    {
        var (resolver, registry) = Build(new LatticeOptions { WalMaxRetainedBytes = 777 });

        var resolved = await resolver.ResolveAsync(LatticeConstants.SystemTreePrefix + "trees");

        Assert.That(resolved.WalMaxRetainedBytes, Is.EqualTo(777),
            "System trees carry no registry entry; their ceiling resolves to the static option.");
        await registry.DidNotReceive().GetEntryAsync(Arg.Any<string>());
    }

    // ---- GetWalMaxRetainedBytesAsync (fast path) ----

    [Test]
    public async Task GetWalMaxRetainedBytesAsync_no_override_falls_back_to_static_value()
    {
        var (resolver, _) = Build(new LatticeOptions { WalMaxRetainedBytes = 8589934592 });

        var value = await resolver.GetWalMaxRetainedBytesAsync("user-tree");

        Assert.That(value, Is.EqualTo(8589934592));
    }

    [Test]
    public async Task GetWalMaxRetainedBytesAsync_no_override_null_static_resolves_null()
    {
        var (resolver, _) = Build(new LatticeOptions { WalMaxRetainedBytes = null });

        var value = await resolver.GetWalMaxRetainedBytesAsync("user-tree");

        Assert.That(value, Is.Null);
    }

    [Test]
    public async Task GetWalMaxRetainedBytesAsync_override_wins_over_static_value()
    {
        var (resolver, registry) = Build(new LatticeOptions { WalMaxRetainedBytes = 8589934592 });
        registry.GetEntryAsync("user-tree").Returns(_ => Task.FromResult<TreeRegistryEntry?>(
            EntryWithOverride(21474836480)));

        var value = await resolver.GetWalMaxRetainedBytesAsync("user-tree");

        Assert.That(value, Is.EqualTo(21474836480));
    }

    [Test]
    public async Task GetWalMaxRetainedBytesAsync_null_override_falls_back_to_static_value()
    {
        var (resolver, registry) = Build(new LatticeOptions { WalMaxRetainedBytes = 2048 });
        registry.GetEntryAsync("user-tree").Returns(_ => Task.FromResult<TreeRegistryEntry?>(
            EntryWithOverride(null)));

        var value = await resolver.GetWalMaxRetainedBytesAsync("user-tree");

        Assert.That(value, Is.EqualTo(2048));
    }

    [Test]
    public async Task GetWalMaxRetainedBytesAsync_system_tree_uses_static_and_bypasses_registry()
    {
        var (resolver, registry) = Build(new LatticeOptions { WalMaxRetainedBytes = 777 });

        var value = await resolver.GetWalMaxRetainedBytesAsync(LatticeConstants.SystemTreePrefix + "trees");

        Assert.That(value, Is.EqualTo(777));
        await registry.DidNotReceive().GetEntryAsync(Arg.Any<string>());
    }

    [Test]
    public void GetWalMaxRetainedBytesAsync_throws_on_null_treeId()
    {
        var (resolver, _) = Build();

        Assert.ThrowsAsync<ArgumentNullException>(
            async () => await resolver.GetWalMaxRetainedBytesAsync(null!));
    }

    [Test]
    public async Task ResolveAsync_and_fast_path_agree_on_the_resolved_ceiling()
    {
        // The two seams must never disagree: the WAL garbage collector reads the
        // cheap fast path and must see the same ceiling the full record reports.
        var (resolver, registry) = Build(new LatticeOptions { WalMaxRetainedBytes = 8589934592 });
        registry.GetEntryAsync("user-tree").Returns(_ => Task.FromResult<TreeRegistryEntry?>(
            EntryWithOverride(333)));

        var resolved = await resolver.ResolveAsync("user-tree");
        var fast = await resolver.GetWalMaxRetainedBytesAsync("user-tree");

        Assert.That(fast, Is.EqualTo(resolved.WalMaxRetainedBytes));
        Assert.That(fast, Is.EqualTo(333));
    }

    [Test]
    public async Task GetWalMaxRetainedBytesAsync_observes_a_later_override_change_without_restart()
    {
        // THE load-bearing property of issue #3333, and the one a memoising
        // implementation would silently break: the fast path must read the
        // registry fresh on every call, so a ceiling changed at runtime is
        // observed by the next WAL garbage-collection pass rather than at the
        // next silo start. A cache here would make every assertion above still
        // pass while restoring the restart requirement the feature removes.
        var (resolver, registry) = Build(new LatticeOptions { WalMaxRetainedBytes = 8589934592 });
        registry.GetEntryAsync("user-tree").Returns(_ => Task.FromResult<TreeRegistryEntry?>(
            EntryWithOverride(8589934592)));

        var before = await resolver.GetWalMaxRetainedBytesAsync("user-tree");
        Assert.That(before, Is.EqualTo(8589934592));

        registry.GetEntryAsync("user-tree").Returns(_ => Task.FromResult<TreeRegistryEntry?>(
            EntryWithOverride(21474836480)));

        var after = await resolver.GetWalMaxRetainedBytesAsync("user-tree");

        Assert.That(after, Is.EqualTo(21474836480),
            "The ceiling must never be memoised: a runtime override change has to be visible to the "
            + "very next resolve, otherwise correcting a stale ceiling still costs a silo restart.");
    }
}
