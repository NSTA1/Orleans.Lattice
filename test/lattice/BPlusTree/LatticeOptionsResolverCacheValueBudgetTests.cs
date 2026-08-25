using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Unit tests for the per-tree runtime <see cref="LatticeOptions.MaxCacheValueBytes"/>
/// override folded into <see cref="LatticeOptionsResolver"/> (both the full
/// <see cref="LatticeOptionsResolver.ResolveAsync(string)"/> record and the
/// lightweight <see cref="LatticeOptionsResolver.GetMaxCacheValueBytesAsync(string)"/>
/// fast path). The override on
/// <see cref="TreeRegistryEntry.MaxCacheValueBytes"/> wins when pinned; an
/// absent override falls back to the silo-wide static option byte-for-byte,
/// exactly mirroring the existing
/// <see cref="TreeRegistryEntry.MaintainProjectionDigest"/> precedence.
/// </summary>
[TestFixture]
public class LatticeOptionsResolverCacheValueBudgetTests
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

        // Default to a structurally-pinned entry with NO cache-value override so
        // the fall-back branch is the baseline. Individual tests overwrite the
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

    private static TreeRegistryEntry EntryWithOverride(long? maxCacheValueBytes) =>
        new()
        {
            MaxLeafKeys = LatticeConstants.DefaultMaxLeafKeys,
            MaxInternalChildren = LatticeConstants.DefaultMaxInternalChildren,
            ShardCount = LatticeConstants.DefaultShardCount,
            MaxCacheValueBytes = maxCacheValueBytes,
        };

    // ---- ResolveAsync (full record) ----

    [Test]
    public async Task ResolveAsync_no_override_null_static_resolves_null()
    {
        // Byte-for-byte baseline: the default is an unbounded (null) cap and no
        // registry override, so the resolved value must remain null.
        var (resolver, _) = Build(new LatticeOptions { MaxCacheValueBytes = null });

        var resolved = await resolver.ResolveAsync("user-tree");

        Assert.That(resolved.MaxCacheValueBytes, Is.Null);
    }

    [Test]
    public async Task ResolveAsync_no_override_falls_back_to_static_value()
    {
        var (resolver, _) = Build(new LatticeOptions { MaxCacheValueBytes = 4096 });

        var resolved = await resolver.ResolveAsync("user-tree");

        Assert.That(resolved.MaxCacheValueBytes, Is.EqualTo(4096),
            "With no per-tree override pinned, the resolved cap must equal the static option exactly.");
    }

    [Test]
    public async Task ResolveAsync_override_wins_over_static_value()
    {
        var (resolver, registry) = Build(new LatticeOptions { MaxCacheValueBytes = 4096 });
        registry.GetEntryAsync("user-tree").Returns(_ => Task.FromResult<TreeRegistryEntry?>(
            EntryWithOverride(1024)));

        var resolved = await resolver.ResolveAsync("user-tree");

        Assert.That(resolved.MaxCacheValueBytes, Is.EqualTo(1024),
            "A per-tree runtime override must win over the silo-wide static option.");
    }

    [Test]
    public async Task ResolveAsync_override_wins_even_when_static_is_null()
    {
        var (resolver, registry) = Build(new LatticeOptions { MaxCacheValueBytes = null });
        registry.GetEntryAsync("user-tree").Returns(_ => Task.FromResult<TreeRegistryEntry?>(
            EntryWithOverride(512)));

        var resolved = await resolver.ResolveAsync("user-tree");

        Assert.That(resolved.MaxCacheValueBytes, Is.EqualTo(512),
            "An override caps a mirror that the static option leaves unbounded.");
    }

    [Test]
    public async Task ResolveAsync_null_override_falls_back_to_static_value()
    {
        var (resolver, registry) = Build(new LatticeOptions { MaxCacheValueBytes = 2048 });
        registry.GetEntryAsync("user-tree").Returns(_ => Task.FromResult<TreeRegistryEntry?>(
            EntryWithOverride(null)));

        var resolved = await resolver.ResolveAsync("user-tree");

        Assert.That(resolved.MaxCacheValueBytes, Is.EqualTo(2048),
            "A cleared (null) override must fall back to the static option, not to unbounded.");
    }

    [Test]
    public async Task ResolveAsync_system_tree_uses_static_option_and_bypasses_registry()
    {
        var (resolver, registry) = Build(new LatticeOptions { MaxCacheValueBytes = 777 });

        var resolved = await resolver.ResolveAsync(LatticeConstants.SystemTreePrefix + "trees");

        Assert.That(resolved.MaxCacheValueBytes, Is.EqualTo(777),
            "System trees carry no registry entry; their cache cap resolves to the static option.");
        await registry.DidNotReceive().GetEntryAsync(Arg.Any<string>());
    }

    // ---- GetMaxCacheValueBytesAsync (fast path) ----

    [Test]
    public async Task GetMaxCacheValueBytesAsync_no_override_falls_back_to_static_value()
    {
        var (resolver, _) = Build(new LatticeOptions { MaxCacheValueBytes = 4096 });

        var value = await resolver.GetMaxCacheValueBytesAsync("user-tree");

        Assert.That(value, Is.EqualTo(4096));
    }

    [Test]
    public async Task GetMaxCacheValueBytesAsync_no_override_null_static_resolves_null()
    {
        var (resolver, _) = Build(new LatticeOptions { MaxCacheValueBytes = null });

        var value = await resolver.GetMaxCacheValueBytesAsync("user-tree");

        Assert.That(value, Is.Null);
    }

    [Test]
    public async Task GetMaxCacheValueBytesAsync_override_wins_over_static_value()
    {
        var (resolver, registry) = Build(new LatticeOptions { MaxCacheValueBytes = 4096 });
        registry.GetEntryAsync("user-tree").Returns(_ => Task.FromResult<TreeRegistryEntry?>(
            EntryWithOverride(1024)));

        var value = await resolver.GetMaxCacheValueBytesAsync("user-tree");

        Assert.That(value, Is.EqualTo(1024));
    }

    [Test]
    public async Task GetMaxCacheValueBytesAsync_null_override_falls_back_to_static_value()
    {
        var (resolver, registry) = Build(new LatticeOptions { MaxCacheValueBytes = 2048 });
        registry.GetEntryAsync("user-tree").Returns(_ => Task.FromResult<TreeRegistryEntry?>(
            EntryWithOverride(null)));

        var value = await resolver.GetMaxCacheValueBytesAsync("user-tree");

        Assert.That(value, Is.EqualTo(2048));
    }

    [Test]
    public async Task GetMaxCacheValueBytesAsync_system_tree_uses_static_and_bypasses_registry()
    {
        var (resolver, registry) = Build(new LatticeOptions { MaxCacheValueBytes = 777 });

        var value = await resolver.GetMaxCacheValueBytesAsync(LatticeConstants.SystemTreePrefix + "trees");

        Assert.That(value, Is.EqualTo(777));
        await registry.DidNotReceive().GetEntryAsync(Arg.Any<string>());
    }

    [Test]
    public void GetMaxCacheValueBytesAsync_throws_on_null_treeId()
    {
        var (resolver, _) = Build();

        Assert.ThrowsAsync<ArgumentNullException>(
            async () => await resolver.GetMaxCacheValueBytesAsync(null!));
    }

    [Test]
    public async Task ResolveAsync_and_fast_path_agree_on_the_resolved_cap()
    {
        // The two seams must never disagree: a consumer that reads the cheap
        // fast path must see the same cap the full record would report.
        var (resolver, registry) = Build(new LatticeOptions { MaxCacheValueBytes = 4096 });
        registry.GetEntryAsync("user-tree").Returns(_ => Task.FromResult<TreeRegistryEntry?>(
            EntryWithOverride(333)));

        var resolved = await resolver.ResolveAsync("user-tree");
        var fast = await resolver.GetMaxCacheValueBytesAsync("user-tree");

        Assert.That(fast, Is.EqualTo(resolved.MaxCacheValueBytes));
        Assert.That(fast, Is.EqualTo(333));
    }
}
