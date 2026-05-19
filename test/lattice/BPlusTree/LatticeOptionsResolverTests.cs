using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Unit tests for <see cref="LatticeOptionsResolver"/>. Covers system-tree
/// special-casing, the per-tree
/// <see cref="TreeRegistryEntry.MaintainProjectionDigest"/> override
/// precedence, and the one-way
/// <see cref="TreeRegistryEntry.ProjectionDigestPermanentlyDisabled"/>
/// latch that supersedes both the configured option and the per-tree
/// override.
/// </summary>
[TestFixture]
public class LatticeOptionsResolverTests
{
    [SetUp]
    public void Setup()
    {
        // Each test starts with a clean "warned-latched-trees" memo so
        // log-emission tests are independent.
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

        // Default to a fully-pinned entry so the lazy-register branch is
        // not exercised. Individual tests overwrite the response.
        registry.GetEntryAsync(Arg.Any<string>()).Returns(_ => Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry
            {
                MaxLeafKeys = LatticeConstants.DefaultMaxLeafKeys,
                MaxInternalChildren = LatticeConstants.DefaultMaxInternalChildren,
                ShardCount = LatticeConstants.DefaultShardCount,
            }));

        return (new LatticeOptionsResolver(factory, monitor), registry);
    }

    [Test]
    public async Task System_tree_forces_MaintainProjectionDigest_false_even_when_configured_true()
    {
        var (resolver, _) = Build(new LatticeOptions { MaintainProjectionDigest = true });

        var resolved = await resolver.ResolveAsync(LatticeConstants.SystemTreePrefix + "trees");

        Assert.That(resolved.MaintainProjectionDigest, Is.False,
            "System trees are silo-internal metadata that is never replicated; " +
            "the digest is a cross-silo drift canary that has no consumer here.");
    }

    [Test]
    public async Task System_tree_bypasses_registry()
    {
        var (resolver, registry) = Build();

        await resolver.ResolveAsync(LatticeConstants.SystemTreePrefix + "trees");

        // Resolver must not query the registry for system trees, to
        // avoid circular bootstrap during silo startup.
        await registry.DidNotReceive().GetEntryAsync(Arg.Any<string>());
    }

    [Test]
    public async Task User_tree_falls_back_to_silo_option_when_no_per_tree_override()
    {
        var (resolver, _) = Build(new LatticeOptions { MaintainProjectionDigest = true });

        var resolved = await resolver.ResolveAsync("user-tree");

        Assert.That(resolved.MaintainProjectionDigest, Is.True);
    }

    [Test]
    public async Task User_tree_per_tree_override_true_wins_over_silo_false()
    {
        var (resolver, registry) = Build(new LatticeOptions { MaintainProjectionDigest = false });
        registry.GetEntryAsync("user-tree").Returns(_ => Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry
            {
                MaxLeafKeys = LatticeConstants.DefaultMaxLeafKeys,
                MaxInternalChildren = LatticeConstants.DefaultMaxInternalChildren,
                ShardCount = LatticeConstants.DefaultShardCount,
                MaintainProjectionDigest = true,
            }));

        var resolved = await resolver.ResolveAsync("user-tree");

        Assert.That(resolved.MaintainProjectionDigest, Is.True);
    }

    [Test]
    public async Task User_tree_per_tree_override_false_wins_over_silo_true()
    {
        var (resolver, registry) = Build(new LatticeOptions { MaintainProjectionDigest = true });
        registry.GetEntryAsync("user-tree").Returns(_ => Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry
            {
                MaxLeafKeys = LatticeConstants.DefaultMaxLeafKeys,
                MaxInternalChildren = LatticeConstants.DefaultMaxInternalChildren,
                ShardCount = LatticeConstants.DefaultShardCount,
                MaintainProjectionDigest = false,
            }));

        var resolved = await resolver.ResolveAsync("user-tree");

        Assert.That(resolved.MaintainProjectionDigest, Is.False);
    }

    [Test]
    public async Task Latch_supersedes_silo_option_true()
    {
        var (resolver, registry) = Build(new LatticeOptions { MaintainProjectionDigest = true });
        registry.GetEntryAsync("user-tree").Returns(_ => Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry
            {
                MaxLeafKeys = LatticeConstants.DefaultMaxLeafKeys,
                MaxInternalChildren = LatticeConstants.DefaultMaxInternalChildren,
                ShardCount = LatticeConstants.DefaultShardCount,
                ProjectionDigestPermanentlyDisabled = true,
            }));

        var resolved = await resolver.ResolveAsync("user-tree");

        Assert.That(resolved.MaintainProjectionDigest, Is.False,
            "The one-way latch must force the effective value to false " +
            "regardless of the silo-wide configuration.");
    }

    [Test]
    public async Task Latch_supersedes_per_tree_override_true()
    {
        var (resolver, registry) = Build(new LatticeOptions { MaintainProjectionDigest = false });
        registry.GetEntryAsync("user-tree").Returns(_ => Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry
            {
                MaxLeafKeys = LatticeConstants.DefaultMaxLeafKeys,
                MaxInternalChildren = LatticeConstants.DefaultMaxInternalChildren,
                ShardCount = LatticeConstants.DefaultShardCount,
                MaintainProjectionDigest = true,
                ProjectionDigestPermanentlyDisabled = true,
            }));

        var resolved = await resolver.ResolveAsync("user-tree");

        Assert.That(resolved.MaintainProjectionDigest, Is.False);
    }

    [Test]
    public async Task Latch_null_treated_as_not_latched()
    {
        // Backwards compatibility: persisted registry rows from before the
        // latch field was added serialize as null, which the resolver
        // must treat identically to false.
        var (resolver, registry) = Build(new LatticeOptions { MaintainProjectionDigest = true });
        registry.GetEntryAsync("user-tree").Returns(_ => Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry
            {
                MaxLeafKeys = LatticeConstants.DefaultMaxLeafKeys,
                MaxInternalChildren = LatticeConstants.DefaultMaxInternalChildren,
                ShardCount = LatticeConstants.DefaultShardCount,
                ProjectionDigestPermanentlyDisabled = null,
            }));

        var resolved = await resolver.ResolveAsync("user-tree");

        Assert.That(resolved.MaintainProjectionDigest, Is.True);
    }

    [Test]
    public async Task MaxLeafReplayEntries_is_propagated_from_base_options()
    {
        // Regression: the resolver builds a fresh ResolvedLatticeOptions
        // via an object initializer and originally omitted
        // MaxLeafReplayEntries, so a host that lowered the replay budget
        // via IOptionsMonitor<LatticeOptions> would see the base-class
        // default (10 000) leak through to LatticeFallOffLogDetector. The
        // detector's gap-vs-budget arithmetic must observe the configured
        // value end-to-end.
        var (resolver, _) = Build(new LatticeOptions { MaxLeafReplayEntries = 5 });

        var resolved = await resolver.ResolveAsync("user-tree");

        Assert.That(resolved.MaxLeafReplayEntries, Is.EqualTo(5));
    }

    [Test]
    public async Task MaxLeafReplayEntries_propagates_default_when_not_overridden()
    {
        // Companion to the regression test above: when the host leaves
        // MaxLeafReplayEntries at its default, the resolved view exposes
        // the same default (10 000). Asserting both directions guards
        // against a future "override default with a different constant"
        // mistake in the resolver path.
        var (resolver, _) = Build(new LatticeOptions());

        var resolved = await resolver.ResolveAsync("user-tree");

        Assert.That(resolved.MaxLeafReplayEntries, Is.EqualTo(LatticeOptions.DefaultMaxLeafReplayEntries));
    }
}
