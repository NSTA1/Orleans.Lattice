using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.Fakes;

/// <summary>
/// Test helper that builds a real <see cref="LatticeOptionsResolver"/>
/// wired to a mocked <see cref="IGrainFactory"/> and <see cref="ILatticeRegistry"/>
/// so unit-tested grains can resolve per-tree structural pins without
/// standing up an Orleans cluster.
/// </summary>
internal static class TestOptionsResolver
{
    /// <summary>
    /// Creates a <see cref="LatticeOptionsResolver"/> seeded with the given
    /// structural pin. The same pin is returned for every tree id queried.
    /// </summary>
    public static LatticeOptionsResolver Create(
        LatticeOptions? baseOptions = null,
        int maxLeafKeys = 128,
        int maxInternalChildren = 128,
        int shardCount = 1,
        IGrainFactory? factory = null)
    {
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(baseOptions ?? new LatticeOptions());

        factory ??= Substitute.For<IGrainFactory>();
        var registry = Substitute.For<ILatticeRegistry>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.GetEntryAsync(Arg.Any<string>()).Returns(_ => Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry
            {
                MaxLeafKeys = maxLeafKeys,
                MaxInternalChildren = maxInternalChildren,
                ShardCount = shardCount,
            }));

        return new LatticeOptionsResolver(factory, optionsMonitor);
    }

    /// <summary>
    /// Creates a <see cref="LatticeOptionsResolver"/> that reuses an existing
    /// <see cref="IGrainFactory"/>. The caller is responsible for having
    /// stubbed <c>GetGrain&lt;ILatticeRegistry&gt;</c> and
    /// <c>registry.GetEntryAsync</c> on that factory, or for using a
    /// system-tree id that bypasses the registry.
    /// </summary>
    public static LatticeOptionsResolver ForFactory(
        IGrainFactory factory,
        LatticeOptions? baseOptions = null)
    {
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(baseOptions ?? new LatticeOptions());
        return new LatticeOptionsResolver(factory, optionsMonitor);
    }
}
