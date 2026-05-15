using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.Fakes;

/// <summary>
/// Test helper that supplies the production
/// <see cref="DefaultLatticeOriginClusterIdResolver"/> for unit-tested
/// grains. The default resolver always returns <see cref="string.Empty"/>,
/// matching single-cluster host behaviour, which is correct for every
/// unit test that does not exercise the replication package.
/// </summary>
internal static class TestOriginClusterIdResolver
{
    /// <summary>
    /// Returns a <see cref="DefaultLatticeOriginClusterIdResolver"/>
    /// instance. The resolver is stateless and safe to share across tests,
    /// but a fresh instance per call keeps test isolation symmetric with
    /// the other Fakes helpers.
    /// </summary>
    public static ILatticeOriginClusterIdResolver Default() => new DefaultLatticeOriginClusterIdResolver();
}