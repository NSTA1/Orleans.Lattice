using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// WAL providers for <see cref="WalMoveFenceClusterFixture"/>. Separate from
/// <c>WalMoveProviders</c> so the two placement-move fixtures never share a
/// store. The baseline ("default") is the move source; the hooked "secondary" is
/// the move target, whose verification read hosts the test's interleaving.
/// </summary>
internal static class WalMoveFenceProviders
{
    /// <summary>The "default" baseline WAL provider (the move source).</summary>
    public static InMemoryWalStorageProvider Baseline { get; private set; } = new();

    /// <summary>The hooked "secondary" WAL provider (the move target).</summary>
    public static HookedWalStorageProvider Secondary { get; private set; } = new(new InMemoryWalStorageProvider());

    /// <summary>Resets both providers to empty stores.</summary>
    public static void Reset()
    {
        Baseline = new InMemoryWalStorageProvider();
        Secondary = new HookedWalStorageProvider(new InMemoryWalStorageProvider());
    }
}
