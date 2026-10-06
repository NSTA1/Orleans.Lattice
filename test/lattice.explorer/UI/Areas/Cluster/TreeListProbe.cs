using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.UI.Areas.Cluster;
using Orleans.Lattice.Explorer.UI.Areas.Data;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Cluster;

/// <summary>
/// Watches the circuit's two remembered tree lists - the Cluster area's
/// <see cref="ClusterTreeCatalog"/> and the Data area's <see cref="DataDirectory"/> -
/// so a test can prove a mutation that changes the set of trees forgets both,
/// rather than leaving a stale list on screen until a reload.
/// </summary>
internal sealed class TreeListProbe
{
    private readonly ClusterTreeCatalog _catalog;
    private readonly IReadOnlyList<ClusterTreeEntry> _primed;
    private int _directoryChanges;

    private TreeListProbe(IServiceProvider services, IReadOnlyList<ClusterTreeEntry> primed)
    {
        _catalog = services.GetRequiredService<ClusterTreeCatalog>();
        _primed = primed;
        services.GetRequiredService<DataDirectory>().Changed += () => Interlocked.Increment(ref _directoryChanges);
    }

    /// <summary>Primes the catalogue so it is remembered, and starts counting directory changes.</summary>
    /// <param name="services">The test context's services.</param>
    /// <returns>The probe.</returns>
    public static async Task<TreeListProbe> PrimeAsync(IServiceProvider services)
    {
        var catalog = services.GetRequiredService<ClusterTreeCatalog>();
        var primed = await catalog.GetAsync(false, default);
        Assert.That(primed, Is.Not.Empty, "an empty list is a shared instance, so seed at least one tree");
        Assert.That(await catalog.GetAsync(false, default), Is.SameAs(primed), "the catalogue remembers its list while fresh");
        return new TreeListProbe(services, primed);
    }

    /// <summary>The number of times the Data directory was told to forget its trees.</summary>
    public int DirectoryChanges => Volatile.Read(ref _directoryChanges);

    /// <summary>Whether the catalogue's remembered list was forgotten since priming.</summary>
    /// <returns><see langword="true"/> when the next read is a fresh list.</returns>
    public async Task<bool> CatalogForgottenAsync() =>
        !ReferenceEquals(_primed, await _catalog.GetAsync(false, default));

    /// <summary>Asserts both lists were forgotten.</summary>
    public async Task AssertBothForgottenAsync()
    {
        Assert.That(DirectoryChanges, Is.GreaterThan(0), "the Data directory's tree list was forgotten");
        Assert.That(await CatalogForgottenAsync(), Is.True, "the Cluster catalogue's tree list was forgotten");
    }

    /// <summary>Asserts neither list was forgotten.</summary>
    public async Task AssertNeitherForgottenAsync()
    {
        Assert.That(DirectoryChanges, Is.Zero, "the Data directory's tree list was kept");
        Assert.That(await CatalogForgottenAsync(), Is.False, "the Cluster catalogue's tree list was kept");
    }
}
