using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Api.TreeAdmin.Tests;

/// <summary>
/// Tree-count admission on the alias seam: the detector for the <c>Repoint</c>
/// action of <c>spec/tenant-quotas/TenantTreeCount.tla</c>. A re-point of an existing
/// logical id adds no registry row and is not charged.
/// </summary>
public sealed partial class LatticeTreeAdminTenantQuotaTests
{
    private const string AliasTarget = "orders-v2";
    private const string AcmeAliasTarget = "t/acme/orders-v2";

    /// <summary>
    /// Wires a registry holding <paramref name="rows"/>: the prefix-honouring
    /// tree-id listing the count reads, existence, and the single and batched
    /// entry reads the alias carry-across performs.
    /// </summary>
    private static ILatticeRegistry WireAliasRegistry(
        IGrainFactory factory,
        IReadOnlyDictionary<string, TreeRegistryEntry> rows)
    {
        var registry = WirePrefixedRegistry(factory, [.. rows.Keys]);
        registry.ExistsAsync(Arg.Any<string>()).Returns(call => rows.ContainsKey(call.Arg<string>()));
        registry.GetEntryAsync(Arg.Any<string>())
            .Returns(call => rows.GetValueOrDefault(call.Arg<string>()));
        registry.GetEntriesAsync(Arg.Any<IReadOnlyList<string>>()).Returns(call =>
        {
            var found = new Dictionary<string, TreeRegistryEntry>(StringComparer.Ordinal);
            foreach (var id in call.Arg<IReadOnlyList<string>>())
            {
                if (rows.TryGetValue(id, out var row)) found[id] = row;
            }

            return Task.FromResult(found);
        });
        return registry;
    }

    [Test]
    public async Task SetTreeAliasAsync_repointing_an_existing_logical_id_is_not_charged()
    {
        // The logical id already has a row, so the alias rewrites it and the count
        // does not change: a tenant at its cap must still be able to move an alias.
        var factory = Substitute.For<IGrainFactory>();
        var registry = WireAliasRegistry(factory, new Dictionary<string, TreeRegistryEntry>(StringComparer.Ordinal)
        {
            [AcmeTree] = new() { PhysicalTreeId = "t/acme/orders-v1" },
            ["t/acme/orders-v1"] = new(),
            [AcmeAliasTarget] = new(),
        });
        var admission = new RecordingAdmissionController(active: true, admit: false);
        var facade = Create(factory, admission);

        LatticeActiveTenantContext.Current = Acme;
        await facade.SetTreeAliasAsync(Tree, AliasTarget);

        Assert.That(admission.AdmitCalls, Is.Zero,
            "re-pointing an existing logical id adds no registry row and must not be charged or refused");
        await registry.Received(1).SwapAliasAsync(
            AcmeTree, AcmeAliasTarget, Arg.Any<ShardMap>(), Arg.Any<int?>(), Arg.Any<string?>());
    }
}
