using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Api.TreeAdmin.Tests;

/// <summary>
/// Tree-count admission on the alias seam: the detectors for the <c>CheckAlias</c>
/// and <c>Repoint</c> actions of <c>spec/tenant-quotas/TenantTreeCount.tla</c>.
/// An alias onto a logical id with no registry row adds a row, so it is admitted
/// exactly as a create; a re-point of an existing logical id adds none and is not
/// charged.
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
    public async Task SetTreeAliasAsync_new_logical_id_refuses_when_the_counted_trees_reach_the_ceiling()
    {
        // The tenant is at its ceiling, and an alias onto a logical id with no
        // registry row adds a row: it must be refused before anything is written,
        // exactly as a create would be (#5333).
        var factory = Substitute.For<IGrainFactory>();
        var registry = WireAliasRegistry(factory, new Dictionary<string, TreeRegistryEntry>(StringComparer.Ordinal)
        {
            ["t/acme/a"] = new(),
            ["t/acme/b"] = new(),
            [AcmeAliasTarget] = new(),
        });
        var admission = new CountingAdmissionController(ceiling: 3);
        var facade = Create(factory, admission);

        LatticeActiveTenantContext.Current = Acme;
        Assert.That(
            async () => await facade.SetTreeAliasAsync(Tree, AliasTarget),
            Throws.TypeOf<LatticeTenantAccessDeniedException>());

        Assert.That(admission.ObservedCount, Is.EqualTo(3),
            "the alias must be checked against the same tenant tree count a create is");
        await registry.DidNotReceiveWithAnyArgs().SwapAliasAsync(default!, default!, default!, default, default);
        await registry.DidNotReceiveWithAnyArgs().SetAliasAsync(default!, default!);
    }

    [Test]
    public async Task SetTreeAliasAsync_new_logical_id_is_charged_for_the_composed_logical_id()
    {
        var factory = Substitute.For<IGrainFactory>();
        var registry = WireAliasRegistry(factory, new Dictionary<string, TreeRegistryEntry>(StringComparer.Ordinal)
        {
            [AcmeAliasTarget] = new(),
        });
        var admission = new RecordingAdmissionController(active: true, admit: true);
        var facade = Create(factory, admission);

        LatticeActiveTenantContext.Current = Acme;
        await facade.SetTreeAliasAsync(Tree, AliasTarget);

        Assert.Multiple(() =>
        {
            Assert.That(admission.AdmitCalls, Is.EqualTo(1), "an alias that adds a registry row is charged once, like a create");
            Assert.That(admission.LastTenant, Is.EqualTo(Acme));
            Assert.That(admission.LastTreeId, Is.EqualTo(AcmeTree),
                "the charge is for the logical id the new row is registered under, not the alias target");
        });
        await registry.Received(1).SwapAliasAsync(
            AcmeTree, AcmeAliasTarget, Arg.Any<ShardMap>(), Arg.Any<int?>(), Arg.Any<string?>());
    }

    [Test]
    public void SetTreeAliasAsync_when_authorization_denies_never_consults_admission()
    {
        var factory = Substitute.For<IGrainFactory>();
        WireAliasRegistry(factory, new Dictionary<string, TreeRegistryEntry>(StringComparer.Ordinal));
        var admission = new RecordingAdmissionController(active: true, admit: true);
        var facade = Create(factory, admission, new DenyingGate());

        LatticeActiveTenantContext.Current = Acme;
        Assert.That(
            async () => await facade.SetTreeAliasAsync(Tree, AliasTarget),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());

        Assert.That(admission.AdmitCalls, Is.Zero,
            "a denied alias must not consume the named tenant's quota or rate budget");
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
