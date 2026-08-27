using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Backup;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Api.TreeAdmin.Tests;

/// <summary>
/// Regression tests for tenant scoping on the tree-administration facade
/// (issue 1689). Before this, every verb dialed
/// <c>GetGrain&lt;ILattice&gt;(treeId)</c> with the caller-supplied id verbatim and
/// never consulted <see cref="ITenantContextResolver"/>, so two tenants asking to
/// administer <c>orders</c> reached the SAME physical tree, and a tenant could not
/// reach its own <c>t/{tenant}/orders</c> because the reserved-namespace guard
/// (correctly) refuses a caller-supplied <c>t/</c> id.
/// </summary>
/// <remarks>
/// The load-bearing property is that composition happens at the ENTRY POINT, so the
/// reserved-namespace guard, the authorization gate, and the grain dial all see the
/// SAME effective id. Authorizing the bare name and then operating on the composed
/// one (or the reverse) would check one tree and act on another.
/// </remarks>
[TestFixture]
public sealed class LatticeTreeAdminTenantScopingTests
{
    private const string Tree = "orders";
    private const string AcmeTree = "t/acme/orders";
    private const string GlobexTree = "t/globex/orders";

    private static readonly TenantId Acme = TenantId.Parse("acme");
    private static readonly TenantId Globex = TenantId.Parse("globex");

    [SetUp]
    [TearDown]
    public void ClearAmbientTenant() => LatticeActiveTenantContext.Current = null;

    /// <summary>
    /// An access gate that records the tree id of every authorization request, so a
    /// test can prove the gate was consulted for exactly the id the operation then
    /// addressed.
    /// </summary>
    private sealed class RecordingGate(bool allow = true) : ILatticeAccessGate
    {
        public List<string> AuthorizedTreeIds { get; } = [];

        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request, CancellationToken cancellationToken = default)
        {
            AuthorizedTreeIds.Add(request.TreeId);
            return new(allow ? LatticeAccessDecision.Allow() : LatticeAccessDecision.Deny("denied by test"));
        }
    }

    private static LatticeTreeAdmin Create(
        IGrainFactory factory,
        ITenantContextResolver resolver,
        ILatticeAccessGate? gate = null,
        ILatticeBackupRestoreService? restoreService = null)
        => new(
            Substitute.For<ILatticeSchemaControl>(),
            factory,
            new TreeAdminAccessAuthorizer(gate ?? new RecordingGate()),
            Options.Create(new LatticeApiTreeAdminOptions()),
            resolver,
            restoreService);

    private static ILatticeRegistry WireRegistry(IGrainFactory factory)
    {
        var registry = Substitute.For<ILatticeRegistry>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        return registry;
    }

    // ----- (a) a tenant-scoped caller resolves to the composed tree -----

    [Test]
    public async Task CreateTreeAsync_registers_the_tenant_composed_tree()
    {
        var factory = Substitute.For<IGrainFactory>();
        var registry = WireRegistry(factory);
        var facade = Create(factory, new AmbientTenantContextResolver());

        using (LatticeActiveTenantContext.With(Acme))
        {
            await facade.CreateTreeAsync(Tree);
        }

        await registry.Received(1).RegisterAsync(AcmeTree, Arg.Any<TreeRegistryEntry?>());
        await registry.DidNotReceive().RegisterAsync(Tree, Arg.Any<TreeRegistryEntry?>());
    }

    [Test]
    public async Task DeleteTreeAsync_dials_the_tenant_composed_tree()
    {
        var factory = Substitute.For<IGrainFactory>();
        var lattice = Substitute.For<ILattice>();
        var deletion = Substitute.For<ITreeDeletionGrain>();
        factory.GetGrain<ILattice>(AcmeTree).Returns(lattice);
        factory.GetGrain<ITreeDeletionGrain>(AcmeTree).Returns(deletion);
        deletion.GetDeletionStatusAsync().Returns(new TreeDeletionSnapshot { IsDeleted = true });
        var facade = Create(factory, new AmbientTenantContextResolver());

        TreeDeletionStatus status;
        using (LatticeActiveTenantContext.With(Acme))
        {
            status = await facade.DeleteTreeAsync(Tree);
        }

        Assert.Multiple(() =>
        {
            Assert.That(status.IsDeleted, Is.True, "the composed tree's deletion grain must have been read");

            // The response echoes the caller's own unqualified name, so the internal
            // composition never leaks onto the wire.
            Assert.That(status.TreeId, Is.EqualTo(Tree));
        });

        factory.Received().GetGrain<ILattice>(AcmeTree);
        factory.DidNotReceive().GetGrain<ILattice>(Tree);
    }

    // ----- (b) two tenants, one unqualified name, two different trees -----

    [Test]
    public async Task Two_tenants_using_the_same_unqualified_name_reach_different_trees()
    {
        var factory = Substitute.For<IGrainFactory>();
        var registry = WireRegistry(factory);
        var facade = Create(factory, new AmbientTenantContextResolver());

        using (LatticeActiveTenantContext.With(Acme))
        {
            await facade.CreateTreeAsync(Tree);
        }

        using (LatticeActiveTenantContext.With(Globex))
        {
            await facade.CreateTreeAsync(Tree);
        }

        // The crux of issue 1689: the same requested name must NOT collide in one
        // physical tree once tenancy is on.
        await registry.Received(1).RegisterAsync(AcmeTree, Arg.Any<TreeRegistryEntry?>());
        await registry.Received(1).RegisterAsync(GlobexTree, Arg.Any<TreeRegistryEntry?>());
        await registry.DidNotReceive().RegisterAsync(Tree, Arg.Any<TreeRegistryEntry?>());
    }

    [Test]
    public async Task Two_tenants_reading_the_same_unqualified_name_are_authorized_over_different_trees()
    {
        var factory = Substitute.For<IGrainFactory>();
        WireRegistry(factory);
        var gate = new RecordingGate();
        var facade = Create(factory, new AmbientTenantContextResolver(), gate);

        using (LatticeActiveTenantContext.With(Acme))
        {
            await facade.CheckTreeExistsAsync(Tree);
        }

        using (LatticeActiveTenantContext.With(Globex))
        {
            await facade.CheckTreeExistsAsync(Tree);
        }

        Assert.That(gate.AuthorizedTreeIds, Is.EqualTo(new[] { AcmeTree, GlobexTree }));
    }

    // ----- (c) tenancy off is unchanged -----

    [Test]
    public async Task With_tenancy_off_the_bare_name_is_used_unchanged()
    {
        var factory = Substitute.For<IGrainFactory>();
        var registry = WireRegistry(factory);
        var gate = new RecordingGate();
        var facade = Create(factory, new NullTenantContextResolver(), gate);

        var result = await facade.CreateTreeAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(result.TreeId, Is.EqualTo(Tree));
            Assert.That(gate.AuthorizedTreeIds, Is.EqualTo(new[] { Tree }));
        });

        await registry.Received(1).RegisterAsync(Tree, Arg.Any<TreeRegistryEntry?>());
    }

    [Test]
    public async Task With_tenancy_off_an_ambient_tenant_is_ignored_by_the_core_resolver()
    {
        // The core no-op resolver always resolves TenantId.Default, so even a
        // stamped ambient tenant cannot change behaviour on a cluster that never
        // registered the tenancy add-on.
        var factory = Substitute.For<IGrainFactory>();
        var registry = WireRegistry(factory);
        var facade = Create(factory, new NullTenantContextResolver());

        using (LatticeActiveTenantContext.With(Acme))
        {
            await facade.CreateTreeAsync(Tree);
        }

        await registry.Received(1).RegisterAsync(Tree, Arg.Any<TreeRegistryEntry?>());
    }

    [Test]
    public async Task The_default_tenant_returns_the_bare_name_unchanged()
    {
        var factory = Substitute.For<IGrainFactory>();
        var registry = WireRegistry(factory);
        var resolver = new AmbientTenantContextResolver();
        var facade = Create(factory, resolver);

        // No ambient tenant: the resolver reports TenantId.Default, which is
        // default-tenant adoption and must leave the name alone.
        await facade.CreateTreeAsync(Tree);

        await registry.Received(1).RegisterAsync(Tree, Arg.Any<TreeRegistryEntry?>());
        Assert.That(resolver.SynchronousResolutions, Is.EqualTo(1),
            "the warm path must resolve synchronously, with no await");
    }

    // ----- (d) authorization and the operation use the SAME effective id -----

    [Test]
    public async Task The_gate_and_the_grain_dial_see_the_same_effective_id()
    {
        var factory = Substitute.For<IGrainFactory>();
        var lattice = Substitute.For<ILattice>();
        factory.GetGrain<ILattice>(AcmeTree).Returns(lattice);
        var gate = new RecordingGate();
        var facade = Create(factory, new AmbientTenantContextResolver(), gate);

        using (LatticeActiveTenantContext.With(Acme))
        {
            await facade.TriggerShardCompactionAsync(Tree, shardIndex: 0);
        }

        Assert.That(gate.AuthorizedTreeIds, Is.EqualTo(new[] { AcmeTree }),
            "authorizing the bare name while compacting the composed one would gate the wrong tree");
        factory.Received().GetGrain<ILattice>(AcmeTree);
        await lattice.Received(1).CompactShardAsync(0, Arg.Any<CancellationToken>());
    }

    [Test]
    public void A_denied_gate_refuses_before_the_composed_tree_is_touched()
    {
        var factory = Substitute.For<IGrainFactory>();
        var gate = new RecordingGate(allow: false);
        var facade = Create(factory, new AmbientTenantContextResolver(), gate);

        using (LatticeActiveTenantContext.With(Acme))
        {
            Assert.That(
                async () => await facade.TriggerShardCompactionAsync(Tree, shardIndex: 0),
                Throws.InstanceOf<LatticeAuthorizationDeniedException>());
        }

        Assert.That(gate.AuthorizedTreeIds, Is.EqualTo(new[] { AcmeTree }));
        factory.DidNotReceive().GetGrain<ILattice>(Arg.Any<string>());
    }

    // ----- ThrowIfReserved interplay -----

    [Test]
    public async Task A_composed_id_passes_the_reserved_namespace_guard()
    {
        // Composition happens BEFORE the guard, and the guard admits a t/ id whose
        // structural owner is the ambient active tenant - so a tenant naming its own
        // tree by its unqualified name is no longer trapped between "denied" and
        // "reserved" (issue 1689, reproduction A + B).
        var factory = Substitute.For<IGrainFactory>();
        var registry = WireRegistry(factory);
        var facade = Create(factory, new AmbientTenantContextResolver());

        using (LatticeActiveTenantContext.With(Acme))
        {
            await facade.CreateTreeAsync(Tree);
        }

        await registry.Received(1).RegisterAsync(AcmeTree, Arg.Any<TreeRegistryEntry?>());
    }

    [Test]
    public void An_explicit_foreign_tenant_namespace_id_is_still_refused()
    {
        // Composition must not have opened a route into another tenant's namespace:
        // an already-qualified id is never re-composed, so it reaches the guard as
        // written and is refused because its owner is not the active tenant.
        var factory = Substitute.For<IGrainFactory>();
        var facade = Create(factory, new AmbientTenantContextResolver());

        using (LatticeActiveTenantContext.With(Acme))
        {
            Assert.That(
                async () => await facade.CreateTreeAsync(GlobexTree),
                Throws.InstanceOf<ArgumentException>());
        }

        Assert.That(factory.ReceivedCalls(), Is.Empty);
    }

    [Test]
    public async Task An_explicit_own_tenant_namespace_id_is_admitted_and_never_double_composed()
    {
        var factory = Substitute.For<IGrainFactory>();
        var registry = WireRegistry(factory);
        var facade = Create(factory, new AmbientTenantContextResolver());

        using (LatticeActiveTenantContext.With(Acme))
        {
            await facade.CreateTreeAsync(AcmeTree);
        }

        // t/acme/t/acme/orders would be the double-composition bug.
        await registry.Received(1).RegisterAsync(AcmeTree, Arg.Any<TreeRegistryEntry?>());
    }

    [Test]
    public void A_system_namespace_id_is_still_refused_under_a_tenant()
    {
        var factory = Substitute.For<IGrainFactory>();
        var facade = Create(factory, new AmbientTenantContextResolver());

        using (LatticeActiveTenantContext.With(Acme))
        {
            Assert.That(
                async () => await facade.CreateTreeAsync(LatticeConstants.SystemTreePrefix + "registry"),
                Throws.InstanceOf<ArgumentException>());
        }
    }

    // ----- two-tree-id verbs -----

    [Test]
    public async Task SetTreeAliasAsync_composes_BOTH_tree_ids()
    {
        var factory = Substitute.For<IGrainFactory>();
        var registry = WireRegistry(factory);
        var facade = Create(factory, new AmbientTenantContextResolver());

        using (LatticeActiveTenantContext.With(Acme))
        {
            await facade.SetTreeAliasAsync(Tree, "orders-v2");
        }

        // Composing only the logical id would alias a tenant's tree onto a bare,
        // cluster-global physical tree - a cross-tenant crossing.
        await registry.Received(1).SetAliasAsync(AcmeTree, "t/acme/orders-v2");
    }

    [Test]
    public void SetTreeAliasAsync_refuses_a_foreign_tenant_physical_target()
    {
        var factory = Substitute.For<IGrainFactory>();
        WireRegistry(factory);
        var facade = Create(factory, new AmbientTenantContextResolver());

        using (LatticeActiveTenantContext.With(Acme))
        {
            Assert.That(
                async () => await facade.SetTreeAliasAsync(Tree, "t/globex/secrets"),
                Throws.InstanceOf<ArgumentException>());
        }
    }

    [Test]
    public async Task SnapshotTreeAsync_composes_BOTH_tree_ids()
    {
        var factory = Substitute.For<IGrainFactory>();
        var lattice = Substitute.For<ILattice>();
        factory.GetGrain<ILattice>(AcmeTree).Returns(lattice);
        var facade = Create(factory, new AmbientTenantContextResolver());

        TreeSnapshotStatus status;
        using (LatticeActiveTenantContext.With(Acme))
        {
            status = await facade.SnapshotTreeAsync(Tree, "orders-copy", TreeSnapshotMode.Online);
        }

        // Composing only the source would let a tenant drain its own tree into a
        // bare, cluster-global destination.
        await lattice.Received(1).SnapshotAsync(
            "t/acme/orders-copy",
            Arg.Any<SnapshotMode>(),
            Arg.Any<int?>(),
            Arg.Any<int?>(),
            Arg.Any<CancellationToken>());

        Assert.Multiple(() =>
        {
            // Both echoed ids are the caller's own unqualified names.
            Assert.That(status.TreeId, Is.EqualTo(Tree));
            Assert.That(status.RequestedDestinationTreeId, Is.EqualTo("orders-copy"));
        });
    }

    [Test]
    public void SnapshotTreeAsync_refuses_a_foreign_tenant_destination()
    {
        var factory = Substitute.For<IGrainFactory>();
        var facade = Create(factory, new AmbientTenantContextResolver());

        using (LatticeActiveTenantContext.With(Acme))
        {
            Assert.That(
                async () => await facade.SnapshotTreeAsync(Tree, GlobexTree, TreeSnapshotMode.Online),
                Throws.InstanceOf<ArgumentException>());
        }

        Assert.That(factory.ReceivedCalls(), Is.Empty);
    }

    // ----- restore: composed target, echoed bare name, symmetric revert -----

    [Test]
    public async Task RestoreTreeAsync_targets_the_composed_tree_but_echoes_the_bare_name()
    {
        var service = Substitute.For<ILatticeBackupRestoreService>();
        LatticeRestoreRequest? captured = null;
        service.RestoreAsync(Arg.Any<LatticeRestoreRequest>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                captured = call.Arg<LatticeRestoreRequest>();
                return new LatticeRestoreResult(
                    "bk-1", AcmeTree, LatticeRestoreMode.ShadowCutover, "op-1",
                    ["bk-1"], entriesApplied: 1,
                    shadowPhysicalTreeId: "phys-new", previousPhysicalTreeId: "phys-old");
            });
        var facade = Create(
            Substitute.For<IGrainFactory>(), new AmbientTenantContextResolver(), restoreService: service);

        TreeRestoreResult result;
        using (LatticeActiveTenantContext.With(Acme))
        {
            result = await facade.RestoreTreeAsync(Tree, "bk-1");
        }

        Assert.Multiple(() =>
        {
            Assert.That(captured?.TargetTreeId, Is.EqualTo(AcmeTree), "the engine must restore into the composed tree");
            Assert.That(result.TargetTreeId, Is.EqualTo(Tree), "the caller must get its own unqualified name back");
        });
    }

    [Test]
    public async Task RevertTreeRestoreAsync_recomposes_the_echoed_bare_name()
    {
        var service = Substitute.For<ILatticeBackupRestoreService>();
        LatticeRestoreResult? captured = null;
        service.RevertRestoreAsync(Arg.Any<LatticeRestoreResult>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                captured = call.Arg<LatticeRestoreResult>();
                return Task.CompletedTask;
            });
        var facade = Create(
            Substitute.For<IGrainFactory>(), new AmbientTenantContextResolver(), restoreService: service);

        var dto = new TreeRestoreResult
        {
            BackupId = "bk-1",
            TargetTreeId = Tree,
            Mode = TreeRestoreMode.ShadowCutover,
            OperationId = "op-1",
            ManifestChain = ["bk-1"],
            EntriesApplied = 1,
            ShadowPhysicalTreeId = "phys-new",
            PreviousPhysicalTreeId = "phys-old",
        };

        using (LatticeActiveTenantContext.With(Acme))
        {
            await facade.RevertTreeRestoreAsync(dto);
        }

        // Round-trip symmetry: RestoreTreeAsync echoes the bare name, so the revert
        // must compose it again to address the same physical tree.
        Assert.That(captured?.TargetTreeId, Is.EqualTo(AcmeTree));
    }

    [Test]
    public void RevertTreeRestoreAsync_under_another_tenant_cannot_revert_the_original_tree()
    {
        // A leaked result DTO is not a capability: composed under a DIFFERENT active
        // tenant it names that tenant's own namespace, never the original tree.
        var service = Substitute.For<ILatticeBackupRestoreService>();
        LatticeRestoreResult? captured = null;
        service.RevertRestoreAsync(Arg.Any<LatticeRestoreResult>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                captured = call.Arg<LatticeRestoreResult>();
                return Task.CompletedTask;
            });
        var facade = Create(
            Substitute.For<IGrainFactory>(), new AmbientTenantContextResolver(), restoreService: service);

        var dto = new TreeRestoreResult
        {
            BackupId = "bk-1",
            TargetTreeId = Tree,
            Mode = TreeRestoreMode.ShadowCutover,
            OperationId = "op-1",
            ManifestChain = ["bk-1"],
            EntriesApplied = 1,
        };

        using (LatticeActiveTenantContext.With(Globex))
        {
            facade.RevertTreeRestoreAsync(dto).GetAwaiter().GetResult();
        }

        Assert.That(captured?.TargetTreeId, Is.EqualTo(GlobexTree));
    }

    // ----- WAL placement echoes the caller's name -----

    [Test]
    public async Task GetWalPlacementAsync_addresses_the_composed_tree_and_echoes_the_bare_name()
    {
        var factory = Substitute.For<IGrainFactory>();
        var admin = Substitute.For<ILatticeAdmin>();
        factory.GetGrain<ILatticeAdmin>(LatticeConstants.AdminGrainKey).Returns(admin);
        admin.GetWalPlacementAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new WalPlacement { TreeId = AcmeTree, Version = 3 });
        var facade = Create(factory, new AmbientTenantContextResolver());

        TreeWalPlacement placement;
        using (LatticeActiveTenantContext.With(Acme))
        {
            placement = await facade.GetWalPlacementAsync(Tree);
        }

        await admin.Received(1).GetWalPlacementAsync(AcmeTree, Arg.Any<CancellationToken>());
        Assert.That(placement.TreeId, Is.EqualTo(Tree));
    }

    // ----- resolver contract: async fallback and fail-closed denial -----

    [Test]
    public async Task The_asynchronous_resolver_fallback_still_composes()
    {
        var factory = Substitute.For<IGrainFactory>();
        var registry = WireRegistry(factory);
        var resolver = new AmbientTenantContextResolver(resolveSynchronously: false);
        var facade = Create(factory, resolver);

        using (LatticeActiveTenantContext.With(Acme))
        {
            await facade.CreateTreeAsync(Tree);
        }

        await registry.Received(1).RegisterAsync(AcmeTree, Arg.Any<TreeRegistryEntry?>());
        Assert.Multiple(() =>
        {
            Assert.That(resolver.AsynchronousResolutions, Is.EqualTo(1));
            Assert.That(resolver.SynchronousResolutions, Is.Zero);
        });
    }

    [Test]
    public void A_denying_resolver_fails_closed_before_any_grain_is_dialed()
    {
        var factory = Substitute.For<IGrainFactory>();
        var gate = new RecordingGate();
        var facade = Create(factory, new AmbientTenantContextResolver(deny: true), gate);

        using (LatticeActiveTenantContext.With(Acme))
        {
            Assert.That(
                async () => await facade.CreateTreeAsync(Tree),
                Throws.InstanceOf<LatticeTenantAccessDeniedException>());
        }

        Assert.Multiple(() =>
        {
            Assert.That(factory.ReceivedCalls(), Is.Empty, "no grain may be dialed for an unattributable request");
            Assert.That(gate.AuthorizedTreeIds, Is.Empty, "the gate is never even consulted");
        });
    }

    [Test]
    public void An_empty_tree_id_is_rejected_against_the_facades_own_parameter_name()
    {
        var facade = Create(Substitute.For<IGrainFactory>(), new AmbientTenantContextResolver());

        // The core helper guards its own 'treeName' parameter, so the facade guards
        // first to keep the rejection pointing at the argument the caller passed.
        Assert.That(
            async () => await facade.CreateTreeAsync(string.Empty),
            Throws.InstanceOf<ArgumentException>().With.Property("ParamName").EqualTo("treeId"));
    }
}
