using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Regression tests for tenant scoping on the read-only state facade (issue 1689).
/// Before this, every per-tree read verb dialed
/// <c>GetGrain&lt;ILattice&gt;(treeId)</c> with the caller-supplied id verbatim and
/// never consulted <see cref="ITenantContextResolver"/>, so two tenants scanning
/// <c>orders</c> read the SAME physical tree.
/// </summary>
/// <remarks>
/// <para>
/// Composition happens at each verb's ENTRY POINT, so the auth-backed visibility
/// check, the reserved / system classification, the per-tree options lookup, and the
/// grain dial all see the SAME effective id. Checking visibility on the bare name
/// while reading the composed one would gate a different tree than it read.
/// </para>
/// <para>
/// The enumeration verbs (<c>ListTreesAsync</c>, <c>ListTagIndexesAsync</c>) take no
/// caller-supplied tree id and are scoped by the registry prefix pushdown plus
/// <c>ITenantEnumerationFilter</c> instead; those are pinned by
/// <see cref="LatticeStateQueryTenantCatalogTests"/> and
/// <c>LatticeStateQueryEnumerationPushdownTests</c> and are deliberately untouched
/// here.
/// </para>
/// </remarks>
[TestFixture]
public sealed class LatticeStateQueryTenantScopingTests
{
    private const string Tree = "orders";
    private const string AcmeTree = "t/acme/orders";
    private const string GlobexTree = "t/globex/orders";
    private const string Key = "k1";

    private static readonly TenantId Acme = TenantId.Parse("acme");
    private static readonly TenantId Globex = TenantId.Parse("globex");

    [SetUp]
    [TearDown]
    public void ClearAmbientTenant() => LatticeActiveTenantContext.Current = null;

    /// <summary>
    /// An access gate that records the tree id of every visibility decision, so a
    /// test can prove the read was gated on exactly the id it then dialed.
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

    /// <summary>A membership context that always resolves the same known subject.</summary>
    private sealed class FixedMembership(string subjectId) : ILatticeMembershipContext
    {
        public ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default)
            => new(new LatticeSubject(subjectId));

        public bool TryResolveCurrent(out LatticeSubject subject)
        {
            subject = new LatticeSubject(subjectId);
            return true;
        }
    }

    private static LatticeStateQuery CreateQuery(
        IGrainFactory factory,
        ITenantContextResolver resolver,
        ILatticeAccessGate? gate = null,
        ILatticeSchemaDeadLetterStore? deadLetters = null)
    {
        var services = Substitute.For<IServiceProvider>();
        if (gate is not null)
        {
            services.GetService(typeof(ILatticeAccessGate)).Returns(gate);
            services.GetService(typeof(ILatticeMembershipContext)).Returns(new FixedMembership("operator-1"));
        }

        if (deadLetters is not null)
        {
            services.GetService(typeof(ILatticeSchemaDeadLetterStore)).Returns(deadLetters);
        }

        var options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        options.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        return new LatticeStateQuery(
            factory, options, Options.Create(new LatticeApiStateOptions()), services, resolver);
    }

    /// <summary>Wires an existing, readable tree grain at <paramref name="treeId"/>.</summary>
    private static ILattice WireTree(IGrainFactory factory, string treeId)
    {
        var lattice = Substitute.For<ILattice>();
        lattice.TreeExistsAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(true));
        factory.GetGrain<ILattice>(treeId).Returns(lattice);
        return lattice;
    }

    private static void WireEntry(ILattice tree, string key, byte[] value)
        => tree.GetWithVersionAsync(key, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new VersionedValue { Value = value, Version = HybridLogicalClock.Zero }));

    // ----- (a) a tenant-scoped caller resolves to the composed tree -----

    [Test]
    public async Task GetEntryAsync_reads_the_tenant_composed_tree()
    {
        var factory = Substitute.For<IGrainFactory>();
        var acme = WireTree(factory, AcmeTree);
        WireEntry(acme, Key, [1, 2, 3]);
        var query = CreateQuery(factory, new AmbientTenantContextResolver());

        EntryDetailResult result;
        using (LatticeActiveTenantContext.With(Acme))
        {
            result = await query.GetEntryAsync(Tree, Key);
        }

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));

            // The response echoes the caller's own unqualified name, so the internal
            // composition never leaks onto the wire.
            Assert.That(result.TreeId, Is.EqualTo(Tree));
        });

        factory.Received().GetGrain<ILattice>(AcmeTree);
        factory.DidNotReceive().GetGrain<ILattice>(Tree);
    }

    [Test]
    public async Task GetTreeSummaryAsync_reads_the_tenant_composed_tree()
    {
        var factory = Substitute.For<IGrainFactory>();
        WireTree(factory, AcmeTree);
        var query = CreateQuery(factory, new AmbientTenantContextResolver());

        TreeSummaryResult summary;
        using (LatticeActiveTenantContext.With(Acme))
        {
            summary = await query.GetTreeSummaryAsync(Tree, deep: false);
        }

        Assert.Multiple(() =>
        {
            Assert.That(summary.Status, Is.EqualTo(StateQueryStatus.Found));
            Assert.That(summary.TreeId, Is.EqualTo(Tree));
            Assert.That(summary.Summary?.TreeId, Is.EqualTo(Tree));
        });

        factory.Received().GetGrain<ILattice>(AcmeTree);
        factory.DidNotReceive().GetGrain<ILattice>(Tree);
    }

    // ----- (b) two tenants, one unqualified name, two different trees -----

    [Test]
    public async Task Two_tenants_reading_the_same_unqualified_name_get_different_values()
    {
        var factory = Substitute.For<IGrainFactory>();
        var acme = WireTree(factory, AcmeTree);
        var globex = WireTree(factory, GlobexTree);
        WireEntry(acme, Key, [0xAC]);
        WireEntry(globex, Key, [0x61]);
        var query = CreateQuery(factory, new AmbientTenantContextResolver());

        EntryDetailResult acmeRead;
        using (LatticeActiveTenantContext.With(Acme))
        {
            acmeRead = await query.GetEntryAsync(Tree, Key);
        }

        EntryDetailResult globexRead;
        using (LatticeActiveTenantContext.With(Globex))
        {
            globexRead = await query.GetEntryAsync(Tree, Key);
        }

        // The reproduction in issue 1689: both tenants previously landed in ONE
        // physical tree and read each other's data back verbatim.
        Assert.Multiple(() =>
        {
            Assert.That(acmeRead.Entry?.ValuePreview, Is.EqualTo(new byte[] { 0xAC }));
            Assert.That(globexRead.Entry?.ValuePreview, Is.EqualTo(new byte[] { 0x61 }));
        });
    }

    [Test]
    public async Task A_tenant_cannot_reach_another_tenants_tree_by_the_shared_name()
    {
        // Only globex's tree exists; acme asking for the same unqualified name must
        // miss rather than fall through to it.
        var factory = Substitute.For<IGrainFactory>();
        var globex = WireTree(factory, GlobexTree);
        WireEntry(globex, Key, [0x61]);
        var query = CreateQuery(factory, new AmbientTenantContextResolver());

        EntryDetailResult acmeRead;
        using (LatticeActiveTenantContext.With(Acme))
        {
            acmeRead = await query.GetEntryAsync(Tree, Key);
        }

        Assert.Multiple(() =>
        {
            Assert.That(acmeRead.Status, Is.EqualTo(StateQueryStatus.TreeNotFound));
            Assert.That(acmeRead.Entry, Is.Null);
        });
    }

    // ----- (c) tenancy off is unchanged -----

    [Test]
    public async Task With_tenancy_off_the_bare_name_is_used_unchanged()
    {
        var factory = Substitute.For<IGrainFactory>();
        var bare = WireTree(factory, Tree);
        WireEntry(bare, Key, [7]);
        var query = CreateQuery(factory, new NullTenantContextResolver());

        var result = await query.GetEntryAsync(Tree, Key);

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
            Assert.That(result.TreeId, Is.EqualTo(Tree));
        });

        factory.Received().GetGrain<ILattice>(Tree);
    }

    [Test]
    public async Task With_tenancy_off_an_ambient_tenant_is_ignored_by_the_core_resolver()
    {
        var factory = Substitute.For<IGrainFactory>();
        var bare = WireTree(factory, Tree);
        WireEntry(bare, Key, [7]);
        var query = CreateQuery(factory, new NullTenantContextResolver());

        EntryDetailResult result;
        using (LatticeActiveTenantContext.With(Acme))
        {
            result = await query.GetEntryAsync(Tree, Key);
        }

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
        factory.Received().GetGrain<ILattice>(Tree);
        factory.DidNotReceive().GetGrain<ILattice>(AcmeTree);
    }

    [Test]
    public async Task The_default_tenant_returns_the_bare_name_unchanged()
    {
        var factory = Substitute.For<IGrainFactory>();
        var bare = WireTree(factory, Tree);
        WireEntry(bare, Key, [7]);
        var resolver = new AmbientTenantContextResolver();
        var query = CreateQuery(factory, resolver);

        // No ambient tenant: default-tenant adoption leaves the name alone.
        var result = await query.GetEntryAsync(Tree, Key);

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
        factory.Received().GetGrain<ILattice>(Tree);
        Assert.That(resolver.SynchronousResolutions, Is.EqualTo(1),
            "the warm path must resolve synchronously, with no await");
    }

    // ----- (d) visibility and the read use the SAME effective id -----

    [Test]
    public async Task The_visibility_check_and_the_grain_dial_see_the_same_effective_id()
    {
        var factory = Substitute.For<IGrainFactory>();
        var acme = WireTree(factory, AcmeTree);
        WireEntry(acme, Key, [1]);
        var gate = new RecordingGate();
        var query = CreateQuery(factory, new AmbientTenantContextResolver(), gate);

        using (LatticeActiveTenantContext.With(Acme))
        {
            await query.GetEntryAsync(Tree, Key);
        }

        // Gating the bare name while reading the composed one would check a
        // different tree than it read.
        Assert.That(gate.AuthorizedTreeIds, Is.Not.Empty);
        Assert.That(gate.AuthorizedTreeIds, Is.All.EqualTo(AcmeTree));
        factory.Received().GetGrain<ILattice>(AcmeTree);
    }

    [Test]
    public async Task A_denied_visibility_check_hides_the_composed_tree()
    {
        var factory = Substitute.For<IGrainFactory>();
        WireTree(factory, AcmeTree);
        var gate = new RecordingGate(allow: false);
        var query = CreateQuery(factory, new AmbientTenantContextResolver(), gate);

        EntryDetailResult result;
        using (LatticeActiveTenantContext.With(Acme))
        {
            result = await query.GetEntryAsync(Tree, Key);
        }

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(StateQueryStatus.TreeNotFound));
            Assert.That(gate.AuthorizedTreeIds, Is.All.EqualTo(AcmeTree),
                "the refusal must be evaluated against the tree the read would have addressed");
        });
    }

    // ----- other tree-scoped verbs -----

    [Test]
    public async Task GetEntryHistoryAsync_reads_the_tenant_composed_tree()
    {
        var factory = Substitute.For<IGrainFactory>();
        var acme = WireTree(factory, AcmeTree);
        acme.ScanEntryHistoryAsync(
                Arg.Any<string>(), Arg.Any<HybridLogicalClock?>(), Arg.Any<HybridLogicalClock?>(),
                Arg.Any<int>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new EntryHistoryPage
            {
                Revisions = [],
                Source = EntryHistorySource.WalWindow,
                Truncated = true,
            }));
        var query = CreateQuery(factory, new AmbientTenantContextResolver());

        EntryHistoryResult history;
        using (LatticeActiveTenantContext.With(Acme))
        {
            history = await query.GetEntryHistoryAsync(new EntryHistoryRequest { TreeId = Tree, Key = Key });
        }

        Assert.That(history.TreeId, Is.EqualTo(Tree));
        factory.Received().GetGrain<ILattice>(AcmeTree);
        factory.DidNotReceive().GetGrain<ILattice>(Tree);
    }

    [Test]
    public async Task CancelScanAsync_closes_the_cursor_on_the_tenant_composed_tree()
    {
        var factory = Substitute.For<IGrainFactory>();
        var acme = WireTree(factory, AcmeTree);
        var query = CreateQuery(factory, new AmbientTenantContextResolver());

        using (LatticeActiveTenantContext.With(Acme))
        {
            await query.CancelScanAsync(Tree, "cursor-1");
        }

        await acme.Received(1).CloseCursorAsync("cursor-1", Arg.Any<CancellationToken>());
        factory.DidNotReceive().GetGrain<ILattice>(Tree);
    }

    [Test]
    public async Task GetDeadLetterCountAsync_counts_against_the_tenant_composed_tree()
    {
        var factory = Substitute.For<IGrainFactory>();
        var store = Substitute.For<ILatticeSchemaDeadLetterStore>();
        store.CountAsync(AcmeTree, Arg.Any<CancellationToken>()).Returns(Task.FromResult(4));
        var query = CreateQuery(factory, new AmbientTenantContextResolver(), deadLetters: store);

        int count;
        using (LatticeActiveTenantContext.With(Acme))
        {
            count = await query.GetDeadLetterCountAsync(Tree);
        }

        Assert.That(count, Is.EqualTo(4));
        await store.Received(1).CountAsync(AcmeTree, Arg.Any<CancellationToken>());
        await store.DidNotReceive().CountAsync(Tree, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetPhysicalShardCountAsync_reads_the_tenant_composed_tree()
    {
        var factory = Substitute.For<IGrainFactory>();
        var acme = WireTree(factory, AcmeTree);
        acme.GetRoutingAsync(Arg.Any<CancellationToken>())
            .Returns(new RoutingInfo(AcmeTree, new ShardMap { Slots = [0, 1, 2, 3], Version = 1 }));
        var query = CreateQuery(factory, new AmbientTenantContextResolver());

        int? count;
        using (LatticeActiveTenantContext.With(Acme))
        {
            count = await query.GetPhysicalShardCountAsync(Tree);
        }

        Assert.That(count, Is.EqualTo(4));
        factory.DidNotReceive().GetGrain<ILattice>(Tree);
    }

    // ----- already-qualified and reserved names are never re-composed -----

    [Test]
    public async Task An_explicit_own_tenant_namespace_id_is_never_double_composed()
    {
        var factory = Substitute.For<IGrainFactory>();
        var acme = WireTree(factory, AcmeTree);
        WireEntry(acme, Key, [1]);
        var query = CreateQuery(factory, new AmbientTenantContextResolver());

        EntryDetailResult result;
        using (LatticeActiveTenantContext.With(Acme))
        {
            result = await query.GetEntryAsync(AcmeTree, Key);
        }

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
        factory.DidNotReceive().GetGrain<ILattice>("t/acme/t/acme/orders");
    }

    [Test]
    public async Task A_system_data_tree_name_is_not_composed_under_a_tenant()
    {
        // The core composition helper leaves the reserved system namespaces alone,
        // so a 'sys-' tree keeps addressing the one cluster-global tree it names.
        const string systemDataTree = "sys-auth-rules";
        var factory = Substitute.For<IGrainFactory>();
        var tree = WireTree(factory, systemDataTree);
        WireEntry(tree, Key, [1]);
        var query = CreateQuery(factory, new AmbientTenantContextResolver());

        EntryDetailResult result;
        using (LatticeActiveTenantContext.With(Acme))
        {
            result = await query.GetEntryAsync(systemDataTree, Key);
        }

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
        factory.Received().GetGrain<ILattice>(systemDataTree);
    }

    // ----- resolver contract: async fallback and fail-closed denial -----

    [Test]
    public async Task The_asynchronous_resolver_fallback_still_composes()
    {
        var factory = Substitute.For<IGrainFactory>();
        var acme = WireTree(factory, AcmeTree);
        WireEntry(acme, Key, [1]);
        var resolver = new AmbientTenantContextResolver(resolveSynchronously: false);
        var query = CreateQuery(factory, resolver);

        EntryDetailResult result;
        using (LatticeActiveTenantContext.With(Acme))
        {
            result = await query.GetEntryAsync(Tree, Key);
        }

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
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
        var query = CreateQuery(factory, new AmbientTenantContextResolver(deny: true));

        using (LatticeActiveTenantContext.With(Acme))
        {
            Assert.That(
                async () => await query.GetEntryAsync(Tree, Key),
                Throws.InstanceOf<LatticeTenantAccessDeniedException>());
        }

        Assert.That(factory.ReceivedCalls(), Is.Empty, "no grain may be dialed for an unattributable request");
    }

    [Test]
    public void An_empty_tree_id_is_rejected_against_the_facades_own_parameter_name()
    {
        var query = CreateQuery(Substitute.For<IGrainFactory>(), new AmbientTenantContextResolver());

        // The core helper guards its own 'treeName' parameter, so the facade guards
        // first to keep the rejection pointing at the argument the caller passed.
        Assert.That(
            async () => await query.GetEntryAsync(string.Empty, Key),
            Throws.InstanceOf<ArgumentException>().With.Property("ParamName").EqualTo("treeId"));
    }
}
