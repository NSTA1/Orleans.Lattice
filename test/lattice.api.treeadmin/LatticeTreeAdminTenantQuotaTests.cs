using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Api.TreeAdmin.Tests;

/// <summary>
/// Regression coverage for per-tenant quota admission on tree creation.
/// </summary>
/// <remarks>
/// <para>
/// <c>MaxTreeCount</c> exists to bound how many trees a tenant may create, but it
/// was consulted only by the tenant-scoped administration facade, one layer above
/// this one. <see cref="LatticeTreeAdmin"/> is itself tenant-aware - it composes
/// the caller's active tenant into every tree id at its entry point - so a tenant
/// reaching it directly created trees inside its own namespace with no ceiling
/// applied at all. The single quota dimension whose entire purpose is to bound
/// tree creation therefore did not bind at the point of creation, letting one
/// tenant register unbounded trees, each of which is a registry entry and a set of
/// grain activations that every other tenant on the silo pays for.
/// </para>
/// <para>
/// Enforcement now sits here, the narrowest seam every create funnels through, and
/// is deliberately <b>not</b> duplicated at the outer facade: admission is stateful
/// (it consumes a request-rate token), so charging at both layers would bill a
/// single create twice.
/// </para>
/// <para>
/// The controller is an optional dependency, so a host that has not registered the
/// tenancy add-on resolves <c>null</c> here and the whole check is a single null
/// read that allocates nothing - the pre-tenancy behaviour, unchanged.
/// </para>
/// </remarks>
[TestFixture]
public sealed class LatticeTreeAdminTenantQuotaTests
{
    private const string Tree = "orders";
    private const string AcmeTree = "t/acme/orders";

    private static readonly TenantId Acme = TenantId.Parse("acme");

    [SetUp]
    [TearDown]
    public void ClearAmbientTenant() => LatticeActiveTenantContext.Current = null;

    private sealed class AllowingGate : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request, CancellationToken cancellationToken = default)
            => new(LatticeAccessDecision.Allow());
    }

    private sealed class DenyingGate : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request, CancellationToken cancellationToken = default)
            => new(LatticeAccessDecision.Deny("denied by test"));
    }

    /// <summary>
    /// Records every admission evaluation so a test can assert both that the create
    /// was charged and that it was charged to the right tenant and composed tree id.
    /// </summary>
    private sealed class RecordingAdmissionController(
        bool active,
        bool admit,
        Exception? throwOnAdmit = null) : ITenantAdmissionController
    {
        public bool IsActive => active;

        public int AdmitCalls { get; private set; }

        public TenantId LastTenant { get; private set; }

        public string? LastTreeId { get; private set; }

        public ValueTask<bool> IsAdmittedAsync(TenantId tenant, string treeId, CancellationToken cancellationToken = default)
        {
            AdmitCalls++;
            LastTenant = tenant;
            LastTreeId = treeId;
            if (throwOnAdmit is not null)
            {
                throw throwOnAdmit;
            }

            return new ValueTask<bool>(admit);
        }
    }

    private static LatticeTreeAdmin Create(
        IGrainFactory factory,
        ITenantAdmissionController? admission,
        ILatticeAccessGate? gate = null)
        => new(
            Substitute.For<ILatticeSchemaControl>(),
            factory,
            new TreeAdminAccessAuthorizer(gate ?? new AllowingGate()),
            Options.Create(new LatticeApiTreeAdminOptions()),
            new AmbientTenantContextResolver(),
            restoreService: null,
            viewCatalog: null,
            viewFactory: null,
            tagIndexFactory: null,
            admission: admission);

    private static ILatticeRegistry WireRegistry(IGrainFactory factory)
    {
        var registry = Substitute.For<ILatticeRegistry>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        return registry;
    }

    // ----- the defect: creation was never charged against the tenant -----

    [Test]
    public async Task CreateTreeAsync_charges_the_tenant_for_the_composed_tree()
    {
        var factory = Substitute.For<IGrainFactory>();
        WireRegistry(factory);
        var admission = new RecordingAdmissionController(active: true, admit: true);
        var facade = Create(factory, admission);

        LatticeActiveTenantContext.Current = Acme;
        await facade.CreateTreeAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(admission.AdmitCalls, Is.EqualTo(1),
                "creation must be counted against the tenant's quota; before this it never was");
            Assert.That(admission.LastTenant, Is.EqualTo(Acme));
            Assert.That(admission.LastTreeId, Is.EqualTo(AcmeTree),
                "the quota must be charged for the composed id the tree is actually registered under");
        });
    }

    [Test]
    public async Task CreateTreeAsync_when_quota_refuses_does_not_register_the_tree()
    {
        var factory = Substitute.For<IGrainFactory>();
        var registry = WireRegistry(factory);
        var facade = Create(factory, new RecordingAdmissionController(active: true, admit: false));

        LatticeActiveTenantContext.Current = Acme;
        Assert.That(
            async () => await facade.CreateTreeAsync(Tree),
            Throws.TypeOf<LatticeTenantAccessDeniedException>());

        // The refusal must precede the registry write, or the ceiling would be
        // advisory: the tree would exist and only the caller's result would differ.
        await registry.DidNotReceiveWithAnyArgs().RegisterAsync(default!, default);
    }

    [Test]
    public async Task CreateTreeAsync_propagates_quota_exceeded_and_does_not_register_the_tree()
    {
        var factory = Substitute.For<IGrainFactory>();
        var registry = WireRegistry(factory);
        var facade = Create(
            factory,
            new RecordingAdmissionController(active: true, admit: true, new LatticeQuotaExceededException("quota")));

        LatticeActiveTenantContext.Current = Acme;
        Assert.That(
            async () => await facade.CreateTreeAsync(Tree),
            Throws.TypeOf<LatticeQuotaExceededException>());

        await registry.DidNotReceiveWithAnyArgs().RegisterAsync(default!, default);
    }

    // ----- authorize before accounting (cross-tenant regression) -----

    [Test]
    public void CreateTreeAsync_when_authorization_denies_never_consults_admission()
    {
        var factory = Substitute.For<IGrainFactory>();
        WireRegistry(factory);
        var admission = new RecordingAdmissionController(active: true, admit: true);
        var facade = Create(factory, admission, new DenyingGate());

        // The active tenant is a caller assertion that only authorization validates,
        // so charging before it would let an unauthorized caller name any victim and
        // drain that victim's rate budget - and read its usage and ceiling back out
        // of the resulting quota exception.
        LatticeActiveTenantContext.Current = Acme;
        Assert.That(
            async () => await facade.CreateTreeAsync(Tree),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());

        Assert.That(admission.AdmitCalls, Is.Zero,
            "a create the authorizer denies must not consume the named tenant's quota or rate budget");
    }

    // ----- inert unless the tenancy add-on is registered and active -----

    [Test]
    public async Task CreateTreeAsync_with_no_controller_registered_creates_as_before()
    {
        var factory = Substitute.For<IGrainFactory>();
        var registry = WireRegistry(factory);
        var facade = Create(factory, admission: null);

        LatticeActiveTenantContext.Current = Acme;
        await facade.CreateTreeAsync(Tree);

        await registry.Received(1).RegisterAsync(AcmeTree, Arg.Any<TreeRegistryEntry?>());
    }

    [Test]
    public async Task CreateTreeAsync_with_inactive_controller_creates_without_consulting_it()
    {
        var factory = Substitute.For<IGrainFactory>();
        var registry = WireRegistry(factory);
        var admission = new RecordingAdmissionController(active: false, admit: false);
        var facade = Create(factory, admission);

        LatticeActiveTenantContext.Current = Acme;
        await facade.CreateTreeAsync(Tree);

        Assert.That(admission.AdmitCalls, Is.Zero);
        await registry.Received(1).RegisterAsync(AcmeTree, Arg.Any<TreeRegistryEntry?>());
    }

    // ----- internal machinery is exempt -----

    [Test]
    public async Task CreateTreeAsync_from_system_origin_is_not_charged()
    {
        var factory = Substitute.For<IGrainFactory>();
        var registry = WireRegistry(factory);
        var admission = new RecordingAdmissionController(active: true, admit: false);
        var facade = Create(factory, admission);

        LatticeActiveTenantContext.Current = Acme;
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await facade.CreateTreeAsync(Tree);
        }

        Assert.That(admission.AdmitCalls, Is.Zero,
            "platform machinery must not be refused by, or accounted against, a tenant's quota");
        await registry.Received(1).RegisterAsync(AcmeTree, Arg.Any<TreeRegistryEntry?>());
    }

    /// <summary>
    /// Invokes the tree-count callback the facade supplies, the way the real
    /// tenancy controller does once a tenant carries a <c>MaxTreeCount</c> ceiling,
    /// and records what the callback returned and the prefix it was read under.
    /// </summary>
    private sealed class CountingAdmissionController(long ceiling) : ITenantAdmissionController
    {
        public bool IsActive => true;

        public long? ObservedCount { get; private set; }

        public ValueTask<bool> IsAdmittedAsync(TenantId tenant, string treeId, CancellationToken cancellationToken = default)
            => new(true);

        public async ValueTask<bool> IsTreeCreateAdmittedAsync(
            TenantId tenant,
            string treeId,
            Func<CancellationToken, ValueTask<long>> countTenantTrees,
            CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(countTenantTrees);
            var count = await countTenantTrees(cancellationToken).ConfigureAwait(false);
            ObservedCount = count;
            return count < ceiling;
        }
    }

    /// <summary>
    /// Wires a registry whose tree-id listing honours the prefix argument, so a
    /// count read under the wrong prefix shows up as a wrong count rather than
    /// being masked by a substitute that ignores it.
    /// </summary>
    private static ILatticeRegistry WirePrefixedRegistry(
        IGrainFactory factory,
        IReadOnlyList<string> allTreeIds,
        Action<string?>? onList = null)
    {
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetAllTreeIdsAsync(Arg.Any<string?>()).Returns(call =>
        {
            var prefix = call.Arg<string?>();
            onList?.Invoke(prefix);
            return Task.FromResult<IReadOnlyList<string>>(
                string.IsNullOrEmpty(prefix)
                    ? allTreeIds
                    : allTreeIds.Where(id => id.StartsWith(prefix, StringComparison.Ordinal)).ToList());
        });
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        return registry;
    }

    // ----- the authoritative tree count MaxTreeCount binds on -----

    [Test]
    public async Task CreateTreeAsync_counts_only_the_tenants_own_trees_from_the_registry()
    {
        // The callback is what makes MaxTreeCount bind at all: the controller is
        // handed a counting function rather than a number, so nothing counts unless
        // a ceiling exists. No fixture drove that callback, so the count could have
        // been read cluster-wide - which would refuse a tenant for other tenants'
        // trees - and nothing would have failed.
        var factory = Substitute.For<IGrainFactory>();
        var prefixes = new List<string?>();
        WirePrefixedRegistry(
            factory,
            [
                "t/acme/orders", "t/acme/widgets",
                "t/globex/orders", "t/globex/widgets", "t/globex/parts",
                "shared",
            ],
            prefixes.Add);
        var admission = new CountingAdmissionController(ceiling: 10);
        var facade = Create(factory, admission);

        LatticeActiveTenantContext.Current = Acme;
        await facade.CreateTreeAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(admission.ObservedCount, Is.EqualTo(2),
                "only the creating tenant's own trees may be counted against its ceiling");
            Assert.That(prefixes, Does.Contain(LatticeTenantTrees.ComposePrefix(Acme)),
                "the count must be a prefix-scoped range scan, not a whole-catalog read");
        });
    }

    [Test]
    public async Task CreateTreeAsync_refuses_when_the_counted_trees_reach_the_ceiling()
    {
        var factory = Substitute.For<IGrainFactory>();
        var registry = WirePrefixedRegistry(factory, ["t/acme/a", "t/acme/b", "t/acme/c"]);
        var facade = Create(factory, new CountingAdmissionController(ceiling: 3));

        LatticeActiveTenantContext.Current = Acme;
        Assert.That(
            async () => await facade.CreateTreeAsync(Tree),
            Throws.TypeOf<LatticeTenantAccessDeniedException>());

        await registry.DidNotReceiveWithAnyArgs().RegisterAsync(default!, default);
    }

    [Test]
    public async Task CreateTreeAsync_admits_when_the_counted_trees_are_below_the_ceiling()
    {
        // The accepting counterpart: the same controller and the same counting path,
        // one tree short of the ceiling, must still create.
        var factory = Substitute.For<IGrainFactory>();
        var registry = WirePrefixedRegistry(factory, ["t/acme/a", "t/acme/b"]);
        var facade = Create(factory, new CountingAdmissionController(ceiling: 3));

        LatticeActiveTenantContext.Current = Acme;
        await facade.CreateTreeAsync(Tree);

        await registry.Received(1).RegisterAsync(AcmeTree, Arg.Any<TreeRegistryEntry?>());
    }

    [Test]
    public async Task CreateTreeAsync_reads_the_tenant_tree_count_under_system_origin()
    {
        // The registry is reserved infrastructure: a count read without the
        // system-origin scope is refused by the access gate, so the ceiling would
        // fault instead of binding. Observed from inside the registry call itself,
        // which is the only point at which the scope is in force.
        var factory = Substitute.For<IGrainFactory>();
        var sawSystemOrigin = false;
        WirePrefixedRegistry(factory, ["t/acme/a"], _ => sawSystemOrigin = LatticeAccessGateContext.IsSystemOrigin);
        var facade = Create(factory, new CountingAdmissionController(ceiling: 10));

        LatticeActiveTenantContext.Current = Acme;
        await facade.CreateTreeAsync(Tree);

        Assert.That(sawSystemOrigin, Is.True,
            "the tenant tree count reads reserved registry state and must enter a system-origin scope");
    }

    [Test]
    public async Task CreateTreeAsync_counts_zero_for_a_tenant_with_no_trees_yet()
    {
        // A tenant that has never created a tree must count zero rather than
        // inheriting another tenant's rows, and must be admitted.
        var factory = Substitute.For<IGrainFactory>();
        WirePrefixedRegistry(factory, ["t/globex/orders", "shared"]);
        var admission = new CountingAdmissionController(ceiling: 1);
        var facade = Create(factory, admission);

        LatticeActiveTenantContext.Current = Acme;
        await facade.CreateTreeAsync(Tree);

        Assert.That(admission.ObservedCount, Is.Zero);
    }
}
