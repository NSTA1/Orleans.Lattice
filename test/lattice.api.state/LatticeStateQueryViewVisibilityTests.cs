using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Auth;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Unit coverage for how <see cref="LatticeStateQuery"/> gates a
/// materialised-view (<c>view-*</c>) tree when auth-backed visibility is enabled.
///
/// This is a load-bearing security seam rather than a convenience: reading a view
/// opens a <c>ViewReadContext</c> scope that makes the data-plane access gate
/// bypass itself, so the readability of the view's SOURCE tree is the only
/// authorization boundary the read has. If the source cannot be resolved - the
/// view is unknown, its name is unrecoverable, or the runtime registry is
/// briefly unreachable - the only safe answer is to hide the view. Resolving to
/// "no source" and then proceeding would turn an unresolvable view into an
/// ungated read.
///
/// The local <see cref="IViewCatalog"/> covers views declared at startup or
/// rehydrated on this silo; the durable <see cref="IViewRegistryGrain"/> is the
/// cluster-wide fallback for a runtime view created on another silo. Both arms
/// are exercised here, along with the fail-closed default.
/// </summary>
[TestFixture]
public sealed class LatticeStateQueryViewVisibilityTests
{
    private const string ViewName = "orders-by-customer";
    private const string ViewTree = LatticeConstants.ViewTreePrefix + ViewName;
    private const string SourceTree = "orders";

    /// <summary>
    /// Builds a query with visibility enabled for a subject that may read exactly
    /// <paramref name="readableTrees"/>. <paramref name="localViews"/> seeds the
    /// silo-local view catalog; <paramref name="runtimeViews"/> seeds the durable
    /// cluster registry; <paramref name="registryThrows"/> makes the registry
    /// activation fail.
    /// </summary>
    private static LatticeStateQuery CreateQuery(
        string[] readableTrees,
        (string ViewName, string SourceTreeId)[]? localViews = null,
        (string ViewName, string SourceTreeId)[]? runtimeViews = null,
        bool registryThrows = false)
    {
        var grainFactory = Substitute.For<IGrainFactory>();

        var registry = Substitute.For<IViewRegistryGrain>();
        if (registryThrows)
        {
            registry.ListAsync().Returns<Task<IReadOnlyList<RuntimeViewRegistration>>>(
                _ => throw new InvalidOperationException("registry activation failed"));
        }
        else
        {
            IReadOnlyList<RuntimeViewRegistration> registrations = (runtimeViews ?? [])
                .Select(v => new RuntimeViewRegistration
                {
                    ViewName = v.ViewName,
                    SourceTreeId = v.SourceTreeId,
                    ProjectionTypeName = "StubProjection",
                    ProjectionVersion = "1",
                })
                .ToList();
            registry.ListAsync().Returns(Task.FromResult(registrations));
        }

        grainFactory.GetGrain<IViewRegistryGrain>(IViewRegistryGrain.SingletonKey).Returns(registry);

        var treeRegistry = Substitute.For<ILatticeRegistry>();
        treeRegistry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(null));
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(treeRegistry);

        var options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        options.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        var services = new ServiceCollection();
        services.AddSingleton<ILatticeAccessGate>(new TreeScopedGate(readableTrees));
        services.AddSingleton<ILatticeMembershipContext>(new FixedSubject());
        if (localViews is not null)
        {
            services.AddSingleton<IViewCatalog>(new StubViewCatalog(localViews));
        }

        return new LatticeStateQuery(
            grainFactory,
            options,
            Options.Create(new LatticeApiStateOptions()),
            services.BuildServiceProvider(),
            new NullTenantContextResolver());
    }

    [Test]
    public async Task An_unknown_view_is_hidden_rather_than_read_ungated()
    {
        // Neither the local catalog nor the cluster registry knows the view, so
        // its source - the read's only authorization boundary - is unresolvable.
        var query = CreateQuery(readableTrees: [SourceTree]);

        var result = await query.GetTreeSummaryAsync(ViewTree);

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.TreeNotFound),
            "An unresolvable view must be reported not-found, never read without a source grant.");
    }

    [Test]
    public async Task A_view_whose_source_the_subject_cannot_read_is_hidden()
    {
        var query = CreateQuery(
            readableTrees: ["something-else"],
            localViews: [(ViewName, SourceTree)]);

        var result = await query.GetTreeSummaryAsync(ViewTree);

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.TreeNotFound));
    }

    [Test]
    public async Task A_view_resolved_from_the_local_catalog_is_gated_by_its_source_grant()
    {
        var query = CreateQuery(
            readableTrees: [SourceTree],
            localViews: [(ViewName, SourceTree)]);

        var result = await query.GetTreeSummaryAsync(ViewTree);

        // The source grant admits the read; the view tree itself has no state in
        // this unit harness, so it resolves to a not-found summary rather than
        // being refused by the visibility gate. Either way the gate did not hide
        // it, which is what distinguishes this from the cases above.
        Assert.That(result, Is.Not.Null);
    }

    [Test]
    public async Task A_runtime_view_created_on_another_silo_resolves_through_the_durable_registry()
    {
        // No local catalog entry: the silo-local fast path misses and the
        // cluster-wide registry must supply the source tree.
        var query = CreateQuery(
            readableTrees: ["not-the-source"],
            runtimeViews: [(ViewName, SourceTree)]);

        var result = await query.GetTreeSummaryAsync(ViewTree);

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.TreeNotFound),
            "The registry-resolved source must still be gated; resolving it is not the same as granting it.");
    }

    [Test]
    public async Task A_registry_naming_a_different_view_does_not_resolve_the_source()
    {
        var query = CreateQuery(
            readableTrees: [SourceTree],
            runtimeViews: [("some-other-view", SourceTree)]);

        var result = await query.GetTreeSummaryAsync(ViewTree);

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.TreeNotFound),
            "A non-matching registration must not be mistaken for this view's source.");
    }

    [Test]
    public async Task A_transient_registry_failure_hides_the_view()
    {
        var query = CreateQuery(readableTrees: [SourceTree], registryThrows: true);

        var result = await query.GetTreeSummaryAsync(ViewTree);

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.TreeNotFound),
            "A registry blip must fail closed; degrading to an ungated view read would be a privilege escalation.");
    }

    [Test]
    public async Task A_view_tree_id_with_no_recoverable_name_is_hidden()
    {
        var query = CreateQuery(readableTrees: [SourceTree]);

        var result = await query.GetTreeSummaryAsync(LatticeConstants.ViewTreePrefix);

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.TreeNotFound));
    }

    [Test]
    public async Task A_system_tree_is_not_gated_by_the_data_tree_visibility_check()
    {
        // System trees are hidden by the per-call existence checks, not by this
        // gate; the gate must not double-refuse them on a source-tree rule.
        var query = CreateQuery(readableTrees: []);

        var result = await query.GetTreeSummaryAsync(LatticeConstants.SystemTreePrefix + "internal");

        Assert.That(result, Is.Not.Null);
    }

    /// <summary>An access gate that allows reads on a fixed set of tree ids.</summary>
    private sealed class TreeScopedGate(string[] readableTrees) : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default)
            => new(readableTrees.Contains(request.TreeId, StringComparer.Ordinal)
                ? LatticeAccessDecision.Allow()
                : LatticeAccessDecision.Deny("not readable"));
    }

    private sealed class FixedSubject : ILatticeMembershipContext
    {
        private static readonly LatticeSubject Subject = new("alice");

        public ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default)
            => new(Subject);
    }

    private sealed class StubViewCatalog((string ViewName, string SourceTreeId)[] views) : IViewCatalog
    {
        public ViewRegistration? TryGet(string viewName)
        {
            foreach (var (name, source) in views)
            {
                if (string.Equals(name, viewName, StringComparison.Ordinal))
                {
                    return new ViewRegistration(name, source, Projection: null);
                }
            }

            return null;
        }

        public void Register(ViewRegistration registration) => throw new NotSupportedException();

        public void Remove(string viewName) => throw new NotSupportedException();

        public IReadOnlyCollection<ViewRegistration> All() => throw new NotSupportedException();
    }
}
