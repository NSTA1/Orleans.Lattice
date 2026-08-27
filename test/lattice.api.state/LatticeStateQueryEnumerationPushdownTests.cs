using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Pins the contract that the registry-enumeration prefix pushdown (issue #1682)
/// is a <b>performance hint and never an authorization boundary</b>.
/// </summary>
/// <remarks>
/// The optimisation narrows which registry keys are read so the whole catalog no
/// longer crosses the grain boundary to be discarded client-side. That is only
/// safe while every caller-facing check stays exactly where it was, so these
/// tests assert the observable consequences: narrowing can only ever <em>shrink</em>
/// the result, the tenant filter still runs on whatever the scan returned, and a
/// request that could see system trees is never narrowed at all.
/// </remarks>
[TestFixture]
public sealed class LatticeStateQueryEnumerationPushdownTests
{
    [TearDown]
    public void ClearAmbientTenant() => LatticeActiveTenantContext.Current = null;

    /// <summary>Records every prefix the query asked the registry for.</summary>
    private sealed class RecordingRegistry
    {
        public List<string?> Prefixes { get; } = [];
    }

    private static (LatticeStateQuery Query, RecordingRegistry Recorder) CreateQuery(
        IReadOnlyList<string> allTreeIds)
    {
        var recorder = new RecordingRegistry();
        var grainFactory = Substitute.For<IGrainFactory>();

        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.GetAllTreeIdsAsync().Returns(Task.FromResult(allTreeIds));
        registry.GetAllTreeIdsAsync(Arg.Any<string?>()).Returns(call =>
        {
            var prefix = call.Arg<string?>();
            recorder.Prefixes.Add(prefix);
            return Task.FromResult<IReadOnlyList<string>>(
                string.IsNullOrEmpty(prefix)
                    ? allTreeIds
                    : allTreeIds.Where(id => id.StartsWith(prefix, StringComparison.Ordinal)).ToList());
        });
        registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(null));

        var deletion = Substitute.For<ITreeDeletionGrain>();
        deletion.IsDeletedAsync().Returns(Task.FromResult(false));
        grainFactory.GetGrain<ITreeDeletionGrain>(Arg.Any<string>()).Returns(deletion);

        var options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        options.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        var services = new ServiceCollection().BuildServiceProvider();

        var query = new LatticeStateQuery(
            grainFactory,
            options,
            Options.Create(new LatticeApiStateOptions()),
            services,
            new NullTenantContextResolver());

        return (query, recorder);
    }

    private static readonly string[] Catalog =
    [
        "t/acme/orders",
        "t/acme/users",
        "t/globex/secrets",
        "legacy-tree",
    ];

    [Test]
    public async Task A_tenant_request_is_narrowed_to_its_own_prefix()
    {
        var (query, recorder) = CreateQuery(Catalog);

        using (LatticeActiveTenantContext.With(TenantId.Parse("acme")))
        {
            await query.ListTreesAsync(new CatalogRequest());
        }

        Assert.That(recorder.Prefixes, Is.EqualTo(new[] { "t/acme/" }));
    }

    [Test]
    public async Task A_request_that_includes_system_trees_is_never_narrowed()
    {
        // Narrowing would skip the reserved and system-data ids this request is
        // explicitly asking for, so the enumeration must stay unscoped.
        var (query, recorder) = CreateQuery(Catalog);

        using (LatticeActiveTenantContext.With(TenantId.Parse("acme")))
        {
            await query.ListTreesAsync(new CatalogRequest { IncludeSystemTrees = true });
        }

        Assert.That(recorder.Prefixes, Is.EqualTo(new string?[] { null }));
    }

    [Test]
    public async Task The_default_tenant_is_never_narrowed()
    {
        // Bare legacy ids share no common prefix, so there is nothing to push down.
        var (query, recorder) = CreateQuery(Catalog);

        using (LatticeActiveTenantContext.With(TenantId.Default))
        {
            await query.ListTreesAsync(new CatalogRequest());
        }

        Assert.That(recorder.Prefixes, Is.EqualTo(new string?[] { null }));
    }

    [Test]
    public async Task No_active_tenant_is_never_narrowed()
    {
        var (query, recorder) = CreateQuery(Catalog);

        await query.ListTreesAsync(new CatalogRequest());

        Assert.That(recorder.Prefixes, Is.EqualTo(new string?[] { null }));
    }

    [Test]
    public async Task Narrowing_can_only_shrink_the_result_never_widen_it()
    {
        // The security property in one assertion: whatever the pushdown returns for
        // a tenant must be a subset of the unscoped enumeration. A prefix can never
        // surface an id the caller could not already have enumerated.
        var (scopedQuery, _) = CreateQuery(Catalog);
        var (unscopedQuery, _) = CreateQuery(Catalog);

        TreeCatalogPage scoped;
        using (LatticeActiveTenantContext.With(TenantId.Parse("acme")))
        {
            scoped = await scopedQuery.ListTreesAsync(new CatalogRequest());
        }

        var unscoped = await unscopedQuery.ListTreesAsync(new CatalogRequest());

        var scopedIds = scoped.Entries.Select(e => e.TreeId).ToArray();
        var unscopedIds = unscoped.Entries.Select(e => e.TreeId).ToArray();

        Assert.That(scopedIds, Is.SubsetOf(unscopedIds));
    }

    [Test]
    public async Task A_tenant_never_sees_another_tenants_ids_through_the_pushdown()
    {
        var (query, _) = CreateQuery(Catalog);

        TreeCatalogPage page;
        using (LatticeActiveTenantContext.With(TenantId.Parse("acme")))
        {
            page = await query.ListTreesAsync(new CatalogRequest());
        }

        var ids = page.Entries.Select(e => e.TreeId).ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(ids, Does.Not.Contain("t/globex/secrets"));
            Assert.That(ids, Does.Not.Contain("legacy-tree"));
        });
    }

    [Test]
    public async Task The_tag_index_catalog_is_narrowed_to_the_tag_prefix()
    {
        var (query, recorder) = CreateQuery(["tag-colour", "t/acme/orders", "legacy-tree"]);

        await query.ListTagIndexesAsync(new CatalogRequest());

        Assert.That(recorder.Prefixes, Is.EqualTo(new[] { LatticeConstants.TagIndexTreePrefix }));
    }
}
