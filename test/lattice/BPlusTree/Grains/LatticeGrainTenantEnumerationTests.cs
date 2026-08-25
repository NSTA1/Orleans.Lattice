using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Pins the T3 tenant-filtered enumeration behaviour of
/// <see cref="ILattice.GetAllTreeIdsAsync"/>. With no active
/// <see cref="ITenantEnumerationFilter"/> (or an inactive one), or with no
/// active tenant stamped on <see cref="LatticeActiveTenantContext"/>, the
/// registry enumeration is returned byte-for-byte unchanged - the very same
/// list reference, so the warm (tenancy-off) path allocates nothing. Only when
/// a filter reports itself active and a tenant is present is the pruning seam
/// invoked to produce the tenant-visible subset.
/// </summary>
[TestFixture]
public class LatticeGrainTenantEnumerationTests
{
    private const string DataTreeId = "orders";

    [SetUp]
    [TearDown]
    public void ClearAmbientTenant()
    {
        // The active tenant flows on the ambient RequestContext, which is
        // logical-thread scoped; clear it so no test leaks into another.
        LatticeActiveTenantContext.Current = null;
    }

    private static (LatticeGrain grain, ILatticeRegistry registry) CreateGrain(
        IReadOnlyList<string> allTreeIds,
        ITenantEnumerationFilter? filter)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("lattice", DataTreeId));

        var grainFactory = Substitute.For<IGrainFactory>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.GetAllTreeIdsAsync().Returns(Task.FromResult(allTreeIds));

        var optionsResolver = TestOptionsResolver.ForFactory(grainFactory);
        var services = Substitute.For<IServiceProvider>();
        if (filter is not null)
        {
            services.GetService(typeof(ITenantEnumerationFilter)).Returns(filter);
        }

        var grain = new LatticeGrain(
            context, grainFactory, optionsMonitor, optionsResolver, services, NullLogger<LatticeGrain>.Instance);
        return (grain, registry);
    }

    [Test]
    public async Task GetAllTreeIdsAsync_with_no_filter_registered_returns_all_unchanged()
    {
        IReadOnlyList<string> all = ["orders", "t/contoso/orders", "users"];
        var (grain, _) = CreateGrain(all, filter: null);

        var result = await grain.GetAllTreeIdsAsync();

        Assert.That(result, Is.SameAs(all), "the tenancy-off path must not copy or re-materialise the list");
    }

    [Test]
    public async Task GetAllTreeIdsAsync_with_inactive_filter_returns_all_unchanged()
    {
        IReadOnlyList<string> all = ["orders", "t/contoso/orders", "users"];
        var filter = new RecordingEnumerationFilter(isActive: false);
        var (grain, _) = CreateGrain(all, filter);

        var result = await grain.GetAllTreeIdsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.SameAs(all));
            Assert.That(filter.FilterCallCount, Is.Zero, "an inactive filter must never be invoked");
        });
    }

    [Test]
    public async Task GetAllTreeIdsAsync_with_active_filter_but_no_active_tenant_returns_all_unchanged()
    {
        IReadOnlyList<string> all = ["orders", "t/contoso/orders", "users"];
        var filter = new RecordingEnumerationFilter(isActive: true, result: ["t/contoso/orders"]);
        var (grain, _) = CreateGrain(all, filter);

        // No LatticeActiveTenantContext scope entered: Current is null.
        var result = await grain.GetAllTreeIdsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.SameAs(all), "with no active tenant the enumeration is unchanged");
            Assert.That(filter.FilterCallCount, Is.Zero, "no tenant means the seam is skipped entirely");
        });
    }

    [Test]
    public async Task GetAllTreeIdsAsync_with_active_filter_and_active_tenant_returns_subset()
    {
        IReadOnlyList<string> all = ["orders", "t/contoso/orders", "users"];
        IReadOnlyList<string> subset = ["t/contoso/orders"];
        var filter = new RecordingEnumerationFilter(isActive: true, result: subset);
        var (grain, _) = CreateGrain(all, filter);
        var tenant = TenantId.Parse("contoso");

        IReadOnlyList<string> result;
        using (LatticeActiveTenantContext.With(tenant))
        {
            result = await grain.GetAllTreeIdsAsync();
        }

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.SameAs(subset));
            Assert.That(filter.FilterCallCount, Is.EqualTo(1));
            Assert.That(filter.LastTenant, Is.EqualTo(tenant));
            Assert.That(filter.LastInput, Is.SameAs(all), "the filter receives the unmodified registry enumeration");
        });
    }

    [Test]
    public async Task GetAllTreeIdsAsync_with_active_tenant_that_owns_no_trees_returns_empty()
    {
        IReadOnlyList<string> all = ["orders", "t/contoso/orders", "users"];
        var filter = new RecordingEnumerationFilter(isActive: true, result: []);
        var (grain, _) = CreateGrain(all, filter);

        IReadOnlyList<string> result;
        using (LatticeActiveTenantContext.With(TenantId.Parse("fabrikam")))
        {
            result = await grain.GetAllTreeIdsAsync();
        }

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.Empty);
            Assert.That(filter.FilterCallCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task GetAllTreeIdsAsync_with_empty_registry_returns_empty_unchanged()
    {
        IReadOnlyList<string> all = [];
        var (grain, _) = CreateGrain(all, filter: null);

        var result = await grain.GetAllTreeIdsAsync();

        Assert.That(result, Is.SameAs(all));
    }

    private sealed class RecordingEnumerationFilter(bool isActive, IReadOnlyList<string>? result = null)
        : ITenantEnumerationFilter
    {
        public bool IsActive { get; } = isActive;

        public int FilterCallCount { get; private set; }

        public TenantId? LastTenant { get; private set; }

        public IReadOnlyList<string>? LastInput { get; private set; }

        public IReadOnlyList<string> Filter(TenantId tenant, IReadOnlyList<string> treeIds)
        {
            FilterCallCount++;
            LastTenant = tenant;
            LastInput = treeIds;
            return result ?? treeIds;
        }
    }
}
