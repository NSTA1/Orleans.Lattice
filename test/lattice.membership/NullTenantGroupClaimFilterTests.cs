using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;

namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// Unit tests for <see cref="NullTenantGroupClaimFilter"/> and its registration by
/// <c>AddLatticeMembership</c>: the inactive null seam a cluster without the
/// tenancy add-on runs with, which leaves asserted group claims untouched and is
/// displaced by a later <c>Replace</c>.
/// </summary>
[TestFixture]
public sealed class NullTenantGroupClaimFilterTests
{
    private static (ISiloBuilder Builder, IServiceCollection Services) CreateBuilder()
    {
        var services = new ServiceCollection();

        // AddLatticeMembership's ordering guard keys off the core options
        // validator that AddLattice registers; stub it so the guard passes.
        services.AddSingleton(Substitute.For<IValidateOptions<LatticeOptions>>());

        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);
        return (builder, services);
    }

    [Test]
    public void IsActive_is_false()
    {
        ITenantGroupClaimFilter filter = new NullTenantGroupClaimFilter();

        Assert.That(filter.IsActive, Is.False);
    }

    [Test]
    public void Filter_leaves_every_asserted_group_untouched()
    {
        ITenantGroupClaimFilter filter = new NullTenantGroupClaimFilter();
        var groups = new List<string> { "t/contoso/admins", "cluster-readers", "t/fabrikam/ops" };

        filter.Filter(groups);

        Assert.That(groups, Is.EqualTo(new[] { "t/contoso/admins", "cluster-readers", "t/fabrikam/ops" }));
    }

    [Test]
    public void Filter_null_collection_throws()
    {
        ITenantGroupClaimFilter filter = new NullTenantGroupClaimFilter();

        Assert.That(() => filter.Filter(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeMembership_registers_the_inactive_null_filter()
    {
        var (builder, services) = CreateBuilder();

        builder.AddLatticeMembership();

        using var provider = services.BuildServiceProvider();
        var filter = provider.GetRequiredService<ITenantGroupClaimFilter>();

        Assert.That(filter, Is.TypeOf<NullTenantGroupClaimFilter>());
        Assert.That(filter.IsActive, Is.False);
        Assert.That(services.Count(d => d.ServiceType == typeof(ITenantGroupClaimFilter)), Is.EqualTo(1));
    }

    [Test]
    public void A_later_Replace_displaces_the_null_filter()
    {
        var (builder, services) = CreateBuilder();
        builder.AddLatticeMembership();
        var active = new ActiveFilter();

        services.Replace(ServiceDescriptor.Singleton<ITenantGroupClaimFilter>(active));

        using var provider = services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<ITenantGroupClaimFilter>(), Is.SameAs(active));
    }

    [Test]
    public void The_null_filter_does_not_displace_an_earlier_registration()
    {
        var (builder, services) = CreateBuilder();
        var active = new ActiveFilter();
        services.AddSingleton<ITenantGroupClaimFilter>(active);

        builder.AddLatticeMembership();

        using var provider = services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<ITenantGroupClaimFilter>(), Is.SameAs(active));
    }

    private sealed class ActiveFilter : ITenantGroupClaimFilter
    {
        public bool IsActive => true;

        public void Filter(ICollection<string> assertedGroups)
        {
        }
    }
}
