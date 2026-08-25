using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Configuration;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="TenantResidencyResolver"/>, the hot-path residency /
/// online seam the T7 gate enforcer and T16 apply path consult. It always reports
/// <see cref="TenantResidencyResolver.IsActive"/> <c>true</c> and delegates
/// <c>IsOnlineInServingRegion</c> to the maintainer's current snapshot. Driven
/// deterministically through the maintainer's rebuild hook - no timing.
/// </summary>
[TestFixture]
public sealed class TenantResidencyResolverTests
{
    private static async IAsyncEnumerable<TenantRecord> Stream(
        IEnumerable<TenantRecord> records,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var record in records)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return record;
        }

        await Task.CompletedTask;
    }

    private static TenantRecord Configured(TenantId tenant, string regionId, TenantRegionStatus status)
    {
        var record = TenantRecord.Create(
            tenant, TenantStatus.Active, TenantQuotas.Unbounded, TenantPlacement.Shared, TestClocks.Clock(1), "op");
        record.SetRegionStatus(regionId, status, TestClocks.Clock(2), "op");
        return record;
    }

    private static TenantResidencySnapshotMaintainer Maintainer(ITenantRegistry registry, string regionId = "region-a") =>
        new(
            registry,
            Options.Create(new ClusterOptions { ClusterId = regionId }),
            Array.Empty<ITenantRegionStatusChangeListener>(),
            NullLogger<TenantResidencySnapshotMaintainer>.Instance);

    [Test]
    public void Ctor_null_maintainer_throws() =>
        Assert.That(() => new TenantResidencyResolver(null!), Throws.ArgumentNullException);

    [Test]
    public void IsActive_is_true()
    {
        var resolver = new TenantResidencyResolver(Maintainer(Substitute.For<ITenantRegistry>()));

        Assert.That(resolver.IsActive, Is.True);
    }

    [Test]
    public void IsOnlineInServingRegion_admits_an_unconfigured_tenant_before_any_rebuild()
    {
        var resolver = new TenantResidencyResolver(Maintainer(Substitute.For<ITenantRegistry>()));

        Assert.That(resolver.IsOnlineInServingRegion(TenantId.Parse("acme")), Is.True);
    }

    [Test]
    public async Task IsOnlineInServingRegion_is_true_for_a_tenant_online_here()
    {
        var acme = TenantId.Parse("acme");
        var registry = Substitute.For<ITenantRegistry>();
        registry.ListAsync(Arg.Any<CancellationToken>())
            .Returns(_ => Stream(new[] { Configured(acme, "region-a", TenantRegionStatus.Online) }));
        var maintainer = Maintainer(registry, "region-a");
        await maintainer.RebuildNowAsync();
        var resolver = new TenantResidencyResolver(maintainer);

        Assert.That(resolver.IsOnlineInServingRegion(acme), Is.True);
    }

    [Test]
    public async Task IsOnlineInServingRegion_is_false_for_a_configured_but_not_online_tenant()
    {
        var acme = TenantId.Parse("acme");
        var registry = Substitute.For<ITenantRegistry>();
        registry.ListAsync(Arg.Any<CancellationToken>())
            .Returns(_ => Stream(new[] { Configured(acme, "region-a", TenantRegionStatus.Backfilling) }));
        var maintainer = Maintainer(registry, "region-a");
        await maintainer.RebuildNowAsync();
        var resolver = new TenantResidencyResolver(maintainer);

        Assert.That(resolver.IsOnlineInServingRegion(acme), Is.False);
    }
}
