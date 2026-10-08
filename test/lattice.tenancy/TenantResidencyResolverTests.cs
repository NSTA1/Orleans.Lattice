using System.Runtime.CompilerServices;
using NSubstitute;
using NSubstitute.ExceptionExtensions;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="TenantResidencyResolver"/>, the hot-path residency /
/// online seam the T7 gate enforcer and T16 apply path consult. It always reports
/// <see cref="TenantResidencyResolver.IsActive"/> <c>true</c>, answers from the
/// maintainer's snapshot only while that snapshot is authoritative, and otherwise
/// reports "must confirm" (issue #4051), confirming against the registry record.
/// Driven deterministically through leased or unleased maintainers - no timing.
/// </summary>
[TestFixture]
public sealed class TenantResidencyResolverTests
{
    private static readonly TenantId Acme = TenantId.Parse("acme");

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

    private static TenantRecord Unconfigured(TenantId tenant) =>
        TenantRecord.Create(tenant, TenantStatus.Active, TenantQuotas.Unbounded, TenantPlacement.Shared, TestClocks.Clock(1), "op");

    private static ITenantRegistry Registry(params TenantRecord[] records)
    {
        var registry = Substitute.For<ITenantRegistry>();
        registry.ListAsync(Arg.Any<CancellationToken>()).Returns(_ => Stream(records));
        foreach (var record in records)
        {
            registry.GetAsync(record.Id, Arg.Any<CancellationToken>()).Returns(record);
        }

        return registry;
    }

    private static async Task<TenantResidencyResolver> AuthoritativeAsync(params TenantRecord[] records)
    {
        var registry = Registry(records);
        return new TenantResidencyResolver(
            await TenantPolicyEpochTestCluster.LeasedResidencyAsync(registry, "region-a"), registry);
    }

    private static TenantResidencyResolver NonAuthoritative(params TenantRecord[] records)
    {
        var registry = Registry(records);
        return new TenantResidencyResolver(TenantPolicyEpochTestCluster.UnleasedResidency(registry, "region-a"), registry);
    }

    [Test]
    public void Ctor_null_maintainer_throws() =>
        Assert.That(() => new TenantResidencyResolver(null!, Substitute.For<ITenantRegistry>()), Throws.ArgumentNullException);

    [Test]
    public void Ctor_null_registry_throws() =>
        Assert.That(
            () => new TenantResidencyResolver(TenantPolicyEpochTestCluster.UnleasedResidency(Substitute.For<ITenantRegistry>()), null!),
            Throws.ArgumentNullException);

    [Test]
    public void IsActive_is_true()
    {
        Assert.That(NonAuthoritative().IsActive, Is.True);
    }

    [Test]
    public async Task IsOnlineInServingRegion_admits_an_unconfigured_tenant_once_authoritative()
    {
        var resolver = await AuthoritativeAsync(Unconfigured(Acme));

        Assert.That(resolver.IsOnlineInServingRegion(Acme), Is.True);
    }

    [Test]
    public async Task IsOnlineInServingRegion_is_true_for_a_tenant_online_here()
    {
        var resolver = await AuthoritativeAsync(Configured(Acme, "region-a", TenantRegionStatus.Online));

        Assert.That(resolver.IsOnlineInServingRegion(Acme), Is.True);
    }

    [Test]
    public async Task IsOnlineInServingRegion_is_false_for_a_configured_but_not_online_tenant()
    {
        var resolver = await AuthoritativeAsync(Configured(Acme, "region-a", TenantRegionStatus.Backfilling));

        Assert.That(resolver.IsOnlineInServingRegion(Acme), Is.False);
    }

    [Test]
    public void IsOnlineInServingRegion_is_false_while_the_snapshot_is_not_authoritative()
    {
        var resolver = NonAuthoritative(Unconfigured(Acme));

        Assert.That(resolver.IsOnlineInServingRegion(Acme), Is.False, "the synchronous seam cannot confirm, so it fails closed");
    }

    [Test]
    public async Task TryResolveOnline_answers_only_from_an_authoritative_snapshot()
    {
        var authoritative = await AuthoritativeAsync(Configured(Acme, "region-a", TenantRegionStatus.Online));
        var stale = NonAuthoritative(Configured(Acme, "region-a", TenantRegionStatus.Online));

        Assert.Multiple(() =>
        {
            Assert.That(authoritative.TryResolveOnline(Acme, out var online), Is.True);
            Assert.That(online, Is.True);
            Assert.That(stale.TryResolveOnline(Acme, out var staleOnline), Is.False, "a stale view must be confirmed");
            Assert.That(staleOnline, Is.False);
        });
    }

    [TestCase(TenantRegionStatus.Provisioning, true)]
    [TestCase(TenantRegionStatus.Backfilling, true)]
    [TestCase(TenantRegionStatus.Online, true)]
    [TestCase(TenantRegionStatus.Draining, true)]
    [TestCase(TenantRegionStatus.Offline, false)]
    [TestCase(TenantRegionStatus.Removed, false)]
    public async Task TryResolveSourceResidency_allows_draining_region_to_ship_final_writes(
        TenantRegionStatus sourceStatus,
        bool expectedAllowed)
    {
        var resolver = await AuthoritativeAsync(Configured(Acme, "relay", sourceStatus));

        Assert.Multiple(() =>
        {
            Assert.That(resolver.TryResolveSourceResidency(Acme, "relay", out var configured, out var allowed), Is.True);
            Assert.That(configured, Is.True);
            Assert.That(allowed, Is.EqualTo(expectedAllowed));
            Assert.That(resolver.TryResolveSourceResidency(Acme, "unknown", out _, out var unknownAllowed), Is.True);
            Assert.That(unknownAllowed, Is.False);
        });
    }

    [TestCase(TenantRegionStatus.Online, true)]
    [TestCase(TenantRegionStatus.Draining, false)]
    [TestCase(TenantRegionStatus.Offline, false)]
    public async Task ConfirmOnlineAsync_answers_from_the_registry_record(TenantRegionStatus status, bool expected)
    {
        var resolver = NonAuthoritative(Configured(Acme, "region-a", status));

        Assert.That(await resolver.ConfirmOnlineAsync(Acme), Is.EqualTo(expected));
    }

    [Test]
    public async Task ConfirmOnlineAsync_unregistered_tenant_is_not_online()
    {
        var resolver = NonAuthoritative();

        Assert.That(await resolver.ConfirmOnlineAsync(Acme), Is.False);
    }

    [Test]
    public void ConfirmOnlineAsync_registry_failure_propagates()
    {
        var registry = Substitute.For<ITenantRegistry>();
        registry.GetAsync(Acme, Arg.Any<CancellationToken>()).ThrowsAsync(new InvalidOperationException("down"));
        var resolver = new TenantResidencyResolver(TenantPolicyEpochTestCluster.UnleasedResidency(registry), registry);

        Assert.That(async () => await resolver.ConfirmOnlineAsync(Acme), Throws.InvalidOperationException);
    }

    [Test]
    public void IsOnline_applies_the_snapshot_rule_to_a_record()
    {
        var resolver = NonAuthoritative();

        Assert.Multiple(() =>
        {
            Assert.That(resolver.IsOnline(Unconfigured(Acme)), Is.True, "an unconfigured tenant is online everywhere");
            Assert.That(resolver.IsOnline(Configured(Acme, "region-a", TenantRegionStatus.Online)), Is.True);
            Assert.That(resolver.IsOnline(Configured(Acme, "region-b", TenantRegionStatus.Online)), Is.False, "configured elsewhere is not online here");
            Assert.That(() => resolver.IsOnline(null!), Throws.ArgumentNullException);
        });
    }
}
