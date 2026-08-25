using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="TenantWalPlacementResolver"/>, the active
/// <see cref="ITreePlacementResolver"/> the tenancy add-on contributes. It reads
/// the tenant's <see cref="TenantPlacement"/> from the in-memory
/// <see cref="TenantPlacementSnapshotMaintainer"/> and pins a tree to the tenant's
/// dedicated WAL provider only when one is bound; every other case resolves to
/// <see cref="TreePhysicalPlacement.Default"/> so routing is unchanged. The
/// snapshot is warmed deterministically from a substituted registry - no live
/// silo, no timing, no ordering, and (critically) no grain hop, mirroring the
/// production path that must stay free of registry re-entrancy.
/// </summary>
[TestFixture]
public sealed class TenantWalPlacementResolverTests
{
    private static TenantRecord RecordWith(TenantId tenant, TenantPlacement placement) =>
        TenantRecord.Create(
            tenant,
            TenantStatus.Active,
            TenantQuotas.Unbounded,
            placement,
            TestClocks.Clock(1),
            writerId: "test");

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

    /// <summary>
    /// Builds a resolver whose snapshot has been warmed (deterministically, via
    /// <see cref="TenantPlacementSnapshotMaintainer.RebuildNowAsync"/>) from the
    /// supplied tenant records.
    /// </summary>
    private static async Task<TenantWalPlacementResolver> WarmResolverAsync(params TenantRecord[] records)
    {
        var registry = Substitute.For<ITenantRegistry>();
        registry.ListAsync(Arg.Any<CancellationToken>()).Returns(_ => Stream(records));
        var maintainer = new TenantPlacementSnapshotMaintainer(
            registry, NullLogger<TenantPlacementSnapshotMaintainer>.Instance);
        await maintainer.RebuildNowAsync();
        return new TenantWalPlacementResolver(maintainer);
    }

    private static TenantWalPlacementResolver ColdResolver()
    {
        var registry = Substitute.For<ITenantRegistry>();
        var maintainer = new TenantPlacementSnapshotMaintainer(
            registry, NullLogger<TenantPlacementSnapshotMaintainer>.Instance);
        return new TenantWalPlacementResolver(maintainer);
    }

    [Test]
    public void TryResolveForRegistration_resolves_a_non_tenant_tree_synchronously_to_default()
    {
        var resolver = ColdResolver();

        var resolved = resolver.TryResolveForRegistration("legacy-tree", out var placement);

        Assert.Multiple(() =>
        {
            Assert.That(resolved, Is.True);
            Assert.That(placement, Is.EqualTo(TreePhysicalPlacement.Default));
        });
    }

    [Test]
    public async Task TryResolveForRegistration_pins_a_dedicated_wal_tenant_tree_synchronously()
    {
        var tenant = TenantId.Parse("acme");
        var resolver = await WarmResolverAsync(RecordWith(tenant, new TenantPlacement
        {
            WalProviderName = "wal-acme",
            DedicatedWal = true,
        }));

        // The fast (Try) path always succeeds and never awaits the async fallback -
        // the registry grain therefore makes no re-entrant hop during registration.
        var resolved = resolver.TryResolveForRegistration(
            LatticeTenantTrees.Compose(tenant, "orders"), out var placement);

        Assert.Multiple(() =>
        {
            Assert.That(resolved, Is.True);
            Assert.That(placement.WalProviderKey, Is.EqualTo("wal-acme"));
        });
    }

    [Test]
    public async Task ResolveForRegistrationAsync_non_tenant_tree_returns_default()
    {
        var resolver = ColdResolver();

        var placement = await resolver.ResolveForRegistrationAsync("legacy-tree");

        Assert.That(placement, Is.EqualTo(TreePhysicalPlacement.Default));
    }

    [Test]
    public async Task ResolveForRegistrationAsync_shared_placement_returns_default()
    {
        var tenant = TenantId.Parse("acme");
        var resolver = await WarmResolverAsync(RecordWith(tenant, TenantPlacement.Shared));

        var placement = await resolver.ResolveForRegistrationAsync(
            LatticeTenantTrees.Compose(tenant, "orders"));

        Assert.That(placement, Is.EqualTo(TreePhysicalPlacement.Default));
    }

    [Test]
    public async Task ResolveForRegistrationAsync_dedicated_wal_pins_the_named_provider_key()
    {
        var tenant = TenantId.Parse("acme");
        var resolver = await WarmResolverAsync(RecordWith(tenant, new TenantPlacement
        {
            WalProviderName = "wal-acme",
            DedicatedWal = true,
        }));

        var placement = await resolver.ResolveForRegistrationAsync(
            LatticeTenantTrees.Compose(tenant, "orders"));

        Assert.That(placement.WalProviderKey, Is.EqualTo("wal-acme"));
    }

    [Test]
    public async Task ResolveForRegistrationAsync_surfaces_the_placement_filter_alongside_the_key()
    {
        // PlacementFilter is scoped-out of the v1 registration path but the
        // resolver still surfaces it so the seam stays stable for the follow-up.
        var tenant = TenantId.Parse("acme");
        var resolver = await WarmResolverAsync(RecordWith(tenant, new TenantPlacement
        {
            WalProviderName = "wal-acme",
            PlacementFilter = "silo-group-a",
            DedicatedWal = true,
        }));

        var placement = await resolver.ResolveForRegistrationAsync(
            LatticeTenantTrees.Compose(tenant, "orders"));

        Assert.Multiple(() =>
        {
            Assert.That(placement.WalProviderKey, Is.EqualTo("wal-acme"));
            Assert.That(placement.PlacementFilter, Is.EqualTo("silo-group-a"));
        });
    }

    [Test]
    public async Task ResolveForRegistrationAsync_dedicated_flag_without_a_provider_name_returns_default()
    {
        var tenant = TenantId.Parse("acme");
        var resolver = await WarmResolverAsync(
            RecordWith(tenant, new TenantPlacement { DedicatedWal = true }));

        var placement = await resolver.ResolveForRegistrationAsync(
            LatticeTenantTrees.Compose(tenant, "orders"));

        Assert.That(placement, Is.EqualTo(TreePhysicalPlacement.Default));
    }

    [Test]
    public async Task ResolveForRegistrationAsync_provider_name_without_the_dedicated_flag_returns_default()
    {
        // A provider name is honoured only when DedicatedWal is explicitly set, so
        // an advisory-only name never diverts routing.
        var tenant = TenantId.Parse("acme");
        var resolver = await WarmResolverAsync(
            RecordWith(tenant, new TenantPlacement { WalProviderName = "wal-acme" }));

        var placement = await resolver.ResolveForRegistrationAsync(
            LatticeTenantTrees.Compose(tenant, "orders"));

        Assert.That(placement, Is.EqualTo(TreePhysicalPlacement.Default));
    }

    [Test]
    public async Task ResolveForRegistrationAsync_tenant_absent_from_the_snapshot_returns_default()
    {
        // A tenant-scoped tree registered before its tenant record is observed
        // resolves to the baseline placement - fail-safe, never a wrong provider.
        var tenant = TenantId.Parse("acme");
        var resolver = ColdResolver();

        var placement = await resolver.ResolveForRegistrationAsync(
            LatticeTenantTrees.Compose(tenant, "orders"));

        Assert.That(placement, Is.EqualTo(TreePhysicalPlacement.Default));
    }

    [Test]
    public void TryResolveForRegistration_null_tree_id_throws()
    {
        var resolver = ColdResolver();

        Assert.That(
            () => resolver.TryResolveForRegistration(null!, out _),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ResolveForRegistrationAsync_null_tree_id_throws()
    {
        var resolver = ColdResolver();

        Assert.That(
            async () => await resolver.ResolveForRegistrationAsync(null!),
            Throws.ArgumentNullException);
    }
}
