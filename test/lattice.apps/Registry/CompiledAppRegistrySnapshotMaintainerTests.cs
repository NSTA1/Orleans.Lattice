using Microsoft.Extensions.Logging.Abstractions;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// Unit tests for <see cref="CompiledAppRegistrySnapshotMaintainer"/> over a real
/// <see cref="AppRegistry"/> on an in-memory store (no cluster): the warm-up build, the
/// monotonic epoch, change-feed invalidation filtered to the reserved registry tree, and
/// the atomic snapshot swap. Background rebuilds are awaited through
/// <see cref="CompiledAppRegistrySnapshotMaintainer.BackgroundRebuild"/>, never polled.
/// </summary>
[TestFixture]
public sealed class CompiledAppRegistrySnapshotMaintainerTests
{
    private static LatticeMutation RegistryMutation => new() { TreeId = AppRegistryTreeNames.RegistryTree };

    private static (CompiledAppRegistrySnapshotMaintainer Maintainer, AppRegistry Registry) Create(ManualTimeProvider? time = null)
    {
        var registry = AppRegistryTestData.CreateRegistry(new InMemoryAppRegistryStore());
        var maintainer = new CompiledAppRegistrySnapshotMaintainer(
            registry, NullLogger<CompiledAppRegistrySnapshotMaintainer>.Instance, time);
        return (maintainer, registry);
    }

    [Test]
    public void Fresh_maintainer_starts_cold_at_epoch_zero()
    {
        var (maintainer, _) = Create();

        Assert.That(maintainer.CurrentEpoch, Is.EqualTo(0));
        Assert.That(maintainer.Current, Is.SameAs(CompiledAppRegistrySnapshot.Empty));
        Assert.That(maintainer.LastRebuildUtc, Is.Null);
        Assert.That(maintainer.BackgroundRebuild.IsCompleted, Is.True);
    }

    [Test]
    public async Task EnsureWarmAsync_builds_once_and_is_idempotent()
    {
        var time = new ManualTimeProvider(AppRegistryTestData.Start);
        var (maintainer, registry) = Create(time);
        await registry.InstallAsync(AppRegistryTestData.Request());

        await maintainer.EnsureWarmAsync();
        await maintainer.EnsureWarmAsync();

        Assert.That(maintainer.CurrentEpoch, Is.EqualTo(1));
        Assert.That(maintainer.Current.Epoch, Is.EqualTo(1));
        Assert.That(maintainer.Current.TryGet(TenantId.Default, AppRegistryTestData.Slug, out _), Is.True);
        Assert.That(maintainer.LastRebuildUtc, Is.EqualTo(AppRegistryTestData.Start));
    }

    [Test]
    public async Task A_registry_write_invalidates_the_projection_on_the_change_feed()
    {
        var (maintainer, registry) = Create();
        await registry.InstallAsync(AppRegistryTestData.Request());
        await maintainer.EnsureWarmAsync();
        var before = maintainer.Current;
        Assert.That(before.GetEnabledTenantApps(TenantId.Default), Is.Empty);

        await registry.EnableAsync(TenantId.Default, AppRegistryTestData.Slug);
        await maintainer.OnMutationAsync(RegistryMutation, CancellationToken.None);
        await maintainer.BackgroundRebuild;

        Assert.That(maintainer.CurrentEpoch, Is.EqualTo(2));
        Assert.That(maintainer.Current, Is.Not.SameAs(before), "the snapshot is swapped, not mutated");
        Assert.That(before.GetEnabledTenantApps(TenantId.Default), Is.Empty, "a published snapshot is immutable");
        Assert.That(maintainer.Current.GetEnabledTenantApps(TenantId.Default).Single().Slug, Is.EqualTo(AppRegistryTestData.Slug));
    }

    [Test]
    public async Task A_mutation_on_another_tree_does_not_schedule_a_rebuild()
    {
        var (maintainer, _) = Create();
        await maintainer.EnsureWarmAsync();
        var scheduled = maintainer.BackgroundRebuild;

        await maintainer.OnMutationAsync(new LatticeMutation { TreeId = "a/notes/pages" }, CancellationToken.None);
        await maintainer.OnMutationAsync(new LatticeMutation { TreeId = "sys-tenant-registry" }, CancellationToken.None);
        await maintainer.OnMutationAsync(new LatticeMutation { TreeId = AppRegistryTreeNames.RegistryTree + "-history" }, CancellationToken.None);

        Assert.That(maintainer.BackgroundRebuild, Is.SameAs(scheduled), "no background rebuild was started");
        Assert.That(maintainer.CurrentEpoch, Is.EqualTo(1));
    }

    [Test]
    public void IsRegistryMutation_matches_only_the_registry_tree()
    {
        Assert.That(CompiledAppRegistrySnapshotMaintainer.IsRegistryMutation(RegistryMutation), Is.True);
        Assert.That(CompiledAppRegistrySnapshotMaintainer.IsRegistryMutation(new LatticeMutation { TreeId = "sys-app-registry2" }), Is.False);
        Assert.That(CompiledAppRegistrySnapshotMaintainer.IsRegistryMutation(new LatticeMutation { TreeId = "SYS-APP-REGISTRY" }), Is.False);
    }

    [Test]
    public async Task A_burst_of_registry_mutations_coalesces_into_one_run_plus_one_follow_up()
    {
        var registry = new ControllableRegistry(AppRegistryTestData.CreateRegistry(new InMemoryAppRegistryStore()));
        var maintainer = new CompiledAppRegistrySnapshotMaintainer(registry, NullLogger<CompiledAppRegistrySnapshotMaintainer>.Instance);
        await registry.InstallAsync(AppRegistryTestData.Request());
        await maintainer.EnsureWarmAsync();
        await registry.EnableAsync(TenantId.Default, AppRegistryTestData.Slug);

        // Hold the first background scan open so every later mutation lands while it is
        // in flight; the coalescing contract then fixes the run count exactly.
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        registry.Gate = release.Task;
        for (var i = 0; i < 10; i++)
        {
            await maintainer.OnMutationAsync(RegistryMutation, CancellationToken.None);
        }

        release.SetResult();
        await maintainer.BackgroundRebuild;

        Assert.That(maintainer.CurrentEpoch, Is.EqualTo(3), "warm-up, the in-flight run, and exactly one queued follow-up");
        Assert.That(maintainer.Current.GetEnabledTenantApps(TenantId.Default), Has.Count.EqualTo(1));
    }

    [Test]
    public async Task RebuildNowAsync_advances_the_epoch_and_reflects_the_registry()
    {
        var (maintainer, registry) = Create();

        Assert.That(await maintainer.RebuildNowAsync(), Is.EqualTo(1));
        Assert.That(maintainer.Current.Count, Is.EqualTo(0));

        await registry.InstallAsync(AppRegistryTestData.Request());

        Assert.That(await maintainer.RebuildNowAsync(), Is.EqualTo(2));
        Assert.That(maintainer.Current.Count, Is.EqualTo(1));
        Assert.That(maintainer.Current.Epoch, Is.EqualTo(2));
    }

    [Test]
    public async Task A_failed_background_rebuild_keeps_the_previous_snapshot()
    {
        var registry = new ControllableRegistry(AppRegistryTestData.CreateRegistry(new InMemoryAppRegistryStore()));
        var maintainer = new CompiledAppRegistrySnapshotMaintainer(registry, NullLogger<CompiledAppRegistrySnapshotMaintainer>.Instance);
        await maintainer.EnsureWarmAsync();
        var warm = maintainer.Current;

        registry.Fail = true;
        await maintainer.OnMutationAsync(RegistryMutation, CancellationToken.None);
        await maintainer.BackgroundRebuild;

        Assert.That(maintainer.Current, Is.SameAs(warm));
        Assert.That(maintainer.CurrentEpoch, Is.EqualTo(1));
    }

    [Test]
    public void Constructor_null_arguments_throw()
    {
        var registry = AppRegistryTestData.CreateRegistry(new InMemoryAppRegistryStore());

        Assert.That(() => new CompiledAppRegistrySnapshotMaintainer(null!, NullLogger<CompiledAppRegistrySnapshotMaintainer>.Instance), Throws.ArgumentNullException);
        Assert.That(() => new CompiledAppRegistrySnapshotMaintainer(registry, null!), Throws.ArgumentNullException);
    }

    /// <summary>
    /// Delegates to a real registry, but lets a test hold a scan open on
    /// <see cref="Gate"/> or make it fail.
    /// </summary>
    private sealed class ControllableRegistry(IAppRegistry inner) : IAppRegistry
    {
        public bool Fail { get; set; }

        public Task? Gate { get; set; }

        public async IAsyncEnumerable<AppRegistryRecord> ListAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            if (Gate is { } gate)
            {
                await gate;
            }

            if (Fail)
            {
                throw new InvalidOperationException("scan failed");
            }

            await foreach (var record in inner.ListAsync(cancellationToken))
            {
                yield return record;
            }
        }

        public Task<AppRegistryRecord?> GetAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken = default) => inner.GetAsync(tenant, slug, cancellationToken);

        public IAsyncEnumerable<AppRegistryRecord> ListForTenantAsync(TenantId tenant, CancellationToken cancellationToken = default) => inner.ListForTenantAsync(tenant, cancellationToken);

        public Task<AppRegistryTransitionResult> InstallAsync(AppRegistryInstallRequest request, CancellationToken cancellationToken = default) => inner.InstallAsync(request, cancellationToken);

        public Task<AppRegistryTransitionResult> UpgradeAsync(AppRegistryInstallRequest request, CancellationToken cancellationToken = default) => inner.UpgradeAsync(request, cancellationToken);

        public Task<AppRegistryTransitionResult> EnableAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken = default) => inner.EnableAsync(tenant, slug, cancellationToken);

        public Task<AppRegistryTransitionResult> DisableAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken = default) => inner.DisableAsync(tenant, slug, cancellationToken);

        public Task<AppRegistryTransitionResult> UninstallAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken = default) => inner.UninstallAsync(tenant, slug, cancellationToken);

        public Task<IReadOnlyList<AppTreeOwnershipConflict>> GetTreeOwnershipConflictsAsync(TenantId tenant, AppManifest manifest, AppProvenance provenance, CancellationToken cancellationToken = default) =>
            inner.GetTreeOwnershipConflictsAsync(tenant, manifest, provenance, cancellationToken);
    }
}
