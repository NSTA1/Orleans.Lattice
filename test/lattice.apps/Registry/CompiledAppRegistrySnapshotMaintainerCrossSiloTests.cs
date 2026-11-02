using Microsoft.Extensions.Logging.Abstractions;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// Regression tests for #5595: the compiled app-registry snapshot is per silo, and the core change feed
/// that refreshes it fires only on the silo hosting the registry leaf that committed a write. Two
/// maintainers over one registry stand in for two silos; they share one in-memory cluster epoch, and
/// only the committing "silo" ever observes the mutation. Before the fix the other silo kept the
/// snapshot it first warmed on, so the shared evaluation answered from a stale registry: it hid an app
/// that had been enabled, and kept granting one that had been disabled.
/// </summary>
[TestFixture]
public sealed class CompiledAppRegistrySnapshotMaintainerCrossSiloTests
{
    private static LatticeMutation RegistryMutation => new() { TreeId = AppRegistryTreeNames.RegistryTree };

    private AppRegistry _registry = null!;
    private InMemoryAppRegistryEpochSource _epoch = null!;
    private CompiledAppRegistrySnapshotMaintainer _committing = null!;
    private CompiledAppRegistrySnapshotMaintainer _other = null!;
    private AppRoleGrantEvaluator _otherEvaluator = null!;

    [SetUp]
    public void SetUp()
    {
        _registry = AppRegistryTestData.CreateRegistry(new InMemoryAppRegistryStore());
        _epoch = new InMemoryAppRegistryEpochSource();
        _committing = CreateSilo();
        _other = CreateSilo();
        _otherEvaluator = new AppRoleGrantEvaluator(_other, source: null, gate: null);
    }

    [Test]
    public async Task An_app_enabled_through_another_silo_is_seen_by_the_next_evaluation()
    {
        await _registry.InstallAsync(AppRegistryTestData.Request());
        await WarmBothAsync();
        Assert.That((await _otherEvaluator.GetSnapshotAsync(CancellationToken.None)).GetEnabledTenantApps(TenantId.Default), Is.Empty);

        await _registry.EnableAsync(TenantId.Default, AppRegistryTestData.Slug);
        await _committing.OnMutationAsync(RegistryMutation, CancellationToken.None);

        var snapshot = await _otherEvaluator.GetSnapshotAsync(CancellationToken.None);
        Assert.That(snapshot.GetEnabledTenantApps(TenantId.Default).Select(r => r.Slug), Is.EqualTo(new[] { AppRegistryTestData.Slug }));
    }

    [Test]
    public async Task An_app_disabled_through_another_silo_is_never_granted_afterwards()
    {
        await _registry.InstallAsync(AppRegistryTestData.Request());
        await _registry.EnableAsync(TenantId.Default, AppRegistryTestData.Slug);
        await WarmBothAsync();
        Assert.That((await _otherEvaluator.GetSnapshotAsync(CancellationToken.None)).GetEnabledTenantApps(TenantId.Default), Has.Count.EqualTo(1));

        await _registry.DisableAsync(TenantId.Default, AppRegistryTestData.Slug);
        await _committing.OnMutationAsync(RegistryMutation, CancellationToken.None);

        Assert.That(
            (await _otherEvaluator.GetSnapshotAsync(CancellationToken.None)).GetEnabledTenantApps(TenantId.Default),
            Is.Empty,
            "the revocation committed on the other silo must be honoured by the very next evaluation");
    }

    [Test]
    public async Task The_committing_silo_advances_the_cluster_epoch_before_its_write_returns()
    {
        await WarmBothAsync();
        var before = _epoch.Current;

        await _committing.OnMutationAsync(RegistryMutation, CancellationToken.None);

        Assert.That(_epoch.Current.Version, Is.EqualTo(before.Version + 1));
    }

    [Test]
    public async Task A_mutation_on_another_tree_does_not_advance_the_cluster_epoch()
    {
        await _committing.OnMutationAsync(new LatticeMutation { TreeId = "a/notes/pages" }, CancellationToken.None);

        Assert.That(_epoch.Current.Version, Is.Zero);
        Assert.That(_committing.PublishLoop.IsCompleted, Is.True);
    }

    [Test]
    public async Task An_unchanged_cluster_epoch_reuses_the_snapshot()
    {
        await WarmBothAsync();
        var warm = _other.Current;

        await _other.EnsureCurrentAsync();
        await _other.EnsureCurrentAsync();

        Assert.That(_other.Current, Is.SameAs(warm));
        Assert.That(_other.CurrentEpoch, Is.EqualTo(1));
    }

    [Test]
    public async Task A_new_epoch_incarnation_forces_a_rebuild()
    {
        await _registry.InstallAsync(AppRegistryTestData.Request());
        await WarmBothAsync();

        // A write whose advance was lost with the epoch grain's activation: only the new incarnation tells.
        await _registry.EnableAsync(TenantId.Default, AppRegistryTestData.Slug);
        _epoch.Restart();

        await _other.EnsureCurrentAsync();

        Assert.That(_other.Current.GetEnabledTenantApps(TenantId.Default), Has.Count.EqualTo(1));
    }

    [Test]
    public async Task An_unreadable_cluster_epoch_fails_closed_by_rebuilding_from_the_registry()
    {
        await _registry.InstallAsync(AppRegistryTestData.Request());
        await _registry.EnableAsync(TenantId.Default, AppRegistryTestData.Slug);
        await WarmBothAsync();

        _epoch.Fail = true;
        await _registry.DisableAsync(TenantId.Default, AppRegistryTestData.Slug);

        Assert.That((await _otherEvaluator.GetSnapshotAsync(CancellationToken.None)).GetEnabledTenantApps(TenantId.Default), Is.Empty);

        // The snapshot built while the epoch was unreadable is never trusted afterwards either.
        var epochBefore = _other.CurrentEpoch;
        _epoch.Fail = false;
        await _other.EnsureCurrentAsync();
        Assert.That(_other.CurrentEpoch, Is.EqualTo(epochBefore + 1));
    }

    [Test]
    public async Task A_failed_advance_is_retried_until_it_lands()
    {
        await WarmBothAsync();
        _epoch.FailAdvances = 2;

        await _committing.OnMutationAsync(RegistryMutation, CancellationToken.None);
        await _committing.PublishLoop;

        Assert.That(_epoch.Current.Version, Is.EqualTo(1));
        Assert.That(_epoch.FailAdvances, Is.Zero);
    }

    [Test]
    public async Task Without_a_cluster_epoch_EnsureCurrentAsync_only_warms()
    {
        var maintainer = new CompiledAppRegistrySnapshotMaintainer(_registry, NullLogger<CompiledAppRegistrySnapshotMaintainer>.Instance);

        await maintainer.EnsureCurrentAsync();
        await maintainer.EnsureCurrentAsync();

        Assert.That(maintainer.HasClusterEpoch, Is.False);
        Assert.That(maintainer.CurrentEpoch, Is.EqualTo(1));
    }

    private CompiledAppRegistrySnapshotMaintainer CreateSilo() =>
        new(_registry, NullLogger<CompiledAppRegistrySnapshotMaintainer>.Instance, timeProvider: null, epoch: _epoch);

    private async Task WarmBothAsync()
    {
        await _committing.EnsureCurrentAsync();
        await _other.EnsureCurrentAsync();
    }
}
