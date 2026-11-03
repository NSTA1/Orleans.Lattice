using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// The leased fast path of <see cref="CompiledAppRegistrySnapshotMaintainer"/> (#5713). While a silo
/// holds a live lease from the registry epoch grain, which pushes every advance to it before the
/// registry write is acknowledged, a grant evaluation trusts the snapshot with no epoch read and no
/// allocation. A lapsed or missing lease drops back to the per-evaluation read (#5595), and revocation
/// still holds from the first evaluation after the write is acknowledged. Every lease runs on a
/// <see cref="FakeTimeProvider"/>, so it lapses only when a test advances the clock.
/// </summary>
[TestFixture]
public sealed class CompiledAppRegistrySnapshotMaintainerLeaseTests
{
    private static readonly TimeSpan Lease = TimeSpan.FromSeconds(10);
    private static LatticeMutation RegistryMutation => new() { TreeId = AppRegistryTreeNames.RegistryTree };

    private FakeTimeProvider _time = null!;
    private AppRegistry _registry = null!;
    private InMemoryAppRegistryEpochSource _epoch = null!;

    [SetUp]
    public void SetUp()
    {
        _time = new FakeTimeProvider(AppRegistryTestData.Start);
        _registry = AppRegistryTestData.CreateRegistry(new InMemoryAppRegistryStore());
        _epoch = new InMemoryAppRegistryEpochSource();
    }

    [Test]
    public async Task A_leased_current_snapshot_is_evaluated_with_no_epoch_read()
    {
        await _registry.InstallAsync(AppRegistryTestData.Request());
        await _registry.EnableAsync(TenantId.Default, AppRegistryTestData.Slug);
        var silo = await LeasedSiloAsync();
        var evaluator = new AppRoleGrantEvaluator(silo, source: null, gate: null);
        var reads = _epoch.Reads;

        for (var i = 0; i < 100; i++)
        {
            var pending = evaluator.GetSnapshotAsync(CancellationToken.None);
            Assert.That(pending.IsCompletedSuccessfully, Is.True, "a leased evaluation completes synchronously");
            Assert.That(pending.Result.GetEnabledTenantApps(TenantId.Default), Has.Count.EqualTo(1));
        }

        Assert.That(_epoch.Reads, Is.EqualTo(reads), "no evaluation reads the epoch while the lease is live");
    }

    [Test]
    public async Task A_leased_evaluation_allocates_nothing()
    {
        var silo = await LeasedSiloAsync();
        var evaluator = new AppRoleGrantEvaluator(silo, source: null, gate: null);

        var growth = AllocationProbe.Growth(
            _ => evaluator,
            static (e, count) =>
            {
                for (var i = 0; i < count; i++)
                {
                    AllocationProbe.ScalarSink += e.GetSnapshotAsync(CancellationToken.None).Result.Epoch;
                }
            },
            smallSize: 16,
            largeSize: 1_016);

        Assert.That(growth, Is.Zero, "a leased evaluation must not allocate");
    }

    [Test]
    public async Task An_expired_lease_falls_back_to_the_epoch_read()
    {
        var silo = await LeasedSiloAsync();
        var reads = _epoch.Reads;

        _time.Advance(Lease);
        await silo.EnsureCurrentAsync();

        Assert.Multiple(() =>
        {
            Assert.That(silo.IsLeasedCurrent, Is.False, "an expired lease drops the fast path");
            Assert.That(_epoch.Reads, Is.EqualTo(reads + 1), "the evaluation reads the epoch instead");
        });
    }

    [Test]
    public async Task A_push_never_extends_an_expired_lease()
    {
        var silo = await LeasedSiloAsync();
        _time.Advance(Lease);

        silo.ObserveEpoch(_epoch.Current with { Version = _epoch.Current.Version + 1 });
        await silo.BackgroundRebuild;

        Assert.That(silo.IsLeasedCurrent, Is.False, "only a granted lease extends the fast path");
    }

    [Test]
    public async Task A_silo_that_never_leased_reads_the_epoch_on_every_evaluation()
    {
        var silo = CreateSilo();
        await silo.EnsureCurrentAsync();
        var reads = _epoch.Reads;

        await silo.EnsureCurrentAsync();
        await silo.EnsureCurrentAsync();

        Assert.Multiple(() =>
        {
            Assert.That(silo.IsLeasedCurrent, Is.False);
            Assert.That(_epoch.Reads, Is.EqualTo(reads + 2));
        });
    }

    [Test]
    public async Task A_revocation_pushed_before_it_is_acknowledged_is_honoured_by_the_next_evaluation()
    {
        await _registry.InstallAsync(AppRegistryTestData.Request());
        await _registry.EnableAsync(TenantId.Default, AppRegistryTestData.Slug);
        var committing = await LeasedSiloAsync();
        var other = await LeasedSiloAsync();
        var otherEvaluator = new AppRoleGrantEvaluator(other, source: null, gate: null);
        Assert.That((await otherEvaluator.GetSnapshotAsync(CancellationToken.None)).GetEnabledTenantApps(TenantId.Default), Has.Count.EqualTo(1));

        // The epoch grain pushes the advance to every leased silo before the write is acknowledged.
        await _registry.DisableAsync(TenantId.Default, AppRegistryTestData.Slug);
        await committing.OnMutationAsync(RegistryMutation, CancellationToken.None);
        other.ObserveEpoch(_epoch.Current);

        Assert.That(
            (await otherEvaluator.GetSnapshotAsync(CancellationToken.None)).GetEnabledTenantApps(TenantId.Default),
            Is.Empty,
            "a pushed revocation must be honoured by the very next evaluation, whatever the lease");
    }

    [Test]
    public async Task A_pushed_advance_is_rebuilt_in_the_background_and_the_fast_path_resumes()
    {
        var silo = await LeasedSiloAsync();
        var rebuilds = silo.CurrentEpoch;

        silo.ObserveEpoch(_epoch.Current with { Version = _epoch.Current.Version + 1 });
        Assert.That(silo.IsLeasedCurrent, Is.False, "the push makes the snapshot out of date at once");
        await silo.BackgroundRebuild;

        Assert.Multiple(() =>
        {
            Assert.That(silo.CurrentEpoch, Is.GreaterThan(rebuilds));
            Assert.That(silo.IsLeasedCurrent, Is.True);
        });
    }

    [Test]
    public async Task A_stale_or_repeated_push_is_ignored()
    {
        var silo = await LeasedSiloAsync();

        silo.ObserveEpoch(_epoch.Current);

        Assert.That(silo.IsLeasedCurrent, Is.True);
    }

    [Test]
    public async Task A_lease_from_a_new_incarnation_forces_a_rebuild()
    {
        await _registry.InstallAsync(AppRegistryTestData.Request());
        var silo = await LeasedSiloAsync();

        // A write whose push was lost with the epoch grain's activation: the new incarnation tells.
        await _registry.EnableAsync(TenantId.Default, AppRegistryTestData.Slug);
        _epoch.Restart();
        silo.ApplyLease(new AppRegistryEpochLease(_epoch.Current, Lease), _time.GetTimestamp());
        await silo.EnsureCurrentAsync();

        Assert.That(silo.Current.GetEnabledTenantApps(TenantId.Default), Has.Count.EqualTo(1));
    }

    private CompiledAppRegistrySnapshotMaintainer CreateSilo() =>
        new(_registry, NullLogger<CompiledAppRegistrySnapshotMaintainer>.Instance, _time, _epoch);

    private async Task<CompiledAppRegistrySnapshotMaintainer> LeasedSiloAsync()
    {
        var silo = CreateSilo();
        silo.ApplyLease(new AppRegistryEpochLease(_epoch.Current, Lease), _time.GetTimestamp());
        await silo.BackgroundRebuild;
        await silo.EnsureCurrentAsync();
        Assert.That(silo.IsLeasedCurrent, Is.True, "precondition: the silo is leased and current");
        return silo;
    }
}
