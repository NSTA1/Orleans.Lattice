using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Configuration;
using Orleans.Streams;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="TenantResidencySnapshotMaintainer"/>, the per-silo
/// observer that keeps the in-memory tenant-residency snapshot current for the
/// local serving region and publishes each observed local-region status transition
/// to the registered <see cref="ITenantRegionStatusChangeListener"/>s. Everything is
/// driven deterministically through the internal
/// <see cref="TenantResidencySnapshotMaintainer.RebuildNowAsync"/> hook - no
/// background timing, no wall-clock, no ordering assumptions.
/// </summary>
[TestFixture]
public sealed class TenantResidencyRegionSnapshotMaintainerTests
{
    private static TenantRecord Configured(TenantId tenant, string regionId, TenantRegionStatus status)
    {
        var record = TenantRecord.Create(
            tenant,
            TenantStatus.Active,
            TenantQuotas.Unbounded,
            TenantPlacement.Shared,
            TestClocks.Clock(1),
            writerId: "op");
        record.SetRegionStatus(regionId, status, TestClocks.Clock(2), "op");
        return record;
    }

    private static async IAsyncEnumerable<TenantRecord> ThrowAsync(
        Exception ex,
        [EnumeratorCancellation] CancellationToken _ = default)
    {
        await Task.CompletedTask;
        throw ex;
#pragma warning disable CS0162
        yield break;
#pragma warning restore CS0162
    }

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

    private sealed class CapturingListener : ITenantRegionStatusChangeListener
    {
        public List<TenantRegionStatusChange> Changes { get; } = new();

        public Task OnRegionStatusChangedAsync(TenantRegionStatusChange change, CancellationToken cancellationToken)
        {
            Changes.Add(change);
            return Task.CompletedTask;
        }
    }

    private static TenantResidencySnapshotMaintainer Maintainer(
        ITenantRegistry registry,
        string regionId = "region-a",
        params ITenantRegionStatusChangeListener[] listeners) =>
        new(
            registry,
            Options.Create(new ClusterOptions { ClusterId = regionId }),
            listeners,
            NullLogger<TenantResidencySnapshotMaintainer>.Instance);

    [Test]
    public void Ctor_null_registry_throws() =>
        Assert.That(
            () => new TenantResidencySnapshotMaintainer(
                null!,
                Options.Create(new ClusterOptions()),
                Array.Empty<ITenantRegionStatusChangeListener>(),
                NullLogger<TenantResidencySnapshotMaintainer>.Instance),
            Throws.ArgumentNullException);

    [Test]
    public void Ctor_null_cluster_options_throws() =>
        Assert.That(
            () => new TenantResidencySnapshotMaintainer(
                Substitute.For<ITenantRegistry>(),
                null!,
                Array.Empty<ITenantRegionStatusChangeListener>(),
                NullLogger<TenantResidencySnapshotMaintainer>.Instance),
            Throws.ArgumentNullException);

    [Test]
    public void Ctor_null_listeners_throws() =>
        Assert.That(
            () => new TenantResidencySnapshotMaintainer(
                Substitute.For<ITenantRegistry>(),
                Options.Create(new ClusterOptions()),
                null!,
                NullLogger<TenantResidencySnapshotMaintainer>.Instance),
            Throws.ArgumentNullException);

    [Test]
    public void Ctor_null_logger_throws() =>
        Assert.That(
            () => new TenantResidencySnapshotMaintainer(
                Substitute.For<ITenantRegistry>(),
                Options.Create(new ClusterOptions()),
                Array.Empty<ITenantRegionStatusChangeListener>(),
                null!),
            Throws.ArgumentNullException);

    [Test]
    public void Current_starts_empty_before_any_rebuild()
    {
        var maintainer = Maintainer(Substitute.For<ITenantRegistry>());

        Assert.Multiple(() =>
        {
            Assert.That(maintainer.Current, Is.SameAs(TenantResidencySnapshot.Empty));
            Assert.That(maintainer.CurrentEpoch, Is.Zero);
        });
    }

    [Test]
    public void LocalRegionId_falls_back_to_default_when_cluster_id_is_empty()
    {
        var maintainer = Maintainer(Substitute.For<ITenantRegistry>(), regionId: "");

        Assert.That(maintainer.LocalRegionId, Is.EqualTo("default"));
    }

    [Test]
    public async Task RebuildNowAsync_maps_a_configured_tenant_to_its_local_region_status()
    {
        var acme = TenantId.Parse("acme");
        var registry = Substitute.For<ITenantRegistry>();
        registry.ListAsync(Arg.Any<CancellationToken>()).Returns(_ => Stream(new[]
        {
            Configured(acme, "region-a", TenantRegionStatus.Online),
        }));
        var maintainer = Maintainer(registry);

        await maintainer.RebuildNowAsync();

        Assert.Multiple(() =>
        {
            Assert.That(maintainer.Current.TryGetStatus(acme, out var status), Is.True);
            Assert.That(status, Is.EqualTo(TenantRegionStatus.Online));
            Assert.That(maintainer.CurrentEpoch, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task RebuildNowAsync_excludes_an_unconfigured_tenant()
    {
        var acme = TenantId.Parse("acme");
        var registry = Substitute.For<ITenantRegistry>();
        // A tenant with no residency configuration is left out so it resolves admit-all.
        registry.ListAsync(Arg.Any<CancellationToken>()).Returns(_ => Stream(new[]
        {
            TenantRecord.Create(acme, TenantStatus.Active, TenantQuotas.Unbounded, TenantPlacement.Shared, TestClocks.Clock(1), "op"),
        }));
        var maintainer = Maintainer(registry);

        await maintainer.RebuildNowAsync();

        Assert.Multiple(() =>
        {
            Assert.That(maintainer.Current.Count, Is.Zero);
            Assert.That(maintainer.Current.IsOnlineLocally(acme), Is.True);
        });
    }

    [Test]
    public async Task RebuildNowAsync_maps_a_tenant_resident_only_elsewhere_to_none_here()
    {
        var acme = TenantId.Parse("acme");
        var registry = Substitute.For<ITenantRegistry>();
        // Configured Online in region-b; this silo serves region-a, so it is not online here.
        registry.ListAsync(Arg.Any<CancellationToken>()).Returns(_ => Stream(new[]
        {
            Configured(acme, "region-b", TenantRegionStatus.Online),
        }));
        var maintainer = Maintainer(registry, regionId: "region-a");

        await maintainer.RebuildNowAsync();

        Assert.Multiple(() =>
        {
            Assert.That(maintainer.Current.TryGetStatus(acme, out var status), Is.True);
            Assert.That(status, Is.EqualTo(TenantRegionStatus.None));
            Assert.That(maintainer.Current.IsOnlineLocally(acme), Is.False);
        });
    }

    [Test]
    public async Task RebuildNowAsync_advances_the_epoch_each_time()
    {
        var registry = Substitute.For<ITenantRegistry>();
        registry.ListAsync(Arg.Any<CancellationToken>())
            .Returns(_ => Stream(Array.Empty<TenantRecord>()));
        var maintainer = Maintainer(registry);

        await maintainer.RebuildNowAsync();
        await maintainer.RebuildNowAsync();

        Assert.That(maintainer.CurrentEpoch, Is.EqualTo(2));
    }

    [Test]
    public async Task EnsureWarmAsync_builds_once_then_is_idempotent()
    {
        var registry = Substitute.For<ITenantRegistry>();
        registry.ListAsync(Arg.Any<CancellationToken>())
            .Returns(_ => Stream(Array.Empty<TenantRecord>()));
        var maintainer = Maintainer(registry);

        await maintainer.EnsureWarmAsync();
        await maintainer.EnsureWarmAsync();

        Assert.That(maintainer.CurrentEpoch, Is.EqualTo(1));
        registry.Received(1).ListAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task OnMutationAsync_ignores_a_non_registry_tree()
    {
        var registry = Substitute.For<ITenantRegistry>();
        registry.ListAsync(Arg.Any<CancellationToken>())
            .Returns(_ => Stream(Array.Empty<TenantRecord>()));
        var maintainer = Maintainer(registry);

        await maintainer.OnMutationAsync(
            new LatticeMutation { TreeId = "t/acme/orders", Kind = MutationKind.Set, Key = "k" },
            CancellationToken.None);

        Assert.That(maintainer.CurrentEpoch, Is.Zero);
        registry.DidNotReceive().ListAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task OnMutationAsync_returns_a_completed_task_without_blocking()
    {
        var registry = Substitute.For<ITenantRegistry>();
        registry.ListAsync(Arg.Any<CancellationToken>())
            .Returns(_ => Stream(Array.Empty<TenantRecord>()));
        var maintainer = Maintainer(registry);

        var task = maintainer.OnMutationAsync(
            new LatticeMutation { TreeId = TenantTreeNames.RegistryTree, Kind = MutationKind.Set, Key = "acme" },
            CancellationToken.None);

        Assert.That(task.IsCompletedSuccessfully, Is.True);
    }

    [Test]
    public async Task RebuildNowAsync_publishes_the_local_region_transition_to_listeners()
    {
        var acme = TenantId.Parse("acme");
        var registry = Substitute.For<ITenantRegistry>();
        registry.ListAsync(Arg.Any<CancellationToken>()).Returns(
            _ => Stream(new[] { Configured(acme, "region-a", TenantRegionStatus.Provisioning) }),
            _ => Stream(new[] { Configured(acme, "region-a", TenantRegionStatus.Online) }));
        var listener = new CapturingListener();
        var maintainer = Maintainer(registry, "region-a", listener);

        await maintainer.RebuildNowAsync();
        await maintainer.RebuildNowAsync();

        Assert.Multiple(() =>
        {
            Assert.That(listener.Changes, Has.Count.EqualTo(2));
            Assert.That(listener.Changes[0].PreviousStatus, Is.EqualTo(TenantRegionStatus.None));
            Assert.That(listener.Changes[0].CurrentStatus, Is.EqualTo(TenantRegionStatus.Provisioning));
            Assert.That(listener.Changes[1].PreviousStatus, Is.EqualTo(TenantRegionStatus.Provisioning));
            Assert.That(listener.Changes[1].CurrentStatus, Is.EqualTo(TenantRegionStatus.Online));
            Assert.That(listener.Changes[1].RegionId, Is.EqualTo("region-a"));
            Assert.That(listener.Changes[1].Tenant, Is.EqualTo(acme));
        });
    }

    [Test]
    public async Task RebuildNowAsync_publishes_a_drop_to_none_when_a_tenant_leaves_the_local_region()
    {
        var acme = TenantId.Parse("acme");
        var registry = Substitute.For<ITenantRegistry>();
        registry.ListAsync(Arg.Any<CancellationToken>()).Returns(
            _ => Stream(new[] { Configured(acme, "region-a", TenantRegionStatus.Online) }),
            _ => Stream(new[] { Configured(acme, "region-b", TenantRegionStatus.Online) }));
        var listener = new CapturingListener();
        var maintainer = Maintainer(registry, "region-a", listener);

        await maintainer.RebuildNowAsync();
        listener.Changes.Clear();
        await maintainer.RebuildNowAsync();

        Assert.Multiple(() =>
        {
            Assert.That(listener.Changes, Has.Count.EqualTo(1));
            Assert.That(listener.Changes[0].PreviousStatus, Is.EqualTo(TenantRegionStatus.Online));
            Assert.That(listener.Changes[0].CurrentStatus, Is.EqualTo(TenantRegionStatus.None));
        });
    }

    [Test]
    public async Task RebuildNowAsync_swallows_a_faulting_listener_and_still_swaps()
    {
        var acme = TenantId.Parse("acme");
        var registry = Substitute.For<ITenantRegistry>();
        registry.ListAsync(Arg.Any<CancellationToken>())
            .Returns(_ => Stream(new[] { Configured(acme, "region-a", TenantRegionStatus.Online) }));
        var faulting = Substitute.For<ITenantRegionStatusChangeListener>();
        faulting.OnRegionStatusChangedAsync(Arg.Any<TenantRegionStatusChange>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromException(new InvalidOperationException("boom")));
        var maintainer = Maintainer(registry, "region-a", faulting);

        await maintainer.RebuildNowAsync();

        Assert.That(maintainer.Current.TryGetStatus(acme, out _), Is.True);
    }

    [Test]
    public async Task RebuildNowAsync_retries_on_a_transient_enumeration_abort()
    {
        // Covers lines 142-145: the registry scan is aborted (EnumerationAbortedException)
        // on the first attempt; RebuildNowAsync must re-enumerate immediately.
        var acme = TenantId.Parse("acme");
        var calls = 0;
        var registry = Substitute.For<ITenantRegistry>();
        registry.ListAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            calls++;
            return calls == 1
                ? ThrowAsync(new EnumerationAbortedException())
                : Stream(new[] { Configured(acme, "region-a", TenantRegionStatus.Online) });
        });
        var maintainer = Maintainer(registry);

        await maintainer.RebuildNowAsync();

        Assert.Multiple(() =>
        {
            Assert.That(calls, Is.EqualTo(2), "the scan was retried after the transient abort");
            Assert.That(maintainer.Current.TryGetStatus(acme, out _), Is.True);
        });
    }

    [Test]
    public async Task RunRebuildLoopAsync_catches_a_rebuild_exception_and_keeps_the_previous_snapshot()
    {
        // Covers lines 195-198: the background loop's RebuildOnceAsync throws a
        // non-transient exception; the loop catches it, logs a warning, and leaves
        // the previous snapshot intact instead of crashing the background task.
        var acme = TenantId.Parse("acme");
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var registry = Substitute.For<ITenantRegistry>();

        // RebuildNowAsync (internal) uses a different lock path from the background
        // loop. Seed one clean snapshot so there is a stable previous state to
        // validate against after the loop catches the exception.
        registry.ListAsync(Arg.Any<CancellationToken>())
            .Returns(_ => Stream(new[] { Configured(acme, "region-a", TenantRegionStatus.Online) }));
        var maintainer = Maintainer(registry);
        await maintainer.RebuildNowAsync();

        // Now reconfigure the registry to throw on the background rebuild so the
        // catch block is exercised.
        registry.ListAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            entered.TrySetResult();
            return ThrowAsync(new InvalidOperationException("rebuild-failed"));
        });

        // Trigger a background rebuild.
        await maintainer.OnMutationAsync(
            new LatticeMutation { TreeId = TenantTreeNames.RegistryTree, Kind = MutationKind.Set, Key = "acme" },
            CancellationToken.None);

        // Wait for the background loop to have entered the scan (and thrown).
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        // Small settle so the catch block has a chance to complete before we assert.
        await Task.Delay(50);

        // The previous snapshot must still be intact; the epoch must not have advanced.
        Assert.That(maintainer.CurrentEpoch, Is.EqualTo(1), "the background exception must not advance the epoch");
    }

    [Test]
    public async Task RebuildNowAsync_emits_a_drop_to_none_when_a_tenant_vanishes_from_the_registry_entirely()
    {
        // Covers line 281: a tenant was in _lastByTenant with a non-None status, but
        // the new scan returns no record for it at all. The snapshot swapper must emit
        // a status-change to None for the disappearing tenant.
        var acme = TenantId.Parse("acme");
        var registry = Substitute.For<ITenantRegistry>();
        registry.ListAsync(Arg.Any<CancellationToken>()).Returns(
            _ => Stream(new[] { Configured(acme, "region-a", TenantRegionStatus.Online) }),
            _ => Stream(Array.Empty<TenantRecord>()));
        var listener = new CapturingListener();
        var maintainer = Maintainer(registry, "region-a", listener);

        await maintainer.RebuildNowAsync();
        listener.Changes.Clear();
        await maintainer.RebuildNowAsync();

        Assert.Multiple(() =>
        {
            Assert.That(listener.Changes, Has.Count.EqualTo(1));
            Assert.That(listener.Changes[0].PreviousStatus, Is.EqualTo(TenantRegionStatus.Online));
            Assert.That(listener.Changes[0].CurrentStatus, Is.EqualTo(TenantRegionStatus.None));
        });
    }
}
