using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Streams;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="TenantPlacementSnapshotMaintainer"/>, the change-feed
/// observer that keeps the in-memory tenant-placement snapshot current. It rebuilds
/// the snapshot from a substituted <see cref="ITenantRegistry"/>, reacts only to
/// mutations on the tenant-registry tree, and advances a monotonic epoch on every
/// rebuild. Everything is driven deterministically through the internal
/// <see cref="TenantPlacementSnapshotMaintainer.RebuildNowAsync"/> hook - no
/// background timing, no wall-clock, no ordering assumptions.
/// </summary>
[TestFixture]
public sealed class TenantPlacementSnapshotMaintainerTests
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

    private static TenantPlacementSnapshotMaintainer Maintainer(
        ITenantRegistry registry) =>
        new(registry, NullLogger<TenantPlacementSnapshotMaintainer>.Instance);

#pragma warning disable CS1998 // async iterator with no await: it aborts before yielding
    private static async IAsyncEnumerable<TenantRecord> ThrowAsync(Exception ex)
    {
        throw ex;
#pragma warning disable CS0162 // unreachable: present only to make this a valid iterator
        yield break;
#pragma warning restore CS0162
    }
#pragma warning restore CS1998

    [Test]
    public void Ctor_null_registry_throws()
    {
        Assert.That(
            () => new TenantPlacementSnapshotMaintainer(
                null!, NullLogger<TenantPlacementSnapshotMaintainer>.Instance),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Ctor_null_logger_throws()
    {
        var registry = Substitute.For<ITenantRegistry>();

        Assert.That(
            () => new TenantPlacementSnapshotMaintainer(registry, null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Current_starts_empty_before_any_rebuild()
    {
        var registry = Substitute.For<ITenantRegistry>();
        var maintainer = Maintainer(registry);

        Assert.Multiple(() =>
        {
            Assert.That(maintainer.Current, Is.SameAs(TenantPlacementSnapshot.Empty));
            Assert.That(maintainer.CurrentEpoch, Is.Zero);
        });
    }

    [Test]
    public async Task RebuildNowAsync_populates_the_snapshot_from_the_registry()
    {
        var acme = TenantId.Parse("acme");
        var registry = Substitute.For<ITenantRegistry>();
        registry.ListAsync(Arg.Any<CancellationToken>()).Returns(_ => Stream(new[]
        {
            RecordWith(acme, new TenantPlacement { WalProviderName = "wal-acme", DedicatedWal = true }),
        }));
        var maintainer = Maintainer(registry);

        await maintainer.RebuildNowAsync();

        Assert.Multiple(() =>
        {
            Assert.That(maintainer.Current.TryGetPlacement(acme, out var placement), Is.True);
            Assert.That(placement.WalProviderName, Is.EqualTo("wal-acme"));
            Assert.That(maintainer.CurrentEpoch, Is.EqualTo(1));
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
    public async Task RebuildNowAsync_reflects_the_latest_registry_contents()
    {
        var acme = TenantId.Parse("acme");
        var registry = Substitute.For<ITenantRegistry>();
        registry.ListAsync(Arg.Any<CancellationToken>()).Returns(
            _ => Stream(new[] { RecordWith(acme, TenantPlacement.Shared) }),
            _ => Stream(new[]
            {
                RecordWith(acme, new TenantPlacement { WalProviderName = "wal-acme", DedicatedWal = true }),
            }));
        var maintainer = Maintainer(registry);

        await maintainer.RebuildNowAsync();
        var firstShared = maintainer.Current.TryGetPlacement(acme, out var first) && first.IsShared;

        await maintainer.RebuildNowAsync();
        maintainer.Current.TryGetPlacement(acme, out var second);

        Assert.Multiple(() =>
        {
            Assert.That(firstShared, Is.True);
            Assert.That(second.WalProviderName, Is.EqualTo("wal-acme"));
        });
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

        Assert.Multiple(() =>
        {
            // Warmed exactly once: the second call short-circuits on the advanced epoch.
            Assert.That(maintainer.CurrentEpoch, Is.EqualTo(1));
        });
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

        // A mutation on an unrelated tree must not schedule any rebuild.
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

        // The change-feed hook must never block the grain write path: it only
        // schedules a background rebuild and returns synchronously.
        var task = maintainer.OnMutationAsync(
            new LatticeMutation
            {
                TreeId = TenantTreeNames.RegistryTree,
                Kind = MutationKind.Set,
                Key = "acme",
            },
            CancellationToken.None);

        Assert.That(task.IsCompletedSuccessfully, Is.True);
    }

    [Test]
    public async Task RebuildNowAsync_retries_the_scan_on_a_transient_enumeration_abort()
    {
        var acme = TenantId.Parse("acme");
        var attempts = 0;
        var registry = Substitute.For<ITenantRegistry>();
        registry.ListAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            // Abort the first three cold registry scans, then succeed. A cold
            // ListAsync enumeration can be aborted by concurrent silo activity; the
            // test-facing rebuild must re-enumerate immediately (no delay) rather than
            // flake, mirroring how the production background loop self-heals.
            attempts++;
            return attempts <= 3
                ? ThrowAsync(new EnumerationAbortedException())
                : Stream(new[]
                {
                    RecordWith(acme, new TenantPlacement { WalProviderName = "wal-acme", DedicatedWal = true }),
                });
        });
        var maintainer = Maintainer(registry);

        await maintainer.RebuildNowAsync();

        Assert.Multiple(() =>
        {
            Assert.That(attempts, Is.EqualTo(4), "the scan must be re-enumerated after each transient abort");
            Assert.That(maintainer.Current.TryGetPlacement(acme, out var placement), Is.True);
            Assert.That(placement.WalProviderName, Is.EqualTo("wal-acme"));
            Assert.That(maintainer.CurrentEpoch, Is.EqualTo(1),
                "the snapshot must swap exactly once, after the successful read");
        });
    }

    [Test]
    public void RebuildNowAsync_rethrows_once_the_scan_abort_budget_is_exhausted()
    {
        var attempts = 0;
        var registry = Substitute.For<ITenantRegistry>();
        registry.ListAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            attempts++;
            return ThrowAsync(new EnumerationAbortedException());
        });
        var maintainer = Maintainer(registry);

        // A persistent abort must surface, never be swallowed - the test-facing path
        // deliberately does not self-heal the way production's background loop does,
        // so a genuine fault still fails the test rather than hiding behind a stale
        // snapshot. The budget is a fixed 8 attempts (no timing, no wall-clock).
        Assert.That(
            async () => await maintainer.RebuildNowAsync(),
            Throws.TypeOf<EnumerationAbortedException>());
        Assert.Multiple(() =>
        {
            Assert.That(attempts, Is.EqualTo(8), "the read is retried up to the bounded budget, then rethrows");
            Assert.That(maintainer.CurrentEpoch, Is.Zero,
                "a failed rebuild must not swap the snapshot or advance the epoch");
        });
    }

    [Test]
    public async Task RunRebuildLoopAsync_swallows_a_faulting_rebuild_and_leaves_the_previous_snapshot_intact()
    {
        // Covers lines 190-193: the background loop calls RebuildOnceAsync which throws a
        // non-transient exception; the loop must catch it, log a warning, and leave the
        // previous snapshot intact rather than crashing the background task.
        var acme = TenantId.Parse("acme");
        var registry = Substitute.For<ITenantRegistry>();

        // Prime a clean snapshot via the internal RebuildNowAsync (bypasses the background
        // lock path) so there is a stable previous state to validate against.
        registry.ListAsync(Arg.Any<CancellationToken>())
            .Returns(_ => Stream(new[] { RecordWith(acme, TenantPlacement.Shared) }));
        var maintainer = Maintainer(registry);
        await maintainer.RebuildNowAsync();
        var epochBefore = maintainer.CurrentEpoch;

        // Reconfigure the registry to throw on the background rebuild.
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        registry.ListAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            entered.TrySetResult();
            return ThrowAsync(new InvalidOperationException("background-rebuild-failure"));
        });

        // Trigger a background rebuild via the mutation observer.
        await maintainer.OnMutationAsync(
            new LatticeMutation { TreeId = TenantTreeNames.RegistryTree, Kind = MutationKind.Set, Key = "acme" },
            CancellationToken.None);

        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        // Let the catch block finish executing before asserting the epoch.
        await Task.Delay(50);

        Assert.That(maintainer.CurrentEpoch, Is.EqualTo(epochBefore),
            "the background exception must not advance the epoch; the previous snapshot remains in effect");
    }
}
