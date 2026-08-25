using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;

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
}
