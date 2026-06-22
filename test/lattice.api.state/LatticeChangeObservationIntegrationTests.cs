using System.Text;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Integration coverage for the change-observation facade
/// (<see cref="ILatticeStateObserver"/>). Asserts delivery and field accuracy
/// per change kind, key-range filtering, fresh-tail vs. cursor resume,
/// malformed / topology-mismatched cursor handling, that a slow consumer does
/// not block a concurrent writer, and that cancellation completes the stream.
/// </summary>
[TestFixture]
[Category("Integration")]
public class LatticeChangeObservationIntegrationTests
{
    private ChangeObservationClusterFixture _fixture = null!;
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new ChangeObservationClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (_fixture is not null)
        {
            await _fixture.DisposeAsync();
        }
    }

    private async Task<IReadOnlyList<StateChangeNotification>> ObserveWhileAsync(
        StateObserveRequest request,
        int expectedCount,
        Func<Task> mutate)
    {
        var collectTask = _fixture.CollectAsync(request, expectedCount, Timeout);

        // Give the subscription a moment to seed its tail cursor before the
        // mutations land, so a fresh subscription observes exactly them.
        await Task.Delay(300);
        await mutate();

        return await collectTask;
    }

    [Test]
    public async Task delivers_set_change_with_accurate_fields()
    {
        var treeId = $"obs-set-{Guid.NewGuid():N}";
        var tree = await _fixture.RegisterTreeAsync(treeId);

        var notifications = await ObserveWhileAsync(
            new StateObserveRequest { TreeId = treeId },
            expectedCount: 1,
            () => tree.SetAsync(ChangeObservationClusterFixture.KeyAt(1), ChangeObservationClusterFixture.Utf8("v1")));

        Assert.That(notifications, Has.Count.EqualTo(1));
        var change = notifications[0];
        Assert.Multiple(() =>
        {
            Assert.That(change.Kind, Is.EqualTo(StateChangeKind.Set));
            Assert.That(change.TreeId, Is.EqualTo(treeId));
            Assert.That(change.Key, Is.EqualTo(ChangeObservationClusterFixture.KeyAt(1)));
            Assert.That(change.Category, Is.EqualTo(MutationCategory.User));
            Assert.That(change.Position, Is.Not.Empty);
        });
    }

    [Test]
    public async Task delivers_delete_change()
    {
        var treeId = $"obs-del-{Guid.NewGuid():N}";
        var tree = await _fixture.RegisterTreeAsync(treeId);
        await tree.SetAsync(ChangeObservationClusterFixture.KeyAt(2), ChangeObservationClusterFixture.Utf8("v"));

        var notifications = await ObserveWhileAsync(
            new StateObserveRequest { TreeId = treeId },
            expectedCount: 1,
            () => tree.DeleteAsync(ChangeObservationClusterFixture.KeyAt(2)));

        Assert.That(notifications, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(notifications[0].Kind, Is.EqualTo(StateChangeKind.Delete));
            Assert.That(notifications[0].Key, Is.EqualTo(ChangeObservationClusterFixture.KeyAt(2)));
        });
    }

    [Test]
    public async Task delivers_range_delete_change_with_end_bound()
    {
        var treeId = $"obs-range-{Guid.NewGuid():N}";
        var tree = await _fixture.RegisterTreeAsync(treeId);
        for (var i = 0; i < 5; i++)
        {
            await tree.SetAsync(ChangeObservationClusterFixture.KeyAt(i), ChangeObservationClusterFixture.Utf8("v"));
        }

        var notifications = await ObserveWhileAsync(
            new StateObserveRequest { TreeId = treeId },
            expectedCount: 1,
            () => tree.DeleteRangeAsync(ChangeObservationClusterFixture.KeyAt(1), ChangeObservationClusterFixture.KeyAt(3)));

        Assert.That(notifications, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(notifications[0].Kind, Is.EqualTo(StateChangeKind.DeleteRange));
            Assert.That(notifications[0].Key, Is.EqualTo(ChangeObservationClusterFixture.KeyAt(1)));
            Assert.That(notifications[0].EndExclusiveKey, Is.EqualTo(ChangeObservationClusterFixture.KeyAt(3)));
        });
    }

    [Test]
    public async Task filters_by_key_range()
    {
        var treeId = $"obs-filter-{Guid.NewGuid():N}";
        var tree = await _fixture.RegisterTreeAsync(treeId);

        // Observe only [key-00002, key-00004); writes outside must be excluded.
        var notifications = await ObserveWhileAsync(
            new StateObserveRequest
            {
                TreeId = treeId,
                StartInclusive = ChangeObservationClusterFixture.KeyAt(2),
                EndExclusive = ChangeObservationClusterFixture.KeyAt(4),
            },
            expectedCount: 2,
            async () =>
            {
                await tree.SetAsync(ChangeObservationClusterFixture.KeyAt(0), ChangeObservationClusterFixture.Utf8("v"));
                await tree.SetAsync(ChangeObservationClusterFixture.KeyAt(2), ChangeObservationClusterFixture.Utf8("v"));
                await tree.SetAsync(ChangeObservationClusterFixture.KeyAt(3), ChangeObservationClusterFixture.Utf8("v"));
                await tree.SetAsync(ChangeObservationClusterFixture.KeyAt(9), ChangeObservationClusterFixture.Utf8("v"));
            });

        Assert.That(notifications.Select(n => n.Key), Is.EquivalentTo(new[]
        {
            ChangeObservationClusterFixture.KeyAt(2),
            ChangeObservationClusterFixture.KeyAt(3),
        }));
    }

    [Test]
    public async Task fresh_subscription_starts_from_tail()
    {
        var treeId = $"obs-tail-{Guid.NewGuid():N}";
        var tree = await _fixture.RegisterTreeAsync(treeId);
        // Pre-subscription write must NOT be delivered to a fresh subscription.
        await tree.SetAsync(ChangeObservationClusterFixture.KeyAt(0), ChangeObservationClusterFixture.Utf8("old"));

        var notifications = await ObserveWhileAsync(
            new StateObserveRequest { TreeId = treeId },
            expectedCount: 1,
            () => tree.SetAsync(ChangeObservationClusterFixture.KeyAt(1), ChangeObservationClusterFixture.Utf8("new")));

        Assert.That(notifications, Has.Count.EqualTo(1));
        Assert.That(notifications[0].Key, Is.EqualTo(ChangeObservationClusterFixture.KeyAt(1)));
    }

    [Test]
    public async Task resume_from_cursor_delivers_only_new_changes()
    {
        var treeId = $"obs-resume-{Guid.NewGuid():N}";
        var tree = await _fixture.RegisterTreeAsync(treeId);

        var first = await ObserveWhileAsync(
            new StateObserveRequest { TreeId = treeId },
            expectedCount: 1,
            () => tree.SetAsync(ChangeObservationClusterFixture.KeyAt(1), ChangeObservationClusterFixture.Utf8("a")));
        Assert.That(first, Has.Count.EqualTo(1));
        var resumeToken = first[0].Position;

        // Resume from the cursor; only the change committed after it must arrive.
        var resumed = await ObserveWhileAsync(
            new StateObserveRequest { TreeId = treeId, ContinuationToken = resumeToken },
            expectedCount: 1,
            () => tree.SetAsync(ChangeObservationClusterFixture.KeyAt(2), ChangeObservationClusterFixture.Utf8("b")));

        Assert.That(resumed, Has.Count.EqualTo(1));
        Assert.That(resumed[0].Key, Is.EqualTo(ChangeObservationClusterFixture.KeyAt(2)));
    }

    [Test]
    public async Task malformed_continuation_token_throws_argument()
    {
        var treeId = $"obs-bad-{Guid.NewGuid():N}";
        await _fixture.RegisterTreeAsync(treeId);

        Assert.ThrowsAsync<ArgumentException>(async () =>
        {
            using var cts = new CancellationTokenSource(Timeout);
            await foreach (var _ in _fixture.Observer.ObserveAsync(
                new StateObserveRequest { TreeId = treeId, ContinuationToken = "!!!not-base64!!!" }, cts.Token))
            {
                break;
            }
        });
    }

    [Test]
    public async Task topology_mismatched_cursor_throws_expired()
    {
        var treeId = $"obs-topo-{Guid.NewGuid():N}";
        await _fixture.RegisterTreeAsync(treeId);

        // A token encoding two partitions cannot resume a single-partition tree.
        var token = Convert.ToBase64String(Encoding.ASCII.GetBytes("1|0|0"));

        Assert.ThrowsAsync<LatticeStateCursorExpiredException>(async () =>
        {
            using var cts = new CancellationTokenSource(Timeout);
            await foreach (var _ in _fixture.Observer.ObserveAsync(
                new StateObserveRequest { TreeId = treeId, ContinuationToken = token }, cts.Token))
            {
                break;
            }
        });
    }

    [Test]
    public void observing_missing_tree_throws_not_found()
    {
        Assert.ThrowsAsync<KeyNotFoundException>(async () =>
        {
            using var cts = new CancellationTokenSource(Timeout);
            await foreach (var _ in _fixture.Observer.ObserveAsync(
                new StateObserveRequest { TreeId = $"missing-{Guid.NewGuid():N}" }, cts.Token))
            {
                break;
            }
        });
    }

    [Test]
    public async Task slow_consumer_does_not_block_concurrent_writer()
    {
        var treeId = $"obs-bp-{Guid.NewGuid():N}";
        var tree = await _fixture.RegisterTreeAsync(treeId);

        using var subCts = new CancellationTokenSource(Timeout);
        // A deliberately slow subscriber: enumerate but sleep on each item.
        var slowConsumer = Task.Run(async () =>
        {
            try
            {
                await foreach (var _ in _fixture.Observer.ObserveAsync(
                    new StateObserveRequest { TreeId = treeId }, subCts.Token))
                {
                    await Task.Delay(200, subCts.Token);
                }
            }
            catch (OperationCanceledException)
            {
            }
        });

        await Task.Delay(300);

        // The writer must not be back-pressured by the slow subscriber.
        var start = DateTime.UtcNow;
        for (var i = 0; i < 50; i++)
        {
            await tree.SetAsync(ChangeObservationClusterFixture.KeyAt(i), ChangeObservationClusterFixture.Utf8("v"));
        }
        var elapsed = DateTime.UtcNow - start;

        await subCts.CancelAsync();
        await slowConsumer;

        Assert.That(elapsed, Is.LessThan(TimeSpan.FromSeconds(8)),
            "A slow change subscriber must not block or slow foreground writes.");
    }

    [Test]
    public async Task cancellation_completes_the_subscription()
    {
        var treeId = $"obs-cancel-{Guid.NewGuid():N}";
        await _fixture.RegisterTreeAsync(treeId);

        using var cts = new CancellationTokenSource();
        var enumerationCompleted = false;
        var task = Task.Run(async () =>
        {
            try
            {
                await foreach (var _ in _fixture.Observer.ObserveAsync(
                    new StateObserveRequest { TreeId = treeId }, cts.Token))
                {
                }
            }
            catch (OperationCanceledException)
            {
            }

            enumerationCompleted = true;
        });

        await Task.Delay(300);
        await cts.CancelAsync();
        await task.WaitAsync(Timeout);

        Assert.That(enumerationCompleted, Is.True);
    }

    [Test]
    public async Task excludes_maintenance_changes_by_default()
    {
        var treeId = $"obs-maint-default-{Guid.NewGuid():N}";
        await _fixture.RegisterTreeAsync(treeId);

        // Inject user / maintenance / user directly into the WAL: the default
        // subscription (IncludeMaintenance = false) must deliver only the two
        // user writes and silently skip the maintenance record between them.
        var notifications = await ObserveWhileAsync(
            new StateObserveRequest { TreeId = treeId },
            expectedCount: 2,
            () => _fixture.AppendWalRecordsAsync(
                treeId,
                ChangeObservationClusterFixture.UserSet("u-key-1", "v1"),
                ChangeObservationClusterFixture.MaintenanceSet("m-key", "compaction"),
                ChangeObservationClusterFixture.UserSet("u-key-2", "v2")));

        Assert.Multiple(() =>
        {
            Assert.That(notifications.Select(n => n.Key), Is.EqualTo(new[] { "u-key-1", "u-key-2" }));
            Assert.That(notifications.Select(n => n.Category),
                Is.All.EqualTo(MutationCategory.User), "maintenance writes must be filtered out by default");
        });
    }

    [Test]
    public async Task includes_maintenance_changes_when_requested()
    {
        var treeId = $"obs-maint-included-{Guid.NewGuid():N}";
        await _fixture.RegisterTreeAsync(treeId);

        var notifications = await ObserveWhileAsync(
            new StateObserveRequest { TreeId = treeId, IncludeMaintenance = true },
            expectedCount: 3,
            () => _fixture.AppendWalRecordsAsync(
                treeId,
                ChangeObservationClusterFixture.UserSet("u-key-1", "v1"),
                ChangeObservationClusterFixture.MaintenanceSet("m-key", "compaction"),
                ChangeObservationClusterFixture.UserSet("u-key-2", "v2")));

        Assert.Multiple(() =>
        {
            Assert.That(notifications.Select(n => n.Key), Is.EqualTo(new[] { "u-key-1", "m-key", "u-key-2" }));
            Assert.That(notifications.Count(n => n.Category == MutationCategory.Maintenance), Is.EqualTo(1),
                "the maintenance write must surface when IncludeMaintenance is set");
        });
    }
}
