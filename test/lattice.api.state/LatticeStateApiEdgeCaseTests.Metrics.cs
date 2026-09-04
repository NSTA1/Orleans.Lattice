using System.Collections.Immutable;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Shared metrics sampler and metrics observer edge cases.
/// </summary>
public sealed partial class LatticeStateApiEdgeCaseTests
{
    [Test]
    public async Task Shared_metrics_delay_treats_cancellation_as_loop_shutdown()
    {
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        var delay = InvokeSamplerStaticTask("DelayAsync", TimeSpan.FromSeconds(1), cts.Token);
        await delay.WaitAsync(TimeSpan.FromSeconds(30));

        Assert.That(delay.IsCompletedSuccessfully, Is.True,
            "an already-cancelled delay must be absorbed as a clean loop shutdown, never faulted or cancelled");
    }

    [Test]
    public void Shared_metrics_identity_component_is_canonical_for_subjects_without_groups_or_claims()
    {
        var rendered = InvokeSamplerStatic<string>("BuildIdentityComponent", new LatticeSubject("alice"));

        Assert.That(rendered, Is.EqualTo("5:alice|g=|c="));
    }

    [Test]
    public void Shared_metrics_identity_component_sorts_groups_and_claims()
    {
        var subject = new LatticeSubject(
            "alice",
            ImmutableHashSet.Create(StringComparer.Ordinal, "team-z", "team-a"),
            ImmutableDictionary.CreateRange(StringComparer.Ordinal, new[]
            {
                new KeyValuePair<string, string>("role", "reader"),
                new KeyValuePair<string, string>("region", "west"),
            }));

        var rendered = InvokeSamplerStatic<string>("BuildIdentityComponent", subject);

        Assert.That(rendered, Is.EqualTo("5:alice|g=6:team-a6:team-z|c=6:region4:west4:role6:reader"));
    }

    [Test]
    public async Task Shared_metrics_subscriber_completes_when_sampling_is_cancelled()
    {
        var query = Substitute.For<ILatticeStateQuery>();
        query.ListTreesAsync(Arg.Any<CatalogRequest>(), Arg.Any<CancellationToken>())
            .Returns<Task<TreeCatalogPage>>(_ => throw new OperationCanceledException());
        var sampler = new SharedMetricsSampler(
            query,
            Options.Create(new LatticeApiStateOptions()),
            new ServiceCollection().BuildServiceProvider());

        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var snapshots = new List<IReadOnlyDictionary<string, TreeMetrics>>();
        await foreach (var snapshot in sampler.SubscribeAsync(new TreeMetricsRequest
        {
            SampleInterval = TimeSpan.FromMilliseconds(1),
        }, timeout.Token))
        {
            snapshots.Add(snapshot);
        }

        Assert.That(snapshots, Is.Empty);
    }

    [Test]
    public void Shared_metrics_builds_an_empty_active_tree_metric_without_hotness_rows()
    {
        var metrics = InvokeSamplerStatic<TreeMetrics>(
            "BuildTreeMetrics",
            "empty",
            Array.Empty<ShardStateSummary>(),
            true,
            0,
            null);

        Assert.Multiple(() =>
        {
            Assert.That(metrics.TreeId, Is.EqualTo("empty"));
            Assert.That(metrics.ShardCount, Is.Zero);
            Assert.That(metrics.MinDepth, Is.Zero);
            Assert.That(metrics.ShardHotness, Is.Empty);
        });
    }

    [Test]
    public void Metrics_observer_detects_each_tree_level_metric_change()
    {
        var original = new TreeMetrics { TreeId = "tree", Lifecycle = TreeLifecycleState.Active, ShardCount = 1 };

        Assert.Multiple(() =>
        {
            Assert.That(SameMetrics(original, original with { Lifecycle = TreeLifecycleState.SoftDeleted }), Is.False);
            Assert.That(SameMetrics(original, original with { ShardCount = 2 }), Is.False);
            Assert.That(SameMetrics(original, original with { LiveKeys = 1 }), Is.False);
            Assert.That(SameMetrics(original, original with { Tombstones = 1 }), Is.False);
            Assert.That(SameMetrics(original, original with { MinDepth = 1 }), Is.False);
            Assert.That(SameMetrics(original, original with { MaxDepth = 1 }), Is.False);
            Assert.That(SameMetrics(original, original with { ShardsSplitting = 1 }), Is.False);
            Assert.That(SameMetrics(original, original with { ViewCount = 1 }), Is.False);
            Assert.That(SameMetrics(original, original with { ViewLagTotal = 1 }), Is.False);
        });
    }
}
