using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Coverage for <see cref="SharedMetricsSampler"/>'s shared-loop resilience and
/// signature-keying edges.
///
/// The loop is shared by every subscriber on a signature, so a fault in it is
/// not one subscriber's problem: a transient sampling failure that tore the loop
/// down would disconnect every attached metrics consumer at once. Equally, the
/// signature must fold in the whole resolved identity - claims included - because
/// two subjects that differ only by claim can differ in what they may read, and
/// coalescing them onto one loop would leak the higher-privilege map.
/// </summary>
public partial class SharedMetricsSamplerTests
{
    private const string ResilienceTree = "resilience-tree";

    [Test]
    public async Task A_transient_sampling_failure_does_not_tear_down_the_shared_loop()
    {
        var query = new FlakyStateQuery(failuresBeforeSuccess: 2);
        var sampler = CreateSampler(query, signal: null);
        var request = new TreeMetricsRequest
        {
            TreeIds = new[] { ResilienceTree },
            SampleInterval = TimeSpan.FromMilliseconds(20),
        };

        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        IReadOnlyDictionary<string, TreeMetrics>? first = null;
        await foreach (var map in sampler.SubscribeAsync(request, timeout.Token))
        {
            first = map;
            break;
        }

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.Not.Null,
                "The loop must ride out transient sampling faults and still deliver once the store recovers.");
            Assert.That(query.Attempts, Is.GreaterThan(2), "the failing ticks were retried, not fatal");
            Assert.That(first!.ContainsKey(ResilienceTree), Is.True);
        });
    }

    [Test]
    public async Task A_non_positive_sample_interval_falls_back_to_a_safe_default()
    {
        // A zero or negative interval would otherwise spin the shared loop with no
        // delay between samples, turning a client-supplied value into a hot loop
        // that fans out to the whole cluster.
        var query = new FlakyStateQuery(failuresBeforeSuccess: 0);
        var sampler = CreateSampler(query, signal: null);
        var request = new TreeMetricsRequest
        {
            TreeIds = new[] { ResilienceTree },
            SampleInterval = TimeSpan.Zero,
        };

        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var received = 0;
        await foreach (var _ in sampler.SubscribeAsync(request, timeout.Token))
        {
            received++;
            break;
        }

        Assert.That(received, Is.EqualTo(1));
    }

    [Test]
    public async Task Cancelling_a_subscription_detaches_it_and_stops_the_loop()
    {
        var query = new FlakyStateQuery(failuresBeforeSuccess: 0);
        var sampler = CreateSampler(query, signal: null);
        var request = new TreeMetricsRequest
        {
            TreeIds = new[] { ResilienceTree },
            SampleInterval = TimeSpan.FromMilliseconds(20),
        };

        using var cts = new CancellationTokenSource();
        var enumerator = sampler.SubscribeAsync(request, cts.Token).GetAsyncEnumerator(cts.Token);
        try
        {
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(sampler.ActiveSamplerCount, Is.EqualTo(1));
        }
        finally
        {
            await cts.CancelAsync();
            try
            {
                await enumerator.DisposeAsync();
            }
            catch (OperationCanceledException)
            {
                // Disposing a cancelled iterator surfaces the cancellation; expected.
            }
        }

        Assert.That(sampler.ActiveSamplerCount, Is.Zero,
            "The last subscriber leaving must retire the shared loop rather than leak a sampling timer.");
    }

    [Test]
    public async Task Subjects_differing_only_by_claim_are_not_coalesced_onto_one_loop()
    {
        var query = new FlakyStateQuery(failuresBeforeSuccess: 0);
        var sampler = CreateClaimSampler(query);
        var request = new TreeMetricsRequest
        {
            TreeIds = new[] { ResilienceTree },
            SampleInterval = TimeSpan.FromMilliseconds(40),
        };

        await using var tenantA = new ClaimSubscriptionProbe(sampler, request, token: "tenant-a");
        await tenantA.FirstAsync();
        await using var tenantB = new ClaimSubscriptionProbe(sampler, request, token: "tenant-b");
        await tenantB.FirstAsync();

        Assert.That(sampler.ActiveSamplerCount, Is.EqualTo(2),
            "Two subjects with the same id and groups but different claims must not share a sampling loop.");
    }

    [Test]
    public async Task Subjects_with_identical_claims_share_one_loop()
    {
        var query = new FlakyStateQuery(failuresBeforeSuccess: 0);
        var sampler = CreateClaimSampler(query);
        var request = new TreeMetricsRequest
        {
            TreeIds = new[] { ResilienceTree },
            SampleInterval = TimeSpan.FromMilliseconds(40),
        };

        await using var first = new ClaimSubscriptionProbe(sampler, request, token: "tenant-a");
        await first.FirstAsync();
        await using var second = new ClaimSubscriptionProbe(sampler, request, token: "tenant-a");
        await second.FirstAsync();

        Assert.That(sampler.ActiveSamplerCount, Is.EqualTo(1),
            "Identical identities must still coalesce, otherwise the sharing optimisation is lost.");
    }

    [Test]
    public async Task View_lag_is_rolled_up_per_source_tree()
    {
        var query = new FlakyStateQuery(failuresBeforeSuccess: 0)
        {
            Views =
            {
                new ViewStateSummary { ViewName = "v1", SourceTreeId = ResilienceTree, Lag = 10 },
                new ViewStateSummary { ViewName = "v2", SourceTreeId = ResilienceTree, Lag = 20 },
            },
        };
        var sampler = CreateSampler(query, signal: null);

        var result = await sampler.SampleOnceAsync(
            new TreeMetricsRequest { TreeIds = new[] { ResilienceTree }, IncludeViewLag = true },
            CancellationToken.None);

        var metrics = result[ResilienceTree];
        Assert.Multiple(() =>
        {
            Assert.That(metrics.ViewCount, Is.EqualTo(2), "both views over the source tree are counted");
            Assert.That(metrics.ViewLagTotal, Is.EqualTo(30), "lag is summed across the source tree's views");
        });
    }

    private static SharedMetricsSampler CreateClaimSampler(ILatticeStateQuery query)
    {
        var services = new ServiceCollection();
        services.AddSingleton<ILatticeAccessGate>(new AllowAllAccessGate());
        services.AddSingleton<ILatticeMembershipContext>(new ClaimSubjectMembershipContext());

        return new SharedMetricsSampler(
            query,
            Options.Create(new LatticeApiStateOptions()),
            services.BuildServiceProvider());
    }

    private sealed class ClaimSubscriptionProbe(
        SharedMetricsSampler sampler,
        TreeMetricsRequest request,
        string token) : IAsyncDisposable
    {
        private readonly IAsyncEnumerator<IReadOnlyDictionary<string, TreeMetrics>> _enumerator =
            sampler.SubscribeAsync(request).GetAsyncEnumerator();

        public async Task FirstAsync()
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            using (LatticeCredentialContext.Use(token))
            {
                var moved = await _enumerator.MoveNextAsync().AsTask().WaitAsync(timeout.Token);
                Assert.That(moved, Is.True, "the sampler produced a first map");
            }
        }

        public ValueTask DisposeAsync() => _enumerator.DisposeAsync();
    }

    /// <summary>
    /// Resolves every token to the same subject id and group set, differing only
    /// in the claim dictionary, so the signature's claim component is the only
    /// thing that can separate the two loops.
    /// </summary>
    private sealed class ClaimSubjectMembershipContext : ILatticeMembershipContext
    {
        public ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default)
            => new(Resolve());

        public bool TryResolveCurrent(out LatticeSubject subject)
        {
            subject = Resolve();
            return true;
        }

        private static LatticeSubject Resolve()
        {
            var token = LatticeCredentialContext.Current?.Token ?? "anonymous";
            return new LatticeSubject(
                "shared-subject",
                new[] { "shared-group" },
                new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["tenant"] = token,
                    ["scope"] = "metrics",
                });
        }
    }

    /// <summary>
    /// Fails the first <c>failuresBeforeSuccess</c> shard walks, then succeeds -
    /// the shape of a durable store that is briefly unreachable.
    /// </summary>
    private sealed class FlakyStateQuery(int failuresBeforeSuccess) : ILatticeStateQuery
    {
        private int _attempts;

        public int Attempts => Volatile.Read(ref _attempts);

        public List<ViewStateSummary> Views { get; } = [];

        public Task<ShardSummariesResult> GetShardSummariesAsync(
            string treeId,
            bool deep = true,
            CancellationToken cancellationToken = default)
        {
            if (Interlocked.Increment(ref _attempts) <= failuresBeforeSuccess)
            {
                throw new InvalidOperationException("shard walk unavailable");
            }

            return Task.FromResult(ShardSummariesResult.Found(treeId, new[]
            {
                new ShardStateSummary
                {
                    ShardIndex = 0,
                    Depth = 1,
                    RootIsLeaf = true,
                    LiveKeys = 1,
                    Tombstones = 0,
                    OpsPerSecond = 0,
                    SplitInProgress = false,
                },
            }));
        }

        public Task<TreeCatalogPage> ListTreesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new TreeCatalogPage
            {
                Entries = new List<TreeCatalogEntry>
                {
                    new() { TreeId = ResilienceTree, Config = new TreeConfigSummary() },
                },
            });

        public Task<ViewCatalogPage> ListViewsAsync(CatalogRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new ViewCatalogPage { Entries = Views });

        public Task<int?> GetPhysicalShardCountAsync(string treeId, CancellationToken cancellationToken = default)
            => Task.FromResult((int?)null);

        public Task<TreeSummaryResult> GetTreeSummaryAsync(string treeId, bool deep = true, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<ClusterInfo> GetClusterInfoAsync(CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<TagIndexCatalogPage> ListTagIndexesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<TagValueCatalogPage> ListTagValuesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<CoveredTreeCatalogPage> ListCoveredTreesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<TagValueCatalogPage> ListIndexTagsAsync(CatalogRequest request, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task CancelScanAsync(string treeId, string? cursor = null, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<TagMemberScanPage> ScanTagMembersAsync(TagMemberScanRequest request, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<TreeStructureResult> GetTreeStructureAsync(StructureRequest request, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<EntryScanResult> ScanEntriesAsync(EntryScanRequest request, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<EntryDetailResult> GetEntryAsync(string treeId, string key, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<EntryHistoryResult> GetEntryHistoryAsync(EntryHistoryRequest request, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<int> GetDeadLetterCountAsync(string treeId, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<DeadLetterQueuePage> ListDeadLettersAsync(DeadLetterQueueRequest request, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();
    }
}
