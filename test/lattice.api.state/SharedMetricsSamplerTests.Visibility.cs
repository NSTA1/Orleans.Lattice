using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Regression coverage for the metrics-feed cross-identity isolation contract
/// (issue #971): the shared sampling loop captures the first subscriber's
/// ambient credential and samples a per-tree map filtered to that identity, then
/// fans the same map to every subscriber on the signature. The signature is
/// therefore keyed by the resolved caller subject so a lower-privilege
/// subscriber can never be coalesced onto a higher-privilege subscriber's loop
/// and receive metrics for a tree it cannot read. When visibility is disabled
/// (no access gate) the signature stays identity-free and coalescing is
/// unchanged at zero cost.
/// </summary>
public partial class SharedMetricsSamplerTests
{
    private const string AdminTree = "admin-tree";
    private const string SharedTree = "shared-tree";

    [Test]
    public async Task Subscribe_does_not_leak_higher_privilege_trees_to_a_lower_privilege_subscriber()
    {
        var query = new IdentityScopedStateQuery();
        var sampler = CreateVisibilitySampler(query, new TokenSubjectMembershipContext());
        var request = MetricsRequest();

        // The admin subscriber attaches first, so the shared loop would capture
        // its credential and sample the admin-visible map.
        await using var admin = new SubscriptionProbe(sampler, request, token: "admin");
        var adminMap = await admin.FirstAsync();

        // A lower-privilege subscriber with the identical request shape must NOT
        // be coalesced onto the admin loop; before the fix it shared the loop and
        // received the admin map, leaking a tree it cannot read.
        await using var user = new SubscriptionProbe(sampler, request, token: "user");
        var userMap = await user.FirstAsync();

        Assert.Multiple(() =>
        {
            Assert.That(adminMap.Keys, Is.EquivalentTo(new[] { AdminTree, SharedTree }));
            Assert.That(
                userMap.ContainsKey(AdminTree),
                Is.False,
                "a lower-privilege subscriber must never receive metrics for a tree only the admin can read");
            Assert.That(userMap.Keys, Is.EquivalentTo(new[] { SharedTree }));
            Assert.That(
                sampler.ActiveSamplerCount,
                Is.EqualTo(2),
                "subscribers with different resolved identities must run on separate sampling loops");
        });
    }

    [Test]
    public async Task Subscribe_coalesces_subscribers_that_resolve_to_the_same_identity()
    {
        var query = new IdentityScopedStateQuery();
        var sampler = CreateVisibilitySampler(query, new TokenSubjectMembershipContext());
        var request = MetricsRequest();

        await using var first = new SubscriptionProbe(sampler, request, token: "admin");
        await first.FirstAsync();

        await using var second = new SubscriptionProbe(sampler, request, token: "admin");
        var secondMap = await second.FirstAsync();

        Assert.Multiple(() =>
        {
            Assert.That(
                sampler.ActiveSamplerCount,
                Is.EqualTo(1),
                "the same identity and request shape must still coalesce onto one loop");
            Assert.That(secondMap.Keys, Is.EquivalentTo(new[] { AdminTree, SharedTree }));
        });
    }

    [Test]
    public async Task Subscribe_without_visibility_coalesces_regardless_of_credential()
    {
        // No access gate registered => visibility disabled => identity-free
        // signature => coalescing is byte-for-byte the pre-authorization
        // behaviour, proving the feature is zero-cost when off.
        var query = new IdentityScopedStateQuery();
        var sampler = CreateSampler(query, signal: null);
        var request = MetricsRequest();

        await using var admin = new SubscriptionProbe(sampler, request, token: "admin");
        await admin.FirstAsync();

        await using var user = new SubscriptionProbe(sampler, request, token: "user");
        await user.FirstAsync();

        Assert.That(
            sampler.ActiveSamplerCount,
            Is.EqualTo(1),
            "with visibility disabled the signature is identity-free, so different credentials still coalesce");
    }

    private static SharedMetricsSampler CreateVisibilitySampler(
        ILatticeStateQuery query,
        ILatticeMembershipContext membership)
    {
        var services = new ServiceCollection();
        services.AddSingleton<ILatticeAccessGate>(new AllowAllAccessGate());
        services.AddSingleton(membership);

        return new SharedMetricsSampler(
            query,
            Options.Create(new LatticeApiStateOptions()),
            services.BuildServiceProvider());
    }

    private static TreeMetricsRequest MetricsRequest() => new()
    {
        IncludeShardHotness = false,
        IncludeViewLag = false,
        IncludeSystemTrees = false,
        SampleInterval = TimeSpan.FromMilliseconds(40),
    };

    /// <summary>
    /// Drives a single <see cref="SharedMetricsSampler.SubscribeAsync"/>
    /// subscription under a fixed ambient credential and exposes the first map it
    /// receives while keeping the subscription attached, so multiple identities
    /// can be held live concurrently to assert loop isolation.
    /// </summary>
    private sealed class SubscriptionProbe(SharedMetricsSampler sampler, TreeMetricsRequest request, string token)
        : IAsyncDisposable
    {
        private readonly string _token = token;
        private readonly IAsyncEnumerator<IReadOnlyDictionary<string, TreeMetrics>> _enumerator =
            sampler.SubscribeAsync(request).GetAsyncEnumerator();

        public async Task<IReadOnlyDictionary<string, TreeMetrics>> FirstAsync()
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));

            // The credential must be ambient for the first MoveNextAsync: the
            // sampler resolves the subject and captures the credential for the
            // loop synchronously before the first real await, so this is the
            // window that pins the subscription to an identity-keyed signature.
            using (LatticeCredentialContext.Use(_token))
            {
                var moved = await _enumerator.MoveNextAsync().AsTask().WaitAsync(timeout.Token);
                Assert.That(moved, Is.True, "the sampler produced a first map");
                return _enumerator.Current;
            }
        }

        public ValueTask DisposeAsync() => _enumerator.DisposeAsync();
    }

    /// <summary>
    /// Resolves the ambient <see cref="LatticeCredentialContext"/> token into a
    /// distinct subject synchronously, so each subscriber's signature carries its
    /// own identity without a directory read.
    /// </summary>
    private sealed class TokenSubjectMembershipContext : ILatticeMembershipContext
    {
        public ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default)
            => new(Resolve());

        public bool TryResolveCurrent(out LatticeSubject subject)
        {
            subject = Resolve();
            return true;
        }

        private static LatticeSubject Resolve()
            => LatticeCredentialContext.Current?.Token switch
            {
                "admin" => new LatticeSubject("admin", new[] { "admins" }),
                "user" => new LatticeSubject("user", new[] { "users" }),
                _ => LatticeSubject.Anonymous,
            };
    }

    /// <summary>
    /// A state query whose visible tree set depends on the ambient credential the
    /// shared loop samples under: the admin sees a private tree plus a shared
    /// tree, the user sees only the shared tree. This reproduces the identity
    /// filtering the real <c>LatticeStateQuery</c> applies inside the loop.
    /// </summary>
    private sealed class IdentityScopedStateQuery : ILatticeStateQuery
    {
        public Task<TreeCatalogPage> ListTreesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
        {
            var trees = LatticeCredentialContext.Current?.Token switch
            {
                "admin" => new[] { AdminTree, SharedTree },
                "user" => new[] { SharedTree },
                _ => Array.Empty<string>(),
            };

            var entries = trees
                .Select(id => new TreeCatalogEntry { TreeId = id, Config = new TreeConfigSummary() })
                .ToList();

            return Task.FromResult(new TreeCatalogPage { Entries = entries });
        }

        public Task<ShardSummariesResult> GetShardSummariesAsync(string treeId, bool deep = true, CancellationToken cancellationToken = default)
            => Task.FromResult(ShardSummariesResult.Found(treeId, new[] { OneShard(treeId) }));

        private static ShardStateSummary OneShard(string treeId) => new()
        {
            ShardIndex = 0,
            Depth = 1,
            RootIsLeaf = true,
            LiveKeys = treeId.Length,
            Tombstones = 0,
            OpsPerSecond = 0,
            SplitInProgress = false,
        };

        public Task<TreeSummaryResult> GetTreeSummaryAsync(string treeId, bool deep = true, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<int?> GetPhysicalShardCountAsync(string treeId, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<ViewCatalogPage> ListViewsAsync(CatalogRequest request, CancellationToken cancellationToken = default)
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

        public Task CancelScanAsync(string treeId, string? continuationToken, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();
    }

    /// <summary>
    /// A minimal non-null access gate: its mere presence makes the visibility
    /// filter <c>Enabled</c> so the sampler resolves and keys by subject. The
    /// gate itself is never consulted on the signature path (subject resolution
    /// reads only the membership context), so an allow-all decision is safe.
    /// </summary>
    private sealed class AllowAllAccessGate : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default)
            => new(LatticeAccessDecision.Allow());
    }
}
