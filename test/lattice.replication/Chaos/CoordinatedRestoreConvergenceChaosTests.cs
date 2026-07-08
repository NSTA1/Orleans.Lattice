using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests.Chaos;

/// <summary>
/// Chaos coverage for coordinated cross-cluster restore convergence under duress.
/// Drives the real coordinator / participant grains over the in-process saga
/// harness through many randomized iterations, asserting the all-or-nothing
/// guarantee (#1169) holds every time: either every cluster commits the restore
/// cut, or every prepared cluster is compensated back to its pre-restore state -
/// never a partial cut-over.
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class CoordinatedRestoreConvergenceChaosTests
{
    private const string TargetTree = "orders";
    private const string ManifestId = "backup-1";
    private const string CoordinatorCluster = "site-home";
    private const int Iterations = 40;

    [Test]
    public async Task Randomized_vote_outcomes_always_converge_all_or_nothing()
    {
        var rng = new Random(20260708);

        for (var iteration = 0; iteration < Iterations; iteration++)
        {
            var sagaId = $"restore-conv-{iteration}";
            var clusterCount = 2 + rng.Next(3); // 2..4 clusters
            var failCount = rng.Next(clusterCount + 1); // 0..clusterCount build failures

            // Randomly choose which clusters fail their shadow build this round.
            var failing = new HashSet<int>();
            while (failing.Count < failCount)
            {
                failing.Add(rng.Next(clusterCount));
            }

            var clusters = new CoordinatedRestoreSagaHarness.Cluster[clusterCount];
            var channel = new InProcessSagaControlChannel();
            var clusterIds = new List<string>(clusterCount);
            for (var c = 0; c < clusterCount; c++)
            {
                var idx = c;
                var clusterId = $"site-{iteration}-{c}";
                clusterIds.Add(clusterId);
                clusters[c] = CoordinatedRestoreSagaHarness.CreateCluster(
                    clusterId, sagaId, TargetTree,
                    configureEngine: failing.Contains(idx)
                        ? e => e.BuildFailure = new InvalidOperationException("chaos: build failure")
                        : null);
                channel.Register(clusterId, clusters[c].Grain);
            }

            var coordinator = CoordinatedRestoreSagaHarness.CreateCoordinator(sagaId, channel);
            var outcome = await coordinator.RunAsync(clusterIds, TargetTree, ManifestId, CoordinatorCluster);

            if (failCount == 0)
            {
                Assert.That(outcome, Is.EqualTo(CrossClusterSagaOutcome.Committed),
                    $"iteration {iteration}: a healthy saga must commit");
                foreach (var cluster in clusters)
                {
                    Assert.That(cluster.Engine.CommitCount, Is.EqualTo(1),
                        $"iteration {iteration}: every cluster commits exactly once");
                    Assert.That(cluster.Engine.RevertCount, Is.EqualTo(0),
                        $"iteration {iteration}: no cluster reverts on a healthy commit");
                }
            }
            else
            {
                Assert.That(outcome, Is.EqualTo(CrossClusterSagaOutcome.Aborted),
                    $"iteration {iteration}: any build failure aborts the whole saga");

                // All-or-nothing: no cluster is left partially cut over.
                foreach (var cluster in clusters)
                {
                    Assert.That(cluster.Engine.CommitCount, Is.EqualTo(0),
                        $"iteration {iteration}: no cluster commits when the saga aborts");
                }
            }
        }
    }

    [Test]
    public async Task Coordinator_loss_mid_saga_auto_compensates_every_prepared_cluster()
    {
        var rng = new Random(981);

        for (var iteration = 0; iteration < Iterations; iteration++)
        {
            var sagaId = $"restore-coordloss-{iteration}";
            var clusterCount = 2 + rng.Next(3); // 2..4
            var clusters = new CoordinatedRestoreSagaHarness.Cluster[clusterCount];
            var request = CoordinatedRestoreSagaHarness.Request(sagaId, TargetTree, ManifestId, CoordinatorCluster);

            for (var c = 0; c < clusterCount; c++)
            {
                clusters[c] = CoordinatedRestoreSagaHarness.CreateCluster(
                    $"site-{iteration}-{c}", sagaId, TargetTree);
            }

            // Every cluster prepares (builds its shadow unfenced) and votes commit.
            foreach (var cluster in clusters)
            {
                var vote = await cluster.Grain.PrepareAsync(request);
                Assert.That(vote.Vote, Is.EqualTo(SagaVote.Commit),
                    $"iteration {iteration}: a healthy cluster prepares");
            }

            // Coordinator is lost before it delivers a decision. The durable abort
            // path (what the cutover-fence expiry fires) compensates every cluster.
            foreach (var cluster in clusters)
            {
                await cluster.Grain.AbortAsync(request);
            }

            foreach (var cluster in clusters)
            {
                Assert.That(cluster.Engine.CommitCount, Is.EqualTo(0),
                    $"iteration {iteration}: a lost coordinator never commits");
                Assert.That(cluster.Engine.RevertCount, Is.EqualTo(1),
                    $"iteration {iteration}: every prepared cluster is reverted");
                Assert.That(cluster.Engine.DeleteCount, Is.GreaterThanOrEqualTo(1),
                    $"iteration {iteration}: every prepared cluster garbage collects its shadow");
            }
        }
    }

    [Test]
    public async Task Peer_dropping_between_prepare_and_commit_converges_to_a_full_commit()
    {
        // A peer that voted commit becomes momentarily unreachable at the coordinator's
        // commit delivery, then returns. The saga must converge to a full commit on
        // every cluster (idempotent finalize), never a stranded partial cut-over.
        for (var iteration = 0; iteration < Iterations; iteration++)
        {
            var sagaId = $"restore-peerdrop-{iteration}";
            var a = CoordinatedRestoreSagaHarness.CreateCluster($"site-a-{iteration}", sagaId, TargetTree);
            var b = CoordinatedRestoreSagaHarness.CreateCluster($"site-b-{iteration}", sagaId, TargetTree);

            var inner = new InProcessSagaControlChannel();
            inner.Register("site-a", a.Grain);
            inner.Register("site-b", b.Grain);

            // site-b's first commit delivery drops (unreachable), then heals.
            var flaky = new CommitFlakyControlChannel(inner, "site-b");
            var coordinator = CoordinatedRestoreSagaHarness.CreateCoordinator(sagaId, flaky);

            // First run: prepare succeeds and the decision is Committed, but delivering
            // commit to the dropped peer faults, so RunAsync surfaces the transport error.
            Assert.That(async () =>
                await coordinator.RunAsync(["site-a", "site-b"], TargetTree, ManifestId, CoordinatorCluster),
                Throws.InvalidOperationException,
                $"iteration {iteration}: the dropped commit delivery surfaces");

            // Resume (as the keepalive reminder would): the peer has healed, so finalize
            // retries and the saga completes with a commit on every cluster.
            var outcome = await coordinator.RunAsync(
                ["site-a", "site-b"], TargetTree, ManifestId, CoordinatorCluster);

            Assert.That(outcome, Is.EqualTo(CrossClusterSagaOutcome.Committed),
                $"iteration {iteration}: the saga converges to a full commit after the peer heals");
            Assert.Multiple(() =>
            {
                Assert.That(a.Engine.CommitCount, Is.EqualTo(1),
                    $"iteration {iteration}: site-a committed exactly once (idempotent)");
                Assert.That(b.Engine.CommitCount, Is.EqualTo(1),
                    $"iteration {iteration}: site-b committed exactly once after healing");
                Assert.That(a.Engine.RevertCount, Is.EqualTo(0),
                    $"iteration {iteration}: no cluster reverts on a converged commit");
                Assert.That(b.Engine.RevertCount, Is.EqualTo(0),
                    $"iteration {iteration}: no cluster reverts on a converged commit");
            });
        }
    }

    /// <summary>
    /// Wraps an <see cref="ISagaControlChannel"/> and drops the first commit delivery
    /// to a named cluster (throwing a transport-style error), then passes every later
    /// call through. Models a peer that drops between prepare and commit and then heals.
    /// </summary>
    private sealed class CommitFlakyControlChannel(ISagaControlChannel inner, string flakyCluster) : ISagaControlChannel
    {
        private int _commitAttempts;

        public Task<SagaControlResponse> PrepareAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default) =>
            inner.PrepareAsync(clusterId, request, cancellationToken);

        public Task<SagaControlResponse> CommitAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default)
        {
            if (string.Equals(clusterId, flakyCluster, StringComparison.Ordinal)
                && Interlocked.Increment(ref _commitAttempts) == 1)
            {
                throw new InvalidOperationException($"chaos: peer '{clusterId}' unreachable at commit delivery");
            }

            return inner.CommitAsync(clusterId, request, cancellationToken);
        }

        public Task<SagaControlResponse> AbortAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default) =>
            inner.AbortAsync(clusterId, request, cancellationToken);

        public Task<SagaControlResponse> GetStatusAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default) =>
            inner.GetStatusAsync(clusterId, request, cancellationToken);
    }
}
