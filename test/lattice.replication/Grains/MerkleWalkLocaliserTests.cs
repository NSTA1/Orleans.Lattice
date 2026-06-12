using System.Linq;
using NUnit.Framework;
using Orleans.Lattice;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;
using Orleans.Runtime;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>Tests for the read-only Merkle-walk drift-localisation engine.</summary>
[TestFixture]
public sealed class MerkleWalkLocaliserTests
{
    private const string Tree = "orders";
    private const string Peer = "cluster-b";

    private static LeafProjectionDigest Digest(byte[] hash, int version = 0) => new()
    {
        Hash = hash,
        EntryCount = hash.Length,
        CheckpointOffset = 0,
        Version = version,
    };

    private static GrainId NodeId(string key) => GrainId.Create("n", key);

    [Test]
    public async Task WalkAsync_empty_shard_localises_nothing()
    {
        var tree = new FakeTree(root: null);
        var transport = new StubTransport((_, _) => MerkleWalkProbeResponse.Unavailable);

        var outcome = await MerkleWalkLocaliser.WalkAsync(
            Tree, 0, Peer, tree, transport, maxDepth: 8, maxBytes: 1024, CancellationToken.None);

        Assert.That(outcome.Localised, Is.False);
        Assert.That(outcome.AbortReason, Is.EqualTo(MerkleWalkAbortReason.None));
        Assert.That(transport.Calls, Is.Zero);
    }

    [Test]
    public async Task WalkAsync_flat_leaf_root_localises_at_depth_zero()
    {
        var root = new MerkleWalkLocalNode
        {
            IsLeaf = true,
            Digest = Digest(new byte[] { 1, 1, 1 }),
            Children = Array.Empty<MerkleWalkLocalChild>(),
        };
        var tree = new FakeTree(root);
        var transport = new StubTransport((_, _) =>
            new MerkleWalkProbeResponse { Available = true, Digest = Digest(new byte[] { 2, 2, 2 }) });

        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.MerkleWalkLocalisedName);

        var outcome = await MerkleWalkLocaliser.WalkAsync(
            Tree, 0, Peer, tree, transport, maxDepth: 8, maxBytes: 1024, CancellationToken.None);

        Assert.That(outcome.Localised, Is.True);
        Assert.That(outcome.LeavesLocalised, Is.EqualTo(1));
        Assert.That(outcome.DepthReached, Is.Zero);
        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        Assert.That(collector.Measurements.Single().Value, Is.EqualTo(1L));
    }

    [Test]
    public async Task WalkAsync_matching_root_localises_nothing()
    {
        var hash = new byte[] { 7, 7, 7 };
        var root = new MerkleWalkLocalNode
        {
            IsLeaf = true,
            Digest = Digest(hash),
            Children = Array.Empty<MerkleWalkLocalChild>(),
        };
        var tree = new FakeTree(root);
        var transport = new StubTransport((_, _) =>
            new MerkleWalkProbeResponse { Available = true, Digest = Digest(new byte[] { 7, 7, 7 }) });

        var outcome = await MerkleWalkLocaliser.WalkAsync(
            Tree, 0, Peer, tree, transport, maxDepth: 8, maxBytes: 1024, CancellationToken.None);

        Assert.That(outcome.Localised, Is.False);
        Assert.That(outcome.AbortReason, Is.EqualTo(MerkleWalkAbortReason.None));
    }

    [Test]
    public async Task WalkAsync_descends_to_diverging_leaf_and_prunes_matching_sibling()
    {
        // Root internal diverges; left leaf matches (pruned), right leaf diverges (localised).
        var leftId = NodeId("left");
        var rightId = NodeId("right");
        var root = new MerkleWalkLocalNode
        {
            IsLeaf = false,
            Digest = Digest(new byte[] { 0, 0 }),
            Children = new[]
            {
                new MerkleWalkLocalChild { SeparatorKey = null, NodeId = leftId, ChildIsLeaf = true },
                new MerkleWalkLocalChild { SeparatorKey = "m", NodeId = rightId, ChildIsLeaf = true },
            },
        };

        var tree = new FakeTree(root);
        tree.Add(leftId, new MerkleWalkLocalNode
        {
            IsLeaf = true,
            Digest = Digest(new byte[] { 1, 1 }),
            Children = Array.Empty<MerkleWalkLocalChild>(),
        });
        tree.Add(rightId, new MerkleWalkLocalNode
        {
            IsLeaf = true,
            Digest = Digest(new byte[] { 2, 2 }),
            Children = Array.Empty<MerkleWalkLocalChild>(),
        });

        // Remote: root differs; left range [null,m) matches local left; right range [m,null) differs.
        var transport = new StubTransport((_, req) =>
        {
            byte[] hash = (req.Depth, req.RangeStartKey) switch
            {
                (0, _) => new byte[] { 9, 9 },        // root differs from {0,0}
                (1, null) => new byte[] { 1, 1 },     // left matches
                (1, "m") => new byte[] { 8, 8 },      // right differs from {2,2}
                _ => new byte[] { 0 },
            };
            return new MerkleWalkProbeResponse { Available = true, Digest = Digest(hash) };
        });

        using var localised = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.MerkleWalkLocalisedName);

        var outcome = await MerkleWalkLocaliser.WalkAsync(
            Tree, 0, Peer, tree, transport, maxDepth: 8, maxBytes: 1024, CancellationToken.None);

        Assert.That(outcome.Localised, Is.True);
        Assert.That(outcome.LeavesLocalised, Is.EqualTo(1));
        Assert.That(outcome.DepthReached, Is.EqualTo(1));
        Assert.That(localised.Measurements, Has.Count.EqualTo(1));
        Assert.That(localised.Measurements.Single().Value, Is.EqualTo(1L));
    }

    [Test]
    public async Task WalkAsync_aborts_on_depth_cap()
    {
        var childId = NodeId("child");
        var root = new MerkleWalkLocalNode
        {
            IsLeaf = false,
            Digest = Digest(new byte[] { 0 }),
            Children = new[]
            {
                new MerkleWalkLocalChild { SeparatorKey = null, NodeId = childId, ChildIsLeaf = false },
            },
        };
        var tree = new FakeTree(root);
        tree.Add(childId, new MerkleWalkLocalNode
        {
            IsLeaf = false,
            Digest = Digest(new byte[] { 1 }),
            Children = Array.Empty<MerkleWalkLocalChild>(),
        });

        var transport = new StubTransport((_, _) =>
            new MerkleWalkProbeResponse { Available = true, Digest = Digest(new byte[] { 9 }) });

        using var aborted = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.MerkleWalkAbortedName);

        var outcome = await MerkleWalkLocaliser.WalkAsync(
            Tree, 0, Peer, tree, transport, maxDepth: 1, maxBytes: 1024, CancellationToken.None);

        Assert.That(outcome.Localised, Is.False);
        Assert.That(outcome.AbortReason, Is.EqualTo(MerkleWalkAbortReason.DepthCapExceeded));
        Assert.That(aborted.Measurements, Has.Count.EqualTo(1));
        Assert.That(aborted.Measurements.Single().Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == LatticeReplicationMetrics.TagReason
            && (string?)t.Value == LatticeReplicationMetrics.MerkleWalkAbortDepthCap));
    }

    [Test]
    public async Task WalkAsync_aborts_on_byte_budget()
    {
        var root = new MerkleWalkLocalNode
        {
            IsLeaf = false,
            Digest = Digest(new byte[] { 0, 0, 0, 0 }),
            Children = Array.Empty<MerkleWalkLocalChild>(),
        };
        var tree = new FakeTree(root);
        var transport = new StubTransport((_, _) =>
            new MerkleWalkProbeResponse { Available = true, Digest = Digest(new byte[] { 9, 9, 9, 9 }) });

        using var aborted = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.MerkleWalkAbortedName);

        var outcome = await MerkleWalkLocaliser.WalkAsync(
            Tree, 0, Peer, tree, transport, maxDepth: 8, maxBytes: 1, CancellationToken.None);

        Assert.That(outcome.AbortReason, Is.EqualTo(MerkleWalkAbortReason.ByteBudgetExceeded));
        Assert.That(aborted.Measurements.Single().Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == LatticeReplicationMetrics.TagReason
            && (string?)t.Value == LatticeReplicationMetrics.MerkleWalkAbortByteBudget));
    }

    [Test]
    public async Task WalkAsync_aborts_on_remote_unavailable()
    {
        var root = new MerkleWalkLocalNode
        {
            IsLeaf = true,
            Digest = Digest(new byte[] { 1 }),
            Children = Array.Empty<MerkleWalkLocalChild>(),
        };
        var tree = new FakeTree(root);
        var transport = new StubTransport((_, _) => MerkleWalkProbeResponse.Unavailable);

        using var aborted = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.MerkleWalkAbortedName);

        var outcome = await MerkleWalkLocaliser.WalkAsync(
            Tree, 0, Peer, tree, transport, maxDepth: 8, maxBytes: 1024, CancellationToken.None);

        Assert.That(outcome.AbortReason, Is.EqualTo(MerkleWalkAbortReason.RemoteUnavailable));
        Assert.That(aborted.Measurements.Single().Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == LatticeReplicationMetrics.TagReason
            && (string?)t.Value == LatticeReplicationMetrics.MerkleWalkAbortRemoteUnavailable));
    }

    [Test]
    public async Task WalkAsync_aborts_on_version_skew()
    {
        var root = new MerkleWalkLocalNode
        {
            IsLeaf = true,
            Digest = Digest(new byte[] { 1 }, version: 0),
            Children = Array.Empty<MerkleWalkLocalChild>(),
        };
        var tree = new FakeTree(root);
        var transport = new StubTransport((_, _) =>
            new MerkleWalkProbeResponse { Available = true, Digest = Digest(new byte[] { 2 }, version: 1) });

        using var aborted = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.MerkleWalkAbortedName);

        var outcome = await MerkleWalkLocaliser.WalkAsync(
            Tree, 0, Peer, tree, transport, maxDepth: 8, maxBytes: 1024, CancellationToken.None);

        Assert.That(outcome.AbortReason, Is.EqualTo(MerkleWalkAbortReason.VersionSkew));
        Assert.That(aborted.Measurements.Single().Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == LatticeReplicationMetrics.TagReason
            && (string?)t.Value == LatticeReplicationMetrics.MerkleWalkAbortVersionSkew));
    }

    [Test]
    public void WalkAsync_throws_on_null_required_args()
    {
        var tree = new FakeTree(root: null);
        var transport = new StubTransport((_, _) => MerkleWalkProbeResponse.Unavailable);

        Assert.That(async () => await MerkleWalkLocaliser.WalkAsync(
            null!, 0, Peer, tree, transport, 8, 1024, CancellationToken.None),
            Throws.InstanceOf<ArgumentNullException>());
        Assert.That(async () => await MerkleWalkLocaliser.WalkAsync(
            Tree, 0, null!, tree, transport, 8, 1024, CancellationToken.None),
            Throws.InstanceOf<ArgumentNullException>());
        Assert.That(async () => await MerkleWalkLocaliser.WalkAsync(
            Tree, 0, Peer, null!, transport, 8, 1024, CancellationToken.None),
            Throws.InstanceOf<ArgumentNullException>());
        Assert.That(async () => await MerkleWalkLocaliser.WalkAsync(
            Tree, 0, Peer, tree, null!, 8, 1024, CancellationToken.None),
            Throws.InstanceOf<ArgumentNullException>());
    }

    private sealed class FakeTree(MerkleWalkLocalNode? root) : IMerkleWalkLocalTree
    {
        private readonly Dictionary<GrainId, MerkleWalkLocalNode> _nodes = new();

        public void Add(GrainId id, MerkleWalkLocalNode node) => _nodes[id] = node;

        public ValueTask<MerkleWalkLocalNode?> GetRootAsync(CancellationToken cancellationToken) =>
            new(root);

        public ValueTask<MerkleWalkLocalNode> ResolveAsync(GrainId nodeId, bool isLeaf, CancellationToken cancellationToken) =>
            new(_nodes[nodeId]);
    }

    private sealed class StubTransport(Func<string, MerkleWalkProbeRequest, MerkleWalkProbeResponse> responder)
        : IReplicationDigestProbeTransport
    {
        public int Calls { get; private set; }

        public Task<DigestProbeResponse> ProbeDigestAsync(
            string targetClusterId, DigestProbeRequest request, CancellationToken cancellationToken) =>
            Task.FromResult(new DigestProbeResponse { DigestAvailable = false });

        public Task<MerkleWalkProbeResponse> ProbeMerkleWalkAsync(
            string targetClusterId, MerkleWalkProbeRequest request, CancellationToken cancellationToken)
        {
            Calls++;
            return Task.FromResult(responder(targetClusterId, request));
        }
    }
}
