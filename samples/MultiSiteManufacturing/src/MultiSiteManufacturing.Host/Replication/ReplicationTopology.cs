using Microsoft.Extensions.Configuration;

namespace MultiSiteManufacturing.Host.Replication;

/// <summary>
/// Peer cluster endpoint — identifies another cluster by name and
/// exposes one or more inbound replication URLs. The
/// <see cref="ReplicationHttpClient"/> tries <see cref="BaseUrls"/>
/// in order and falls back to the next on transport failure or a
/// non-success status, so a single peer-silo restart never stalls
/// shipping.
/// </summary>
/// <param name="Name">
/// Short cluster name (matches the peer cluster's
/// <see cref="ReplicationTopology.LocalCluster"/>). Used in batch
/// envelopes for diagnostics.
/// </param>
/// <param name="BaseUrls">
/// Ordered list of base URLs the peer cluster answers
/// <c>POST /replicate/{tree}</c> on. Any silo of the peer accepts
/// and applies a batch (Orleans routes the <c>ILattice</c> call to
/// the hosting activation via the cluster directory), so the list
/// typically contains every silo HTTP endpoint of the peer cluster.
/// </param>
public sealed record ReplicationPeer(string Name, IReadOnlyList<Uri> BaseUrls);

/// <summary>
/// Immutable snapshot of this cluster's replication configuration —
/// loaded once from <c>appsettings.cluster.{name}.json</c> at startup
/// and injected as a DI singleton.
/// </summary>
/// <remarks>
/// <para>
/// The <see cref="ReplicatedTrees"/> list is an <b>explicit opt-in</b>.
/// Writes on trees not in the list flow through the outgoing call
/// filter without any replog side effect, so they stay entirely
/// cluster-local.
/// </para>
/// </remarks>
public sealed class ReplicationTopology
{
    /// <summary>
    /// Short cluster name — matches the <c>--cluster</c> argument.
    /// Used as the <see cref="ReplicationBatch.SourceCluster"/> value
    /// and as the cluster-id tiebreaker in
    /// <see cref="ReplogKeyCodec"/>.
    /// </summary>
    public required string LocalCluster { get; init; }

    /// <summary>Cluster's peers — clusters to ship writes to.</summary>
    public IReadOnlyList<ReplicationPeer> Peers { get; init; } = [];

    /// <summary>Trees opted in to replication.</summary>
    public IReadOnlyList<string> ReplicatedTrees { get; init; } = [];

    /// <summary>
    /// Shared-secret token required in the <c>X-Replication-Token</c>
    /// header on inbound requests. Shipped as a sample-only
    /// placeholder in config; real deployments rotate this or
    /// replace it with mTLS.
    /// </summary>
    public required string SharedSecret { get; init; }

    /// <summary>
    /// Quick predicate — does this cluster replicate any trees at all?
    /// When <c>false</c> the filter short-circuits on every call.
    /// </summary>
    public bool IsEnabled => Peers.Count > 0 && ReplicatedTrees.Count > 0;

    /// <summary>Is <paramref name="tree"/> opted in to replication?</summary>
    public bool IsReplicated(string tree)
    {
        ArgumentNullException.ThrowIfNull(tree);
        for (var i = 0; i < ReplicatedTrees.Count; i++)
        {
            if (string.Equals(ReplicatedTrees[i], tree, StringComparison.Ordinal))
            {
                return true;
            }
        }
        return false;
    }

    /// <summary>
    /// Gate on <b>both</b> the tree and the individual key. Returns
    /// <c>true</c> only when <paramref name="tree"/> is opted in
    /// (<see cref="IsReplicated(string)"/>) <i>and</i> the key passes
    /// any tree-specific per-key filter.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The only tree with a per-key filter is <c>mfg-part-crdt</c>:
    /// the G-Set "labels" sub-tree (<c>{serial}/labels/{label}</c>) is
    /// safe to replicate unconditionally because union is commutative,
    /// associative, and idempotent — two clusters that both add the
    /// same label converge trivially. The LWW-register sub-tree
    /// (<c>{serial}/operator</c>) is <i>not</i> replicated: on inbound
    /// replay the receiver's lattice stamps its own HLC, which loses
    /// the source HLC ordering and can make the two clusters resolve
    /// the register to different winners under concurrent writes. The
    /// filter keeps the register strictly cluster-local until the
    /// library ships native source-HLC-preserving apply (see plan
    /// §13.7 + the FUTURE seam in <see cref="LatticeReplicationFilter"/>).
    /// </para>
    /// <para>
    /// Every other replicated tree (<c>mfg-facts</c>,
    /// <c>mfg-site-activity-index</c>) uses write-once immutable keys,
    /// so the per-key predicate is the identity function for them.
    /// </para>
    /// </remarks>
    public bool IsKeyReplicated(string tree, string key)
    {
        ArgumentNullException.ThrowIfNull(tree);
        ArgumentNullException.ThrowIfNull(key);

        if (!IsReplicated(tree))
        {
            return false;
        }

        // mfg-part-crdt: only the G-Set labels half replicates. The
        // operator LWW register is filtered at origin. The constants
        // are duplicated rather than imported to avoid a dependency
        // from the Replication layer on the Lattice layer — this
        // layer is already the cross-cutting seam, and the cost of a
        // second copy of the string "/labels/" is negligible.
        if (string.Equals(tree, PartCrdtTreeId, StringComparison.Ordinal))
        {
            return key.Contains(PartCrdtLabelsInfix, StringComparison.Ordinal);
        }

        return true;
    }

    // Duplicated from Lattice/PartCrdtStore.cs on purpose (see
    // IsKeyReplicated remarks). Keep these two in sync.
    private const string PartCrdtTreeId = "mfg-part-crdt";
    private const string PartCrdtLabelsInfix = "/labels/";

    /// <summary>
    /// Binds a <see cref="ReplicationTopology"/> from the supplied
    /// config root. Expected JSON shape under <c>"Replication"</c>:
    /// <code>
    ///   "Replication": {
    ///     "LocalCluster": "forge",
    ///     "SharedSecret": "...",
    ///     "Peers": [
    ///       { "Name": "heattreat", "BaseUrls": [ "http://silo-heattreat-a:8080", "http://silo-heattreat-b:8080" ] }
    ///     ],
    ///     "ReplicatedTrees": [ "mfg-facts", "mfg-site-activity-index" ]
    ///   }
    /// </code>
    /// <para>
    /// For backward compatibility a legacy single <c>BaseUrl</c>
    /// scalar is also accepted and is promoted to a one-element
    /// <c>BaseUrls</c> list.
    /// </para>
    /// </summary>
    public static ReplicationTopology Load(IConfiguration root)
    {
        ArgumentNullException.ThrowIfNull(root);
        var section = root.GetSection("Replication");
        var local = section["LocalCluster"]
            ?? throw new InvalidOperationException("Replication:LocalCluster is required.");
        var secret = section["SharedSecret"]
            ?? throw new InvalidOperationException("Replication:SharedSecret is required.");

        var peers = section.GetSection("Peers").GetChildren()
            .Select(p =>
            {
                var name = p["Name"] ?? throw new InvalidOperationException("Peer:Name is required.");

                // Prefer the BaseUrls list; fall back to a single BaseUrl scalar.
                var urlList = p.GetSection("BaseUrls").GetChildren()
                    .Select(c => c.Value)
                    .Where(v => !string.IsNullOrWhiteSpace(v))
                    .Select(v => v!)
                    .ToList();

                if (urlList.Count == 0)
                {
                    var single = p["BaseUrl"];
                    if (!string.IsNullOrWhiteSpace(single))
                    {
                        urlList.Add(single);
                    }
                }

                if (urlList.Count == 0)
                {
                    throw new InvalidOperationException(
                        $"Peer:BaseUrls (or legacy Peer:BaseUrl) is required (peer '{name}').");
                }

                var uris = new Uri[urlList.Count];
                for (var i = 0; i < urlList.Count; i++)
                {
                    if (!Uri.TryCreate(urlList[i], UriKind.Absolute, out var u))
                    {
                        throw new InvalidOperationException(
                            $"Peer:BaseUrls[{i}] is not a valid absolute URI (peer '{name}', value '{urlList[i]}').");
                    }
                    uris[i] = u;
                }
                return new ReplicationPeer(name, uris);
            })
            .ToArray();

        var trees = section.GetSection("ReplicatedTrees").GetChildren()
            .Select(c => c.Value)
            .Where(v => !string.IsNullOrWhiteSpace(v))
            .Select(v => v!)
            .ToArray();

        if (trees.Length == 0)
        {
            trees = DefaultReplicatedTrees;
        }

        return new ReplicationTopology
        {
            LocalCluster = local,
            SharedSecret = secret,
            Peers = peers,
            ReplicatedTrees = trees,
        };
    }

    /// <summary>
    /// Trees the sample opts in by default when config omits the
    /// list: the fact log and the site-activity index. Both use
    /// immutable write-once keys, so cross-cluster replay is
    /// trivially safe. <c>mfg-part-crdt</c> is <b>not</b> on the
    /// default list — its LWW-register half would diverge under the
    /// current application-layer apply path (see plan §13.7).
    /// </summary>
    public static string[] DefaultReplicatedTrees =>
    [
        "mfg-facts",
        "mfg-site-activity-index",
    ];
}
