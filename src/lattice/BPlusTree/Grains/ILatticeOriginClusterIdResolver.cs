namespace Orleans.Lattice;

/// <summary>
/// Producer-side seam that resolves the local <c>OriginClusterId</c> stamped
/// onto every <see cref="WalRecord"/> at WAL-append time when the originating
/// <see cref="LatticeMutation.OriginClusterId"/> is <c>null</c> (i.e. the
/// foreground commit path on a single-cluster host or a not-yet-stamped
/// observer-driven write). The mutation's own origin still wins when present
/// — this resolver only supplies the fallback so multi-site hosts can record
/// "this WAL entry was authored locally by &lt;cluster-id&gt;" without forcing
/// every commit-time call site to thread the cluster id through.
/// <para>
/// Returning <see cref="string.Empty"/> means "no cluster id is configured";
/// the resulting record's <see cref="WalRecord.OriginClusterId"/> is also
/// empty (the converter does not substitute anything else). Hosts that
/// register the replication package replace this default with one that
/// reads <c>LatticeReplicationOptions.ClusterId</c>; single-cluster hosts
/// keep the empty default and remain unaffected.
/// </para>
/// </summary>
public interface ILatticeOriginClusterIdResolver
{
    /// <summary>
    /// Returns the local cluster id to stamp on a WAL record authored for
    /// <paramref name="treeId"/>, or <see cref="string.Empty"/> when no
    /// cluster id is configured. Called on the commit-time hot path;
    /// implementations should be O(1) and side-effect free.
    /// </summary>
    /// <param name="treeId">The logical tree id the mutation was committed to.</param>
    string Resolve(string treeId);
}
