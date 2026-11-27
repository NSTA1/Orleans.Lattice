using System.IO.Hashing;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Auth;

/// <summary>
/// Reads an anchor record for the admin audit trail, under system origin. A tree in
/// the core reserved <c>_lattice_</c> namespace (the tree registry, which holds
/// aliases) is read through the internal system-tree surface, which exposes the
/// value but no version; every other anchor tree is read with its version.
/// </summary>
internal static class AdminAuditAnchorReader
{
    /// <summary>Reads <paramref name="anchor"/>.</summary>
    /// <param name="grainFactory">The grain factory.</param>
    /// <param name="anchor">The anchor.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    internal static async Task<AdminAuditAnchorObservation> ReadAsync(
        IGrainFactory grainFactory,
        LatticeAdminAuditAnchor anchor,
        CancellationToken cancellationToken)
    {
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            if (IsSystemTree(anchor.TreeId))
            {
                var bytes = await grainFactory.GetGrain<ISystemLattice>(anchor.TreeId)
                    .GetAsync(anchor.Key, cancellationToken).ConfigureAwait(false);
                return new AdminAuditAnchorObservation(null, Hash(bytes), Versioned: false);
            }

            var versioned = await grainFactory.GetGrain<ILattice>(anchor.TreeId)
                .GetWithVersionAsync(anchor.Key, cancellationToken).ConfigureAwait(false);
            HybridLogicalClock? version = versioned.Value is null ? null : versioned.Version;
            return new AdminAuditAnchorObservation(version, Hash(versioned.Value), Versioned: true);
        }
    }

    /// <summary>Whether <paramref name="treeId"/> is a core reserved system tree.</summary>
    /// <param name="treeId">The tree id.</param>
    internal static bool IsSystemTree(string treeId) =>
        treeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal);

    /// <summary>The xxHash64 of <paramref name="value"/> as history revisions carry it, <c>0</c> for none.</summary>
    /// <param name="value">The value bytes.</param>
    internal static long Hash(byte[]? value) =>
        value is null ? 0 : unchecked((long)XxHash64.HashToUInt64(value));
}
