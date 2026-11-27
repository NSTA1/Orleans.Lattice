using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Auth;

/// <summary>
/// Storage helpers shared by the admin audit trail, its reconciler and its readers:
/// the tree handle, the record encoding, and the exactly-once finalize.
/// </summary>
internal static class AdminAuditStore
{
    /// <summary>The admin audit tree.</summary>
    /// <param name="grainFactory">The grain factory.</param>
    internal static ILattice Tree(IGrainFactory grainFactory) =>
        grainFactory.GetGrain<ILattice>(AuthConstants.AdminAuditTree);

    /// <summary>Encodes a record.</summary>
    /// <param name="record">The record.</param>
    internal static byte[] Serialize(LatticeAdminAuditRecord record) =>
        JsonLatticeSerializer<LatticeAdminAuditRecord>.Default.Serialize(record);

    /// <summary>Decodes a record, or returns <see langword="null"/> when the bytes are not one.</summary>
    /// <param name="bytes">The stored bytes.</param>
    internal static LatticeAdminAuditRecord? Deserialize(byte[] bytes)
    {
        try
        {
            return JsonLatticeSerializer<LatticeAdminAuditRecord>.Default.Deserialize(bytes);
        }
        catch (System.Text.Json.JsonException)
        {
            return null;
        }
    }

    /// <summary>
    /// Writes <paramref name="final"/> to its final key create-only, then removes its
    /// pending intent. If the final key already exists another finalizer won; only the
    /// intent is removed. Safe to repeat.
    /// </summary>
    /// <param name="grainFactory">The grain factory.</param>
    /// <param name="final">The final record.</param>
    /// <param name="cancellationToken">Cancels the writes.</param>
    /// <returns><see langword="true"/> when this call wrote the final record.</returns>
    internal static async Task<bool> WriteFinalAsync(IGrainFactory grainFactory, LatticeAdminAuditRecord final, CancellationToken cancellationToken)
    {
        var tree = Tree(grainFactory);
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            var written = await tree
                .SetIfVersionAsync(AdminAuditKeys.Final(final), Serialize(final), HybridLogicalClock.Zero, cancellationToken)
                .ConfigureAwait(false);
            await tree.DeleteAsync(AdminAuditKeys.Pending(final.OperationId), cancellationToken).ConfigureAwait(false);
            return written;
        }
    }
}
