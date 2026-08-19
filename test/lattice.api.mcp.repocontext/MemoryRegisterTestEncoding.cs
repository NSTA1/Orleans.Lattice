using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Test-only encoder for the stored agent-memory representation. The store no
/// longer persists a bare <see cref="MemoryRecord"/> under a memory key: it persists
/// an <see cref="MvRegister"/> envelope whose concurrent per-replica values are the
/// Orleans-serialized record bytes (see <c>RepoContextMemoryCodec</c>). A test that
/// seeds a memory value by hand - to drive the projection, the recall staleness
/// path, or the portability re-import merge - must produce that same envelope, which
/// this helper builds so no test hand-rolls the wire shape.
/// </summary>
internal static class MemoryRegisterTestEncoding
{
    /// <summary>
    /// Encodes a single replica's write as the stored <see cref="MvRegister"/>
    /// envelope - the shape a one-cluster deployment persists.
    /// </summary>
    /// <param name="serializer">The Orleans serializer used to encode the record. Must not be <see langword="null"/>.</param>
    /// <param name="replicaId">The authoring replica id.</param>
    /// <param name="record">The record to store.</param>
    /// <returns>The JSON-encoded register envelope bytes.</returns>
    internal static byte[] EncodeSingle(Serializer serializer, string replicaId, MemoryRecord record)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        var register = new MvRegister();
        register.Set(replicaId, serializer.SerializeToArray(record));
        return JsonLatticeSerializer<MvRegister>.Default.Serialize(register);
    }

    /// <summary>
    /// Encodes several writes as one envelope with every value <b>causally
    /// concurrent</b>: each write is applied to a fresh register (so its dot is
    /// independent) and the registers are folded with <see cref="MvRegister.Merge"/>,
    /// reproducing the state two clusters reach after writing the same key without
    /// observing each other. Every value therefore survives the fold.
    /// </summary>
    /// <param name="serializer">The Orleans serializer used to encode each record. Must not be <see langword="null"/>.</param>
    /// <param name="writes">The concurrent (replicaId, record) writes.</param>
    /// <returns>The JSON-encoded register envelope bytes carrying every concurrent value.</returns>
    internal static byte[] EncodeConcurrent(
        Serializer serializer, params (string ReplicaId, MemoryRecord Record)[] writes)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        MvRegister? merged = null;
        foreach (var (replicaId, record) in writes)
        {
            var register = new MvRegister();
            register.Set(replicaId, serializer.SerializeToArray(record));
            merged = merged is null ? register : MvRegister.Merge(merged, register);
        }

        return JsonLatticeSerializer<MvRegister>.Default.Serialize(merged ?? new MvRegister());
    }
}
