using System.Buffers.Binary;
using System.Text;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Authoring and reading helpers for the last-writer-wins (LWW) scalar registers
/// that back the mutable scalar fields of the repository-context record model.
/// <para>
/// A repo-context scalar is modelled as a <see cref="BoundedRegister"/> - the
/// core monotone max-register CRDT - ordered by a 12-byte, order-preserving
/// encoding of the authoring <see cref="HybridLogicalClock"/>. Because the fold
/// keeps the candidate with the greatest order key, the register keeps the value
/// written at the highest HLC, which is precisely last-writer-wins with
/// deterministic, replica-independent tie-breaking (a property inherited from
/// <see cref="BoundedRegister"/>'s total-order fold). The register composes into
/// an <see cref="OrMap{TKey, TValue}"/> unchanged, so a map of LWW scalars is
/// itself a well-formed CRDT.
/// </para>
/// </summary>
internal static class RepoContextValues
{
    /// <summary>
    /// The fixed length, in bytes, of the order key produced by
    /// <see cref="HlcOrderKey(HybridLogicalClock)"/>: 8 bytes for the wall-clock
    /// ticks followed by 4 bytes for the counter.
    /// </summary>
    internal const int HlcOrderKeyLength = 12;

    /// <summary>
    /// Encodes <paramref name="clock"/> as a fixed-width, order-preserving byte
    /// key: big-endian <see cref="HybridLogicalClock.WallClockTicks"/> followed
    /// by big-endian <see cref="HybridLogicalClock.Counter"/>. For non-negative
    /// clocks (the normal case - ticks and counters are non-negative) the
    /// unsigned lexicographic byte order of the key matches
    /// <see cref="HybridLogicalClock.CompareTo(HybridLogicalClock)"/>, so a
    /// <see cref="BoundedRegister"/> folding on this key resolves to the value
    /// authored at the latest HLC.
    /// </summary>
    /// <param name="clock">The authoring hybrid logical clock.</param>
    internal static byte[] HlcOrderKey(HybridLogicalClock clock)
    {
        var key = new byte[HlcOrderKeyLength];
        BinaryPrimitives.WriteInt64BigEndian(key.AsSpan(0, 8), clock.WallClockTicks);
        BinaryPrimitives.WriteInt32BigEndian(key.AsSpan(8, 4), clock.Counter);
        return key;
    }

    /// <summary>
    /// Creates an LWW scalar register holding the UTF-8 encoding of
    /// <paramref name="value"/>, ordered by <paramref name="clock"/>.
    /// </summary>
    /// <param name="value">The scalar string value. Must not be <see langword="null"/>.</param>
    /// <param name="clock">The authoring hybrid logical clock.</param>
    internal static BoundedRegister Lww(string value, HybridLogicalClock clock)
    {
        ArgumentNullException.ThrowIfNull(value);
        var register = new BoundedRegister();
        register.Set(Encoding.UTF8.GetBytes(value), HlcOrderKey(clock));
        return register;
    }

    /// <summary>
    /// Creates an LWW scalar register holding the big-endian encoding of
    /// <paramref name="value"/>, ordered by <paramref name="clock"/>.
    /// </summary>
    /// <param name="value">The scalar integer value.</param>
    /// <param name="clock">The authoring hybrid logical clock.</param>
    internal static BoundedRegister Lww(long value, HybridLogicalClock clock)
    {
        var payload = new byte[sizeof(long)];
        BinaryPrimitives.WriteInt64BigEndian(payload, value);
        var register = new BoundedRegister();
        register.Set(payload, HlcOrderKey(clock));
        return register;
    }

    /// <summary>
    /// Reads the current string value of an LWW scalar register, or
    /// <see langword="null"/> when the register has never been written.
    /// </summary>
    /// <param name="register">The register to read. Must not be <see langword="null"/>.</param>
    internal static string? ReadString(BoundedRegister register)
    {
        ArgumentNullException.ThrowIfNull(register);
        return register.HasValue ? Encoding.UTF8.GetString(register.Value!) : null;
    }

    /// <summary>
    /// Reads the current integer value of an LWW scalar register, or
    /// <see langword="null"/> when the register has never been written (or was
    /// not written as an 8-byte integer payload).
    /// </summary>
    /// <param name="register">The register to read. Must not be <see langword="null"/>.</param>
    internal static long? ReadInt64(BoundedRegister register)
    {
        ArgumentNullException.ThrowIfNull(register);
        if (!register.HasValue || register.Value!.Length < sizeof(long))
        {
            return null;
        }

        return BinaryPrimitives.ReadInt64BigEndian(register.Value);
    }

    /// <summary>
    /// Reads the wall-clock tick component of the hybrid logical clock that authored
    /// a register's current value, recovered from the leading eight big-endian bytes
    /// of its order key (see <see cref="HlcOrderKey(HybridLogicalClock)"/>). This is
    /// the ingest-time anchor a reconcile compares an on-disk modification time
    /// against, so it needs no separately persisted field. Returns
    /// <see langword="null"/> when the register has never been written or its order
    /// key is too short to carry the wall component.
    /// </summary>
    /// <param name="register">The register to read. Must not be <see langword="null"/>.</param>
    internal static long? ReadHlcWallTicks(BoundedRegister register)
    {
        ArgumentNullException.ThrowIfNull(register);
        if (!register.HasValue || register.OrderKey is not { Length: >= sizeof(long) })
        {
            return null;
        }

        return BinaryPrimitives.ReadInt64BigEndian(register.OrderKey);
    }
}
