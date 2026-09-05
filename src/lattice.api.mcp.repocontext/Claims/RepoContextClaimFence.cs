using System.Buffers.Binary;
using System.Text;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The fencing primitive behind the repository-context claim surface: it encodes a
/// <see cref="LockToken.FencingToken"/> into a memory record's claim registers, reads
/// it back, and decides whether one write may proceed under the token its caller
/// presented.
/// <para>
/// The claim state is not a new storage primitive. It is four ordinary
/// <see cref="BoundedRegister"/> slots on <see cref="MemoryRecord"/>, and the
/// fencing guarantee falls out of the register's own lattice: a
/// <see cref="BoundedRegister"/> is a monotone max-register ordered by a
/// caller-supplied total-order key, so writing the fencing token as <em>both</em>
/// the value and the order key makes the stored fence a join-semilattice maximum
/// over tokens. A lower token can never displace a higher one - not through a
/// direct write, and not through a concurrent CRDT merge from another replica.
/// That is what makes the high-water mark trustworthy without a compare-and-swap
/// the store does not have.
/// </para>
/// <para>
/// The same order key is used for the owner and region registers, so those two
/// always describe the grant that owns the current fence rather than drifting
/// independently under last-writer-wins.
/// </para>
/// </summary>
internal static class RepoContextClaimFence
{
    /// <summary>The fixed width of an encoded fencing token, in bytes.</summary>
    internal const int TokenWidth = sizeof(long);

    /// <summary>
    /// The largest region identifier encoded on the stack during a region
    /// comparison; longer identifiers fall back to a heap buffer. Region ids are
    /// cluster names, so the stack path is the only one taken in practice.
    /// </summary>
    private const int MaxStackRegionBytes = 256;

    /// <summary>
    /// Encodes <paramref name="fencingToken"/> as an eight-byte, order-preserving
    /// total-order key: big-endian with the sign bit flipped, so ordinal byte
    /// comparison of two encodings agrees with numeric comparison of the tokens
    /// across the whole <see cref="long"/> range.
    /// </summary>
    /// <param name="fencingToken">The token to encode.</param>
    /// <returns>The encoded key.</returns>
    internal static byte[] Encode(long fencingToken)
    {
        var encoded = new byte[TokenWidth];
        BinaryPrimitives.WriteUInt64BigEndian(encoded, unchecked((ulong)fencingToken) ^ 0x8000_0000_0000_0000UL);
        return encoded;
    }

    /// <summary>
    /// Decodes a register written by <see cref="Encode"/>, or <see langword="null"/>
    /// when the register has never been written or does not carry a well-formed
    /// token. Allocation-free.
    /// </summary>
    /// <param name="register">The register to read. Must not be <see langword="null"/>.</param>
    /// <returns>The token, or <see langword="null"/> when the slot is unset or malformed.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="register"/> is null.</exception>
    internal static long? Decode(BoundedRegister register)
    {
        ArgumentNullException.ThrowIfNull(register);
        if (!register.HasValue || register.Value is not { Length: TokenWidth } value)
        {
            return null;
        }

        return unchecked((long)(BinaryPrimitives.ReadUInt64BigEndian(value) ^ 0x8000_0000_0000_0000UL));
    }

    /// <summary>
    /// Reads a text register (the claim owner or region) as a string, or
    /// <see langword="null"/> when it has never been written.
    /// </summary>
    /// <param name="register">The register to read. Must not be <see langword="null"/>.</param>
    /// <returns>The decoded text, or <see langword="null"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="register"/> is null.</exception>
    internal static string? DecodeText(BoundedRegister register)
    {
        ArgumentNullException.ThrowIfNull(register);
        return register.HasValue && register.Value is { } value ? Encoding.UTF8.GetString(value) : null;
    }

    /// <summary>
    /// Compares a stored text register against <paramref name="candidate"/> without
    /// materialising the stored value as a string. Used on the write path, which
    /// runs on every fenced remember and update, so the common "region matches"
    /// case costs no allocation at all.
    /// </summary>
    /// <param name="register">The stored register. Must not be <see langword="null"/>.</param>
    /// <param name="candidate">The value to compare against. Must not be <see langword="null"/>.</param>
    /// <returns><see langword="true"/> when the register holds exactly <paramref name="candidate"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="register"/> or <paramref name="candidate"/> is null.</exception>
    internal static bool TextEquals(BoundedRegister register, string candidate)
    {
        ArgumentNullException.ThrowIfNull(register);
        ArgumentNullException.ThrowIfNull(candidate);

        if (!register.HasValue || register.Value is not { } stored)
        {
            return false;
        }

        var byteCount = Encoding.UTF8.GetByteCount(candidate);
        if (byteCount != stored.Length)
        {
            return false;
        }

        if (byteCount <= MaxStackRegionBytes)
        {
            Span<byte> scratch = stackalloc byte[MaxStackRegionBytes];
            var written = Encoding.UTF8.GetBytes(candidate, scratch);
            return scratch[..written].SequenceEqual(stored);
        }

        return Encoding.UTF8.GetBytes(candidate).AsSpan().SequenceEqual(stored);
    }

    /// <summary>
    /// Projects the claim state currently recorded on <paramref name="record"/>.
    /// </summary>
    /// <param name="record">The memory record to read. Must not be <see langword="null"/>.</param>
    /// <returns>The projected claim state.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="record"/> is null.</exception>
    internal static RepoContextClaimState Read(MemoryRecord record)
    {
        ArgumentNullException.ThrowIfNull(record);
        return new RepoContextClaimState(
            Decode(record.ClaimFence),
            Decode(record.ClaimReleasedFence),
            DecodeText(record.ClaimOwner),
            DecodeText(record.ClaimRegion));
    }

    /// <summary>
    /// Decides whether a write may proceed against the claim state on
    /// <paramref name="record"/>. Pure: no clock, no lock, no I/O.
    /// <para>
    /// The rules, in order. A record with no live claim admits any write, fenced or
    /// not, so an unclaimed record behaves exactly as it did before the claim
    /// surface existed. A record with a live claim admits only the holder of its
    /// current fence, in the region the claim was taken in. A token below the
    /// record's high-water mark is always refused, even when the claim has since
    /// been released, because a superseded holder must never be able to write.
    /// </para>
    /// </summary>
    /// <param name="record">The record being written. Must not be <see langword="null"/>.</param>
    /// <param name="presentedToken">The fencing token the caller presented, or <see langword="null"/> for an unfenced write.</param>
    /// <param name="region">The region identity serving this write. Must not be <see langword="null"/>.</param>
    /// <returns>The admission decision.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="record"/> or <paramref name="region"/> is null.</exception>
    internal static RepoContextFenceVerdict Evaluate(
        MemoryRecord record, long? presentedToken, string region)
    {
        ArgumentNullException.ThrowIfNull(record);
        ArgumentNullException.ThrowIfNull(region);

        var fence = Decode(record.ClaimFence);
        var released = Decode(record.ClaimReleasedFence);

        if (fence is not { } highWaterMark)
        {
            // The record has never been claimed. Any write is admitted, which is
            // what keeps every pre-existing caller working unchanged.
            return RepoContextFenceVerdict.Accepted;
        }

        var live = released is not { } releasedToken || releasedToken < highWaterMark;

        if (presentedToken is not { } presented)
        {
            return live ? RepoContextFenceVerdict.ClaimRequired : RepoContextFenceVerdict.Accepted;
        }

        if (presented < highWaterMark)
        {
            return RepoContextFenceVerdict.StaleToken;
        }

        if (!live)
        {
            return RepoContextFenceVerdict.ClaimReleased;
        }

        return TextEquals(record.ClaimRegion, region)
            ? RepoContextFenceVerdict.Accepted
            : RepoContextFenceVerdict.ForeignRegion;
    }

    /// <summary>
    /// Stamps a granted claim onto <paramref name="record"/>: the fence, the owner,
    /// and the region are each advanced under the token's order key, so all three
    /// move together and none can be dragged backwards by a stale writer.
    /// </summary>
    /// <param name="record">The record to stamp. Must not be <see langword="null"/>.</param>
    /// <param name="fencingToken">The token granted by the lock.</param>
    /// <param name="owner">The claiming agent identity. Must not be <see langword="null"/>.</param>
    /// <param name="region">The region the claim was taken in. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException">Any argument is null.</exception>
    internal static void StampClaim(MemoryRecord record, long fencingToken, string owner, string region)
    {
        ArgumentNullException.ThrowIfNull(record);
        ArgumentNullException.ThrowIfNull(owner);
        ArgumentNullException.ThrowIfNull(region);

        // One order key serves all three registers. BoundedRegister takes the array
        // by reference and never mutates it (a later Set replaces the reference, and
        // Clone / MergeFrom copy), so sharing one encoding is safe and keeps a claim
        // to two token allocations instead of six.
        var orderKey = Encode(fencingToken);
        record.ClaimFence.Set(Encode(fencingToken), orderKey);
        record.ClaimOwner.Set(Encoding.UTF8.GetBytes(owner), orderKey);
        record.ClaimRegion.Set(Encoding.UTF8.GetBytes(region), orderKey);
    }

    /// <summary>
    /// Stamps a release onto <paramref name="record"/> by advancing the released
    /// high-water mark to <paramref name="fencingToken"/>. The fence itself is left
    /// where it is: a released token must still be refused if it is later presented
    /// against a record another claim has since fenced past.
    /// </summary>
    /// <param name="record">The record to stamp. Must not be <see langword="null"/>.</param>
    /// <param name="fencingToken">The token being released.</param>
    /// <exception cref="ArgumentNullException"><paramref name="record"/> is null.</exception>
    internal static void StampRelease(MemoryRecord record, long fencingToken)
    {
        ArgumentNullException.ThrowIfNull(record);
        record.ClaimReleasedFence.Set(Encode(fencingToken), Encode(fencingToken));
    }

    /// <summary>
    /// The caller-facing explanation for a refused write, naming the verdict and
    /// the state that produced it.
    /// </summary>
    /// <param name="verdict">The refusal. <see cref="RepoContextFenceVerdict.Accepted"/> is not a refusal and is rejected.</param>
    /// <param name="key">The record key the write targeted.</param>
    /// <param name="state">The claim state read off the record.</param>
    /// <param name="presentedToken">The token the caller presented, or <see langword="null"/>.</param>
    /// <param name="region">The region serving the write.</param>
    /// <returns>The diagnostic message.</returns>
    internal static string Explain(
        RepoContextFenceVerdict verdict,
        string key,
        RepoContextClaimState state,
        long? presentedToken,
        string region) => verdict switch
        {
            RepoContextFenceVerdict.StaleToken =>
                $"The write to '{key}' presented fencing token {presentedToken} but the record has already seen "
                + $"token {state.FencingToken}. The claim was superseded; re-claim before writing again.",
            RepoContextFenceVerdict.ClaimRequired =>
                $"The record '{key}' is claimed by '{state.Owner}' under fencing token {state.FencingToken}. "
                + "Present that token as 'fencingToken', or wait for the claim to lapse or be released.",
            RepoContextFenceVerdict.ClaimReleased =>
                $"The claim on '{key}' under fencing token {presentedToken} has already been released. "
                + "Re-claim the record before writing again.",
            RepoContextFenceVerdict.ForeignRegion =>
                $"The record '{key}' is claimed in region '{state.Region}' but this write is served from "
                + $"region '{region}'. Claims are cluster-scoped, so the write fails closed; claim it in "
                + "its home region instead.",
            _ => $"The write to '{key}' was admitted.",
        };
}
