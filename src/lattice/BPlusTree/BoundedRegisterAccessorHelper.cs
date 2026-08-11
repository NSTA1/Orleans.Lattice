using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice;

/// <summary>
/// Shared read/write plumbing for the monotone bounded-register accessors
/// (<see cref="MaxRegisterAccessor{T}"/> and <see cref="MinRegisterAccessor{T}"/>).
/// The two directions differ only in the <see cref="LatticeMergeMode"/> they
/// write and the empty-state direction the shape stamps; the candidate-delta
/// encoding and the state read are identical, so both accessors route through
/// this helper.
/// </summary>
internal static class BoundedRegisterAccessorHelper
{
    /// <summary>
    /// Encodes a blind candidate delta for <paramref name="value"/>. No read is
    /// required: the directional fold is a no-op when the candidate does not beat
    /// the current value, so shipping the candidate unconditionally converges to
    /// the same state at minimal allocation.
    /// </summary>
    public static byte[] EncodeDelta<T>(ILatticeSerializer<T> serializer, Func<T, byte[]> orderKeySelector, T value)
    {
        var encoded = serializer.Serialize(value);
        var orderKey = orderKeySelector(value);
        ArgumentNullException.ThrowIfNull(orderKey);
        var delta = new BoundedRegisterDelta
        {
            Value = encoded,
            OrderKey = orderKey,
            HasValue = true,
        };
        return JsonLatticeSerializer<BoundedRegisterDelta>.Default.Serialize(delta);
    }

    /// <summary>Reads and decodes the stored register, or an empty register in the given direction when the key is absent.</summary>
    public static async Task<BoundedRegister> ReadAsync(ILattice lattice, string key, bool isMin, CancellationToken cancellationToken)
    {
        var bytes = await lattice.GetAsync(key, cancellationToken).ConfigureAwait(false);
        return bytes is null
            ? BoundedRegister.CreateEmpty(isMin)
            : JsonLatticeSerializer<BoundedRegister>.Default.Deserialize(bytes);
    }
}
