using System.Buffers;
using Orleans.Serialization;
using Orleans.Serialization.Buffers;
using Orleans.Serialization.Buffers.Adaptors;
using Orleans.Storage;

namespace Orleans.Lattice;

/// <summary>
/// The grain-storage serializer Lattice installs on the silo. It writes
/// state types marked <see cref="ILatticeBinaryPersistedState"/> through the
/// Orleans binary serializer and delegates every other type, unchanged, to
/// the serializer that was registered before it.
/// <para>
/// This exists because the default JSON grain-storage serializer cannot
/// write a large opaque payload without first materialising the entire
/// document as one contiguous UTF-16 string
/// (<see cref="System.Text.StringBuilder"/> chunks, then
/// <c>StringBuilder.ToString()</c>). That single allocation is roughly 2.7x
/// the payload - base64 inflates the bytes by 4/3, and each character costs
/// two bytes - and it is contiguous, so on a large leaf snapshot it is the
/// allocation that fails first when a silo replays a warm volume under
/// memory pressure. Chunking the payload inside the state type does not
/// help: the cost is a function of the whole document, not of any one
/// member. Only a different serializer removes it.
/// </para>
/// <para>
/// Payloads are self-describing. A binary payload is prefixed with a
/// four-byte magic, so <see cref="Deserialize{T}"/> can tell one from a
/// JSON document written by an earlier build and route it accordingly. That
/// makes the change compatible in both directions with no migration: rows
/// already on disk stay readable, and rows this serializer writes stay
/// readable if a type is later unmarked. No JSON document can be mistaken
/// for a binary payload, because a JSON document never begins with these
/// bytes.
/// </para>
/// </summary>
public sealed class LatticeGrainStorageSerializer : IGrainStorageSerializer
{
    /// <summary>
    /// The four-byte prefix that identifies a payload written by the
    /// Orleans binary serializer rather than by the fallback serializer.
    /// Chosen so it cannot collide with the start of a JSON document.
    /// </summary>
    internal static ReadOnlySpan<byte> BinaryMagic => "LGB1"u8;

    private readonly Serializer serializer;
    private readonly IGrainStorageSerializer fallback;

    /// <summary>
    /// Creates the serializer.
    /// </summary>
    /// <param name="serializer">
    /// The Orleans binary serializer used for marked state types.
    /// </param>
    /// <param name="fallback">
    /// The serializer used for every unmarked state type. This is the
    /// serializer that was registered before Lattice installed this one,
    /// normally the Orleans JSON grain-storage serializer, so an unmarked
    /// type is written exactly as it was before.
    /// </param>
    public LatticeGrainStorageSerializer(Serializer serializer, IGrainStorageSerializer fallback)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(fallback);

        this.serializer = serializer;
        this.fallback = fallback;
    }

    /// <summary>
    /// The serializer that unmarked state types are delegated to.
    /// </summary>
    public IGrainStorageSerializer Fallback => this.fallback;

    /// <summary>
    /// Reports whether <paramref name="stateType"/> is written through the
    /// Orleans binary serializer, that is, whether it is marked
    /// <see cref="ILatticeBinaryPersistedState"/>.
    /// </summary>
    /// <param name="stateType">The persisted state type.</param>
    public static bool WritesBinary(Type stateType)
    {
        ArgumentNullException.ThrowIfNull(stateType);
        return typeof(ILatticeBinaryPersistedState).IsAssignableFrom(stateType);
    }

    /// <summary>
    /// Serializes <paramref name="value"/>, using the Orleans binary
    /// serializer when the state type is marked
    /// <see cref="ILatticeBinaryPersistedState"/> and the configured
    /// fallback otherwise.
    /// </summary>
    /// <typeparam name="T">The persisted state type.</typeparam>
    /// <param name="value">The state to serialize.</param>
    public BinaryData Serialize<T>(T value)
    {
        if (!WritesBinary(typeof(T)))
        {
            return this.fallback.Serialize(value);
        }

        // Write the magic and the payload into one exact-size array so the
        // returned BinaryData wraps it directly.
        //
        // On the transient cost, stated accurately because this is the comment
        // someone reads while diagnosing an OutOfMemoryException on this line.
        // BinaryData is contiguous by contract, so ONE array the size of the
        // payload is the irreducible floor, and the two passes below achieve
        // it. The obstacle is that the serialized length is not known until the
        // value has been serialized, so the destination cannot be pre-sized.
        //
        // Pass one serializes into a PooledBuffer, a SEGMENTED writer that
        // grows by renting further pooled pages rather than by reallocating and
        // copying. Nothing contiguous the size of the payload is allocated, and
        // the pages are returned on Dispose. Pass two allocates the single
        // exact-size array - now that Length is known - and copies into it.
        // Peak contiguous allocation is therefore ~1x the payload.
        //
        // The obvious ArrayBufferWriter<byte> is what this replaced, and it is
        // the shape to avoid: it is backed by ONE array that it grows by
        // doubling, holding the old array and the new one simultaneously at
        // each step, for a peak near 3x the payload. Measured on a 4 MiB
        // payload it allocated 2.75x against 1.00x here. Do not reintroduce it
        // for the convenience of WrittenMemory.
        //
        // This is additive to what the path already bought over the JSON
        // fallback, whose 2.7x contiguous UTF-16 intermediate is unavoidable
        // there. Both together lower the multiplier to its floor, but a payload
        // larger than the address space can serve contiguously still fails, so
        // bounding the payload remains necessary rather than optional; see
        // LatticeOptions.MaxLeafBytes and the byte-overflow pre-split in
        // BPlusLeafGrain.Snapshot.cs.
        //
        // PooledBuffer is a struct and Serializer.Serialize takes its writer by
        // value, so it MUST be passed through BufferWriterBox, whose Value is a
        // ref-returning property. Passing the struct directly compiles and then
        // serializes into a copy, leaving Length at zero - a silent corruption,
        // not a compile error.
        var box = new BufferWriterBox<PooledBuffer>(new PooledBuffer());
        try
        {
            this.serializer.Serialize(value, box);

            var exact = new byte[BinaryMagic.Length + box.Value.Length];
            BinaryMagic.CopyTo(exact);
            box.Value.CopyTo(exact.AsSpan(BinaryMagic.Length));
            return new BinaryData(exact);
        }
        finally
        {
            box.Value.Dispose();
        }
    }

    /// <summary>
    /// Deserializes <paramref name="input"/>, detecting from the payload
    /// itself whether it was written by the Orleans binary serializer or by
    /// the fallback, so state written by any build is readable by any other.
    /// </summary>
    /// <typeparam name="T">The persisted state type.</typeparam>
    /// <param name="input">The stored payload.</param>
    public T Deserialize<T>(BinaryData input)
    {
        ArgumentNullException.ThrowIfNull(input);

        var bytes = input.ToMemory();

        // Route on the payload, not on the type. A type that was marked
        // when the row was written may since have been unmarked, and a row
        // written before the type was marked is still JSON; both must read.
        if (!HasBinaryMagic(bytes.Span))
        {
            return this.fallback.Deserialize<T>(input);
        }

        return this.serializer.Deserialize<T>(
            new ReadOnlySequence<byte>(bytes[BinaryMagic.Length..]));
    }

    private static bool HasBinaryMagic(ReadOnlySpan<byte> payload) =>
        payload.Length >= BinaryMagic.Length
        && payload[..BinaryMagic.Length].SequenceEqual(BinaryMagic);
}
