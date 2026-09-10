using System.Buffers;
using Orleans.Serialization;
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

        // Write the magic and the payload into one buffer so the returned
        // BinaryData wraps the writer's array directly. BinaryData is
        // contiguous by contract, so one buffer the size of the payload is
        // the floor here; what this avoids is the additional 2.7x
        // contiguous UTF-16 intermediate the JSON path cannot avoid.
        var writer = new ArrayBufferWriter<byte>();
        BinaryMagic.CopyTo(writer.GetSpan(BinaryMagic.Length));
        writer.Advance(BinaryMagic.Length);
        this.serializer.Serialize(value, writer);
        return new BinaryData(writer.WrittenMemory);
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
