using System.Buffers.Binary;
using System.Text;

namespace Orleans.Lattice.Vector.Persistence;

/// <summary>
/// The durable checkpoint of an in-flight background build: which generation is
/// being built, how far the source has been consumed, and the identifier to
/// resume strictly after.
/// <para>
/// It is written <i>after</i> the vectors it accounts for are durable, so the
/// cursor it names can only ever be behind the persisted index, never ahead of
/// it. A crash between the two therefore re-consumes one batch, which is
/// harmless because ingest is an upsert, rather than skipping one, which would
/// silently lose vectors.
/// </para>
/// </summary>
/// <param name="Generation">The generation being built.</param>
/// <param name="Phase">The phase reached.</param>
/// <param name="Ingested">How many vectors have been durably ingested.</param>
/// <param name="Expected">How many the source held when it was last counted.</param>
/// <param name="Cursor">The last source identifier durably consumed, or <see langword="null"/> at the start.</param>
public readonly record struct VectorIndexBuildState(
    long Generation,
    VectorIndexBuildPhase Phase,
    int Ingested,
    int Expected,
    string? Cursor)
{
    private const int FixedSize = 24;

    /// <summary>Renders the build state as a complete, checksummed durable record.</summary>
    /// <exception cref="ArgumentOutOfRangeException">The cursor is too long to encode.</exception>
    public byte[] ToRecord()
    {
        var cursorBytes = Cursor is null ? [] : Encoding.UTF8.GetBytes(Cursor);
        var payload = new byte[FixedSize + cursorBytes.Length];
        var span = payload.AsSpan();

        BinaryPrimitives.WriteInt64LittleEndian(span[..8], Generation);
        BinaryPrimitives.WriteInt32LittleEndian(span.Slice(8, 4), (int)Phase);
        BinaryPrimitives.WriteInt32LittleEndian(span.Slice(12, 4), Ingested);
        BinaryPrimitives.WriteInt32LittleEndian(span.Slice(16, 4), Expected);
        BinaryPrimitives.WriteInt32LittleEndian(span.Slice(20, 4), Cursor is null ? -1 : cursorBytes.Length);
        cursorBytes.CopyTo(span[FixedSize..]);
        return VectorIndexRecord.Wrap(payload);
    }

    /// <summary>
    /// Decodes a build state from a complete durable record without throwing, so
    /// an unreadable checkpoint means "start the build again" rather than a
    /// fault.
    /// </summary>
    /// <param name="record">The persisted record bytes.</param>
    /// <param name="state">The decoded state when this returns <see langword="true"/>.</param>
    /// <returns><see langword="true"/> when the record is a build state this build can act on.</returns>
    public static bool TryReadRecord(ReadOnlySpan<byte> record, out VectorIndexBuildState state)
    {
        state = default;
        if (!VectorIndexRecord.TryUnwrap(record, out var payload) || payload.Length < FixedSize)
        {
            return false;
        }

        var generation = BinaryPrimitives.ReadInt64LittleEndian(payload[..8]);
        var phase = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(8, 4));
        var ingested = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(12, 4));
        var expected = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(16, 4));
        var cursorLength = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(20, 4));

        if (generation < 0 || ingested < 0 || expected < 0 || cursorLength < -1)
        {
            return false;
        }

        if (phase is < (int)VectorIndexBuildPhase.NotStarted or > (int)VectorIndexBuildPhase.Ready)
        {
            return false;
        }

        string? cursor = null;
        if (cursorLength >= 0)
        {
            if (payload.Length != FixedSize + cursorLength)
            {
                return false;
            }

            cursor = Encoding.UTF8.GetString(payload.Slice(FixedSize, cursorLength));
        }
        else if (payload.Length != FixedSize)
        {
            return false;
        }

        state = new VectorIndexBuildState(generation, (VectorIndexBuildPhase)phase, ingested, expected, cursor);
        return true;
    }
}
