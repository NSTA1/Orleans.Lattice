namespace Orleans.Lattice.Tests.Fakes;

/// <summary>
/// A minimal active <see cref="ILatticeEnvelopeCodec"/> that stamps and strips a
/// fixed-width version envelope, standing in for the real schema-versioning codec
/// without taking a cross-package dependency on the schema add-on.
/// </summary>
/// <remarks>
/// <para>
/// The header deliberately mirrors the real one in the single respect the
/// envelope-strip regression turns on: the magic lead byte is <c>0xFE</c>, chosen
/// by the production format precisely because it is never a valid UTF-8 lead byte.
/// That is what makes an unstripped state decode fail at byte zero with
/// <c>'0xFE' is an invalid start of a value</c> rather than degrade quietly, and a
/// fake using any other lead byte would not reproduce the defect faithfully.
/// </para>
/// <para>
/// <see cref="StripForFold"/> is version-agnostic and never upcasts, matching the
/// determinism contract on <see cref="ILatticeEnvelopeCodec"/>.
/// </para>
/// </remarks>
internal sealed class FakeEnvelopeCodec : ILatticeEnvelopeCodec
{
    /// <summary>The envelope magic. Never a valid UTF-8 lead byte.</summary>
    internal const byte Magic = 0xFE;

    /// <summary>Magic plus a little-endian <see cref="uint"/> version.</summary>
    internal const int HeaderLength = 1 + sizeof(uint);

    private readonly bool _active;

    internal FakeEnvelopeCodec(bool active = true) => _active = active;

    /// <summary>Counts <see cref="StripForFold"/> calls, so a test can assert the seam was reached.</summary>
    internal int StripCallCount { get; private set; }

    /// <summary>Wraps <paramref name="body"/> in an envelope stamped at <paramref name="version"/>.</summary>
    internal static byte[] Encode(byte[] body, uint version = 1)
    {
        ArgumentNullException.ThrowIfNull(body);
        var buffer = new byte[HeaderLength + body.Length];
        buffer[0] = Magic;
        BitConverter.TryWriteBytes(buffer.AsSpan(1, sizeof(uint)), version);
        body.CopyTo(buffer, HeaderLength);
        return buffer;
    }

    private static bool IsEnveloped(byte[]? value) =>
        value is { Length: >= HeaderLength } && value[0] == Magic;

    public bool IsActive(string treeId) => _active;

    public uint ReadVersion(byte[]? value) =>
        IsEnveloped(value) ? BitConverter.ToUInt32(value.AsSpan(1, sizeof(uint))) : 0u;

    public byte[] StripForFold(byte[] delta)
    {
        StripCallCount++;
        return IsEnveloped(delta) ? delta[HeaderLength..] : delta;
    }
}
