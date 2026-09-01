using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Decodes a stored vector payload record back into its components. It is a
/// single shared kernel deliberately: the exact scan and the approximate plane's
/// background build both read the same content-addressed payload tree, and two
/// copies of this decode would be two things to keep in step for no benefit.
/// </summary>
internal static class RepoContextVectorPayloads
{
    /// <summary>
    /// Decodes one persisted <see cref="VectorPayloadRecord"/> into its vector
    /// components, or returns <see langword="null"/> when the record carries no
    /// payload element.
    /// </summary>
    /// <param name="serializer">The Orleans serializer used to decode the record. Must not be <see langword="null"/>.</param>
    /// <param name="payloadBytes">The serialized payload record. Must not be <see langword="null"/>.</param>
    /// <returns>The decoded components, or <see langword="null"/> when the record is empty.</returns>
    /// <exception cref="ArgumentNullException">An argument is null.</exception>
    internal static float[]? Decode(Serializer serializer, byte[] payloadBytes)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(payloadBytes);

        var payload = serializer.Deserialize<VectorPayloadRecord>(payloadBytes);
        var encoded = FirstElement(payload.Payload);
        return encoded is null ? null : VectorCodec.Decode(encoded);
    }

    private static byte[]? FirstElement(GSet payload)
    {
        foreach (var element in payload.Elements)
        {
            return Convert.FromBase64String(element);
        }

        return null;
    }
}
