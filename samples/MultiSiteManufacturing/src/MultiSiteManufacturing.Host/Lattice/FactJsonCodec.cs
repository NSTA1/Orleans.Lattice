using System.Text.Json;
using System.Text.Json.Serialization;
using MultiSiteManufacturing.Host.Domain;

namespace MultiSiteManufacturing.Host.Lattice;

/// <summary>
/// Polymorphic JSON codec for <see cref="Fact"/> envelopes. Used by
/// <see cref="LatticeFactBackend"/> to serialise facts into the
/// <c>byte[]</c> slots of the Orleans.Lattice tree.
/// </summary>
/// <remarks>
/// Using System.Text.Json with an explicit type-discriminator keeps the
/// sample's wire format readable when browsing the underlying Azure Table
/// via Azure Storage Explorer — inspecting a row shows a human-legible
/// <c>"$type": "InspectionRecorded"</c> alongside the payload.
/// </remarks>
public static class FactJsonCodec
{
    private static readonly JsonSerializerOptions Options = new()
    {
        WriteIndented = false,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        Converters =
        {
            new JsonStringEnumConverter(JsonNamingPolicy.CamelCase),
        },
    };

    /// <summary>Serialises a <see cref="Fact"/> to its byte[] Lattice value.</summary>
    public static byte[] Encode(Fact fact)
    {
        ArgumentNullException.ThrowIfNull(fact);
        return JsonSerializer.SerializeToUtf8Bytes<Fact>(fact, Options);
    }

    /// <summary>Deserialises a Lattice value back into a <see cref="Fact"/>.</summary>
    public static Fact Decode(byte[] payload)
    {
        ArgumentNullException.ThrowIfNull(payload);
        var fact = JsonSerializer.Deserialize<Fact>(payload, Options)
            ?? throw new InvalidOperationException("Decoded null fact.");
        return fact;
    }
}
