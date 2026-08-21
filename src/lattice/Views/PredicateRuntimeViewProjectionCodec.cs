using Orleans.Serialization;

namespace Orleans.Lattice.Views;

/// <summary>Encodes the built-in filter-only predicate projection payload.</summary>
internal sealed class PredicateRuntimeViewProjectionCodec(Serializer<LatticePredicateNode> serializer)
{
    public const string ProviderKey = "orleans.lattice.predicate.v1";

    public byte[] Encode(LatticePredicateNode? filter)
    {
        if (filter is not { } node)
        {
            return [0];
        }

        var serialized = serializer.SerializeToArray(node);
        var payload = new byte[serialized.Length + 1];
        payload[0] = 1;
        serialized.CopyTo(payload.AsSpan(1));
        return payload;
    }

    public LatticePredicateNode? Decode(ReadOnlySpan<byte> payload)
    {
        if (payload.Length == 1 && payload[0] == 0)
        {
            return null;
        }

        if (payload.Length < 2 || payload[0] != 1)
        {
            throw new ArgumentException("The predicate projection payload is invalid.", nameof(payload));
        }

        return serializer.Deserialize(payload[1..].ToArray());
    }
}
