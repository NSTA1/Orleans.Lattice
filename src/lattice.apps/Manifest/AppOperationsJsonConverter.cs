using System.Text.Json;
using System.Text.Json.Serialization;

namespace Orleans.Lattice.Apps;

internal sealed class AppOperationsJsonConverter : JsonConverter<LatticeOperation>
{
    public override LatticeOperation Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType != JsonTokenType.StartArray)
            throw new JsonException("Operations must be an array of operation names.");
        var mask = LatticeOperation.None;
        while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
        {
            if (reader.TokenType != JsonTokenType.String ||
                !Enum.TryParse<LatticeOperation>(reader.GetString(), out var operation) ||
                Enum.GetName(operation) != reader.GetString() ||
                operation == LatticeOperation.None ||
                ((int)operation & ((int)operation - 1)) != 0 ||
                (operation & ~AppManifestValidator.RoleOperations) != 0 ||
                (mask & operation) != 0)
                throw new JsonException("Expected unique, known, single tree-scoped operation names.");
            mask |= operation;
        }
        if (reader.TokenType != JsonTokenType.EndArray || mask == LatticeOperation.None)
            throw new JsonException("At least one tree-scoped operation is required.");
        return mask;
    }

    public override void Write(Utf8JsonWriter writer, LatticeOperation value, JsonSerializerOptions options)
    {
        if (value == LatticeOperation.None || (value & ~AppManifestValidator.RoleOperations) != 0)
            throw new JsonException("Invalid app role operation mask.");
        writer.WriteStartArray();
        foreach (var operation in Enum.GetValues<LatticeOperation>())
            if (operation != LatticeOperation.None && ((int)operation & ((int)operation - 1)) == 0 && (value & operation) != 0)
                writer.WriteStringValue(Enum.GetName(operation));
        writer.WriteEndArray();
    }
}
