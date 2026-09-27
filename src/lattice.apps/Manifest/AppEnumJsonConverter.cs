using System.Text.Json;
using System.Text.Json.Serialization;

namespace Orleans.Lattice.Apps;

internal sealed class AppEnumJsonConverter<T> : JsonConverter<T> where T : struct, Enum
{
    public override T Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType == JsonTokenType.String &&
            Enum.TryParse<T>(reader.GetString(), out var value) &&
            Enum.GetName(value) == reader.GetString())
            return value;
        throw new JsonException($"Expected a named {typeof(T).Name} value.");
    }

    public override void Write(Utf8JsonWriter writer, T value, JsonSerializerOptions options) =>
        writer.WriteStringValue(Enum.GetName(value));
}
