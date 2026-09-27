using System.Text.Json;
using System.Text.Json.Serialization;

namespace Orleans.Lattice.Apps;

internal sealed class AppVersionJsonConverter : JsonConverter<AppVersion>
{
    public override AppVersion Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
        reader.TokenType == JsonTokenType.String && AppVersion.TryParse(reader.GetString(), out var version)
            ? version : throw new JsonException("Expected a Semantic Version 2.0 string.");

    public override void Write(Utf8JsonWriter writer, AppVersion value, JsonSerializerOptions options) =>
        writer.WriteStringValue(value.Value);
}
