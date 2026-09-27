using System.Text.Json;
using System.Text.Json.Serialization;

namespace Orleans.Lattice.Apps;

internal sealed class AppSlugJsonConverter : JsonConverter<AppSlug>
{
    public override AppSlug Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
        reader.TokenType == JsonTokenType.String && AppSlug.TryParse(reader.GetString(), out var slug)
            ? slug : throw new JsonException("Expected a valid app slug string.");

    public override void Write(Utf8JsonWriter writer, AppSlug value, JsonSerializerOptions options) =>
        writer.WriteStringValue(value.Value);
}
