using System.Text.Json;
using System.Text.Json.Serialization;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps;

/// <summary>Strict JSON parsing followed by semantic validation, without activating app code.</summary>
public static class AppManifestParser
{
    private static readonly JsonSerializerOptions Options = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow,
        AllowDuplicateProperties = false,
        Converters =
        {
            new AppSlugJsonConverter(),
            new AppVersionJsonConverter(),
            new AppOperationsJsonConverter(),
            new AppEnumJsonConverter<LatticeScopeKind>(),
            new AppEnumJsonConverter<LatticeMergeMode>(),
        },
    };

    /// <summary>
    /// Parses JSON text; null, malformed content and invalid declarations return diagnostics.
    /// Text longer than the manifest size bound is refused before it is deserialized.
    /// </summary>
    public static AppManifestResult Parse(string? json)
    {
        if (json is null)
            return AppManifestResult.Failure("required", "$", "Manifest JSON is required.");
        if (json.Length > AppManifestLimits.MaxManifestChars)
            return TooLarge();
        try
        {
            return AppManifestValidator.Validate(JsonSerializer.Deserialize<AppManifest>(json, Options));
        }
        catch (JsonException exception)
        {
            return AppManifestResult.Failure("json", exception.Path ?? "$", exception.Message);
        }
    }

    /// <summary>
    /// Parses UTF-8 JSON without closing the caller-owned stream. Invalid content or I/O faults
    /// return diagnostics; a null or unreadable stream is a programming error. At most the
    /// manifest size bound is read: a longer stream is refused without buffering the rest.
    /// </summary>
    public static AppManifestResult Parse(Stream stream)
    {
        ArgumentNullException.ThrowIfNull(stream);
        try
        {
            if (!TryReadBounded(stream, out var utf8))
                return TooLarge();
            return AppManifestValidator.Validate(JsonSerializer.Deserialize<AppManifest>(utf8.Span, Options));
        }
        catch (JsonException exception)
        {
            return AppManifestResult.Failure("json", exception.Path ?? "$", exception.Message);
        }
        catch (IOException exception)
        {
            return AppManifestResult.Failure("io", "$", exception.Message);
        }
    }

    private static AppManifestResult TooLarge() =>
        AppManifestResult.Failure("too-large", "$", $"A manifest may be at most {AppManifestLimits.MaxManifestChars} characters.");

    private static bool TryReadBounded(Stream stream, out ReadOnlyMemory<byte> utf8)
    {
        // Grows with the content, and stops one chunk past the bound: enough to know the
        // stream is oversized without buffering the rest of it.
        var content = new MemoryStream();
        var chunk = new byte[16 * 1024];
        int read;
        while ((read = stream.Read(chunk, 0, chunk.Length)) > 0)
        {
            content.Write(chunk, 0, read);
            if (content.Length > AppManifestLimits.MaxManifestChars)
            {
                utf8 = default;
                return false;
            }
        }

        utf8 = content.GetBuffer().AsMemory(0, (int)content.Length);
        if (utf8.Span.StartsWith((ReadOnlySpan<byte>)[0xEF, 0xBB, 0xBF]))
            utf8 = utf8[3..];
        return true;
    }
}
