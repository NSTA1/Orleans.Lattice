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

    /// <summary>Parses JSON text; null, malformed content and invalid declarations return diagnostics.</summary>
    public static AppManifestResult Parse(string? json)
    {
        if (json is null)
            return AppManifestResult.Failure("required", "$", "Manifest JSON is required.");
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
    /// return diagnostics; a null or unreadable stream is a programming error.
    /// </summary>
    public static AppManifestResult Parse(Stream stream)
    {
        ArgumentNullException.ThrowIfNull(stream);
        try
        {
            return AppManifestValidator.Validate(JsonSerializer.Deserialize<AppManifest>(stream, Options));
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
}
