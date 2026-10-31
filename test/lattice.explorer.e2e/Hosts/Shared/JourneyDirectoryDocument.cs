using System.Text.Json;
using System.Text.Json.Serialization;

namespace Orleans.Lattice.Explorer.EndToEnd.HostContract;

/// <summary>
/// The identity directory the harness supplies to the silo, as a JSON file: the users
/// who may sign in (with their passwords) and the groups the directory publishes. It is
/// the journey estate's stand-in for a real directory such as Entra ID: the silo
/// validates a Basic credential against it and searches it for the subject pickers.
/// Group <em>membership</em> is not here; it lives in the real membership grains.
/// </summary>
internal sealed class JourneyDirectoryDocument
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web) { WriteIndented = true };

    /// <summary>The users, keyed by subject id.</summary>
    [JsonPropertyName("users")]
    public List<JourneyDirectoryUser> Users { get; init; } = [];

    /// <summary>The groups the directory publishes.</summary>
    [JsonPropertyName("groups")]
    public List<JourneyDirectoryGroup> Groups { get; init; } = [];

    /// <summary>Reads a document from <paramref name="path"/>.</summary>
    /// <param name="path">The file.</param>
    public static JourneyDirectoryDocument Read(string path) =>
        JsonSerializer.Deserialize<JourneyDirectoryDocument>(File.ReadAllText(path), Json)
            ?? throw new InvalidOperationException($"The journey directory '{path}' is empty.");

    /// <summary>Writes the document to <paramref name="path"/>, replacing it atomically.</summary>
    /// <param name="path">The file.</param>
    public void Write(string path)
    {
        var temporary = path + ".tmp";
        File.WriteAllText(temporary, JsonSerializer.Serialize(this, Json));
        File.Move(temporary, path, overwrite: true);
    }
}

/// <summary>A user in a <see cref="JourneyDirectoryDocument"/>.</summary>
/// <param name="Id">The subject id the user signs in with.</param>
/// <param name="DisplayName">The display name the directory publishes.</param>
/// <param name="Password">The password the silo checks the credential against.</param>
internal sealed record JourneyDirectoryUser(
    [property: JsonPropertyName("id")] string Id,
    [property: JsonPropertyName("displayName")] string DisplayName,
    [property: JsonPropertyName("password")] string Password);

/// <summary>A group in a <see cref="JourneyDirectoryDocument"/>.</summary>
/// <param name="Id">The group id.</param>
/// <param name="DisplayName">The display name the directory publishes.</param>
internal sealed record JourneyDirectoryGroup(
    [property: JsonPropertyName("id")] string Id,
    [property: JsonPropertyName("displayName")] string DisplayName);
