using System.Text.Json;
using System.Text.Json.Serialization;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;

/// <summary>
/// A journey's screenshot manifest, <c>manifest.json</c> beside its screenshots: the ordered
/// checkpoints with each file's SHA-256, and the provenance - commit and CI run - that the
/// PR's screenshot section is checked against.
/// </summary>
public sealed class JourneyManifest
{
    /// <summary>The manifest's file name.</summary>
    public const string FileName = "manifest.json";

    private static readonly JsonSerializerOptions Json = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    /// <summary>The journey id.</summary>
    public required string Journey { get; init; }

    /// <summary>The commit the run built, or <see langword="null"/> when it cannot be told.</summary>
    public string? Commit { get; init; }

    /// <summary>The CI run id, or <see langword="null"/> outside CI.</summary>
    public string? RunId { get; init; }

    /// <summary>The CI run attempt, or <see langword="null"/> outside CI.</summary>
    public string? RunAttempt { get; init; }

    /// <summary>The viewport every screenshot was taken at: <c>desktop</c> or <c>narrow</c>.</summary>
    public required string Viewport { get; init; }

    /// <summary>The viewport's CSS width.</summary>
    public int Width { get; init; }

    /// <summary>The viewport's CSS height.</summary>
    public int Height { get; init; }

    /// <summary>The checkpoints, in order.</summary>
    public required IReadOnlyList<JourneyManifestEntry> Entries { get; init; }

    /// <summary>Reads a manifest written by <see cref="Write"/>.</summary>
    /// <param name="path">The file.</param>
    public static JourneyManifest Read(string path) =>
        JsonSerializer.Deserialize<JourneyManifest>(File.ReadAllText(path), Json)
            ?? throw new InvalidDataException($"'{path}' holds no manifest.");

    /// <summary>Writes the manifest to <paramref name="path"/>, replacing it whole so a reader never sees half of it.</summary>
    /// <param name="path">The file.</param>
    public void Write(string path)
    {
        var temporary = path + ".tmp";
        File.WriteAllText(temporary, JsonSerializer.Serialize(this, Json));
        File.Move(temporary, path, overwrite: true);
    }

    /// <summary>The manifest as markdown: one captioned row per checkpoint, in order.</summary>
    public string ToMarkdown()
    {
        var text = new System.Text.StringBuilder()
            .Append("# Journey `").Append(Journey).AppendLine("`")
            .AppendLine()
            .Append("Commit `").Append(Commit ?? "unknown").Append("`, run `").Append(RunId ?? "local").Append("`, ")
            .Append(Viewport).Append(' ').Append(Width).Append('x').Append(Height).AppendLine(".")
            .AppendLine()
            .AppendLine("| # | Step | Substep | Persona | Caption | File | SHA-256 | Checks |")
            .AppendLine("|---|---|---|---|---|---|---|---|");
        foreach (var entry in Entries)
        {
            text.Append("| ").Append(entry.Index)
                .Append(" | ").Append(entry.Step)
                .Append(" | ").Append(entry.Substep)
                .Append(" | ").Append(entry.Persona ?? "-")
                .Append(" | ").Append(entry.Title.Replace("|", "\\|", StringComparison.Ordinal))
                .Append(" | `").Append(entry.File)
                .Append("` | `").Append(entry.Sha256[..12])
                .Append("` | ").Append(entry.Passed ? "passed" : "failed")
                .AppendLine(" |");
        }

        return text.ToString();
    }

    /// <summary>The manifest as a self-contained HTML page: the rendering a reviewer reads beside the screenshots.</summary>
    public string ToHtml()
    {
        static string Encode(string? value) => System.Net.WebUtility.HtmlEncode(value ?? "-");

        var html = new System.Text.StringBuilder()
            .Append("<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><title>Journey ").Append(Encode(Journey)).Append("</title>")
            .Append("<style>body{font-family:sans-serif;margin:24px;color:#1a1a1a}table{border-collapse:collapse;width:100%}")
            .Append("th,td{border:1px solid #c8c8c8;padding:6px 10px;text-align:left;vertical-align:top}code{font-family:monospace}")
            .Append(".failed{color:#a4000f;font-weight:bold}</style></head><body><main>")
            .Append("<h1>Journey <code>").Append(Encode(Journey)).Append("</code></h1>")
            .Append("<p>Commit <code>").Append(Encode(Commit)).Append("</code>, run <code>").Append(Encode(RunId ?? "local")).Append("</code>, ")
            .Append(Encode(Viewport)).Append(' ').Append(Width).Append('x').Append(Height).Append(", ").Append(Entries.Count).Append(" checkpoints.</p>")
            .Append("<table><thead><tr><th>#</th><th>Step</th><th>Substep</th><th>Persona</th><th>Caption</th><th>File</th><th>SHA-256</th><th>Checks</th></tr></thead><tbody>");
        foreach (var entry in Entries)
        {
            html.Append("<tr><td>").Append(entry.Index)
                .Append("</td><td><code>").Append(Encode(entry.Step))
                .Append("</code></td><td><code>").Append(Encode(entry.Substep))
                .Append("</code></td><td>").Append(Encode(entry.Persona))
                .Append("</td><td>").Append(Encode(entry.Title))
                .Append("</td><td><code>").Append(Encode(entry.File))
                .Append("</code></td><td><code>").Append(Encode(entry.Sha256[..16])).Append("...</code>")
                .Append("</td><td").Append(entry.Passed ? ">passed" : " class=\"failed\">failed")
                .Append("</td></tr>");
        }

        return html.Append("</tbody></table></main></body></html>").ToString();
    }
}
