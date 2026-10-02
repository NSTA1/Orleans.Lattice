using System.Globalization;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using Orleans.Lattice.Explorer.Core.Data;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// Draws a value's bytes with the renderer the user picked. Automatic rendering
/// is the Core <see cref="ValueRenderer"/>'s own choice; the others force one
/// reading, and say so when the bytes do not fit it. Everything returned is
/// plain text: a value is never interpreted as markup.
/// </summary>
internal static class DataValueRendering
{
    /// <summary>How many characters the value cell shows before asking to expand.</summary>
    public const int DisplayLimit = 4000;

    // Relaxed escaping keeps non-ASCII text and < > & ' + as written: everything
    // returned here is rendered as text, never as markup, so it is HTML-encoded anyway.
    private static readonly JsonSerializerOptions Indented = new()
    {
        WriteIndented = true,
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
    };

    private static readonly JsonSerializerOptions Compacted = new()
    {
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
    };

    /// <summary>The renderers offered for a value, members only when the state API decoded some.</summary>
    /// <param name="hasMembers">Whether CRDT members are available.</param>
    public static IReadOnlyList<DataValueRenderer> Offered(bool hasMembers) => hasMembers
        ? [DataValueRenderer.Members, DataValueRenderer.Auto, DataValueRenderer.Text, DataValueRenderer.Json, DataValueRenderer.Hex]
        : [DataValueRenderer.Auto, DataValueRenderer.Text, DataValueRenderer.Json, DataValueRenderer.Hex];

    /// <summary>The renderer's label.</summary>
    /// <param name="renderer">The renderer.</param>
    public static string Label(DataValueRenderer renderer) => renderer switch
    {
        DataValueRenderer.Text => "UTF-8 text",
        DataValueRenderer.Json => "JSON",
        DataValueRenderer.Hex => "Hex",
        DataValueRenderer.Members => "CRDT members",
        _ => "Automatic",
    };

    /// <summary>Renders <paramref name="bytes"/>.</summary>
    /// <param name="bytes">The value bytes (possibly a preview).</param>
    /// <param name="truncated">Whether the bytes are only a preview of a longer value.</param>
    /// <param name="renderer">The renderer to use.</param>
    /// <param name="members">The decoded CRDT members, for <see cref="DataValueRenderer.Members"/>.</param>
    public static DataRenderedValue Render(
        byte[] bytes,
        bool truncated,
        DataValueRenderer renderer,
        IReadOnlyList<DataCrdtMember>? members = null)
    {
        ArgumentNullException.ThrowIfNull(bytes);
        var preview = truncated ? "Preview only: the full value is longer than the bytes fetched." : null;
        switch (renderer)
        {
            case DataValueRenderer.Members when members is { Count: > 0 }:
                return new DataRenderedValue("CRDT members", RenderMembers(members), null);

            case DataValueRenderer.Text:
                return new DataRenderedValue("UTF-8 text", Encoding.UTF8.GetString(bytes), Join(preview, "Bytes that are not valid UTF-8 are shown as a replacement character."));

            case DataValueRenderer.Json:
                return TryIndent(bytes, out var json)
                    ? new DataRenderedValue("JSON", json, preview)
                    : new DataRenderedValue("UTF-8 text", Encoding.UTF8.GetString(bytes), Join(preview, "This value is not valid JSON, so it is shown as text."));

            case DataValueRenderer.Hex:
                return new DataRenderedValue("Hex", ValueRenderer.HexDump(bytes), preview);

            default:
                var automatic = ValueRenderer.Render(bytes, truncated);
                return new DataRenderedValue(FormatText(automatic.Format), automatic.Content, automatic.Note);
        }
    }

    /// <summary>A one-line preview of a value for a table cell.</summary>
    /// <param name="bytes">The value bytes.</param>
    /// <param name="truncated">Whether the bytes are a preview.</param>
    /// <param name="maximum">The most characters to return.</param>
    public static string Inline(byte[] bytes, bool truncated, int maximum = 160)
    {
        ArgumentNullException.ThrowIfNull(bytes);
        var rendered = ValueRenderer.Render(bytes, truncated);
        var text = rendered.Format == ValueFormat.Json
            ? Compact(bytes) ?? rendered.Content
            : rendered.Format == ValueFormat.Hex
                ? Convert.ToHexString(bytes.AsSpan(0, Math.Min(bytes.Length, maximum / 2))).ToLowerInvariant()
                : rendered.Content;
        text = text.ReplaceLineEndings(" ");
        return text.Length > maximum ? string.Concat(text.AsSpan(0, maximum - 3), "...") : text;
    }

    /// <summary>A byte count for a person: <c>512 B</c>, <c>12.4 KiB</c>.</summary>
    /// <param name="bytes">The count.</param>
    public static string Size(long bytes) => bytes switch
    {
        < 1024 => string.Create(CultureInfo.InvariantCulture, $"{bytes:N0} B"),
        < 1024 * 1024 => string.Create(CultureInfo.InvariantCulture, $"{bytes / 1024d:0.#} KiB"),
        _ => string.Create(CultureInfo.InvariantCulture, $"{bytes / (1024d * 1024d):0.#} MiB"),
    };

    private static string FormatText(ValueFormat format) => format switch
    {
        ValueFormat.Json => "JSON",
        ValueFormat.Text => "UTF-8 text",
        ValueFormat.Hex => "Hex",
        _ => "Empty",
    };

    private static string RenderMembers(IReadOnlyList<DataCrdtMember> members)
    {
        var builder = new StringBuilder();
        foreach (var member in members)
        {
            builder.Append(member.ElementText.ReplaceLineEndings(" "));
            if (member.ReplicaId.Length > 0)
            {
                builder.Append("  (replica ").Append(member.ReplicaId)
                    .Append(", #").Append(member.Ordinal.ToString(CultureInfo.InvariantCulture)).Append(')');
            }

            builder.Append('\n');
        }

        return builder.ToString();
    }

    private static bool TryIndent(byte[] bytes, out string json)
    {
        try
        {
            using var document = JsonDocument.Parse(bytes);
            json = JsonSerializer.Serialize(document.RootElement, Indented);
            return true;
        }
        catch (JsonException)
        {
            json = string.Empty;
            return false;
        }
    }

    private static string? Compact(byte[] bytes)
    {
        try
        {
            using var document = JsonDocument.Parse(bytes);
            return JsonSerializer.Serialize(document.RootElement, Compacted);
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private static string? Join(string? first, string second) => first is null ? second : first + " " + second;
}
