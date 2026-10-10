using System.Globalization;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Unicode;
using Orleans.Lattice.Explorer.Core.Data;
using Orleans.Lattice.Explorer.UI.Design.Components;

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
                return new DataRenderedValue("UTF-8 text", Decode(bytes, truncated), Join(preview, "Bytes that are not valid UTF-8 are shown as a replacement character."));

            case DataValueRenderer.Json:
                return TryIndent(bytes, out var json)
                    ? new DataRenderedValue("JSON", json, preview)
                    : new DataRenderedValue("UTF-8 text", Decode(bytes, truncated), Join(preview, "This value is not valid JSON, so it is shown as text."));

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
    /// <returns>
    /// The preview, ending in <c>...</c> whenever the value continues beyond it: when the
    /// text is longer than <paramref name="maximum"/>, when a hex preview leaves bytes out,
    /// or when the bytes are themselves only a preview.
    /// </returns>
    public static string Inline(byte[] bytes, bool truncated, int maximum = 160)
    {
        ArgumentNullException.ThrowIfNull(bytes);
        var rendered = ValueRenderer.Render(bytes, truncated);
        var hexBytes = Math.Min(bytes.Length, maximum / 2);
        var text = rendered.Format == ValueFormat.Json
            ? Compact(bytes) ?? rendered.Content
            : rendered.Format == ValueFormat.Hex
                ? Convert.ToHexString(bytes.AsSpan(0, hexBytes)).ToLowerInvariant()
                : rendered.Content;
        text = text.ReplaceLineEndings(" ");

        // Two hex digits per byte fill the cell exactly, so the length check alone
        // never marks the bytes a hex preview leaves out (#4354).
        var continues = truncated || (rendered.Format == ValueFormat.Hex && hexBytes < bytes.Length);
        return text.Length > maximum || continues
            ? string.Concat(LtTextCut.Prefix(text, maximum - 3), "...")
            : text;
    }

    /// <summary>A byte count for a person: <c>512 B</c>, <c>12.4 KiB</c>.</summary>
    /// <param name="bytes">The count.</param>
    /// <returns>
    /// The count in the largest unit whose figure, as written, is below 1024, so a size
    /// just under a boundary reads <c>1 MiB</c> rather than <c>1024 KiB</c>.
    /// </returns>
    public static string Size(long bytes)
    {
        if (bytes < 1024)
        {
            return string.Create(CultureInfo.InvariantCulture, $"{bytes:N0} B");
        }

        var kib = bytes / 1024d;
        return Math.Round(kib, 1, MidpointRounding.AwayFromZero) < 1024
            ? string.Create(CultureInfo.InvariantCulture, $"{kib:0.#} KiB")
            : string.Create(CultureInfo.InvariantCulture, $"{bytes / (1024d * 1024d):0.#} MiB");
    }

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

    /// <summary>
    /// Decodes bytes as UTF-8, invalid sequences as replacement characters. A preview
    /// cut inside a character drops the partial character rather than showing it as
    /// one, since those bytes were not invalid, only not fetched.
    /// </summary>
    /// <param name="bytes">The bytes.</param>
    /// <param name="truncated">Whether the bytes are a prefix of a longer value.</param>
    /// <returns>The text.</returns>
    internal static string Decode(byte[] bytes, bool truncated)
    {
        if (!truncated)
        {
            return Encoding.UTF8.GetString(bytes);
        }

        var chars = new char[bytes.Length];
        Utf8.ToUtf16(bytes, chars, out _, out var written, replaceInvalidSequences: true, isFinalBlock: false);
        return new string(chars, 0, written);
    }

    private static string? Join(string? first, string second) => first is null ? second : first + " " + second;
}
