using System.Text.Json;

namespace Orleans.Lattice.Explorer.UI.Downloads;

/// <summary>
/// The record every Explorer download ends with: what the file is, how many items it
/// holds, whether it is complete, whether access withheld anything, and why not when
/// it is not complete. A file that ends without one was cut off in transit.
/// </summary>
/// <remarks>
/// <para>
/// A streamed download has sent its status code before its first byte, so a failure
/// part-way through can be reported only inside the file. The trailer is how a reader
/// tells a complete file from a partial one; it is never optional and it is always last.
/// </para>
/// <para>
/// Its JSON form is the object <c>{"format":...,"items":...,"complete":...,"refused":...,"reason":...}</c>,
/// with <c>refused</c> one of <c>none</c>, <c>some</c>, <c>all</c> or <c>unknown</c> and
/// <c>reason</c> <see langword="null"/> exactly when the file is complete. Each download kind
/// embeds that object in its own format; see <see cref="WriteJson"/>.
/// </para>
/// </remarks>
/// <param name="Format">The file's format and version, such as <c>lattice-export/1</c>.</param>
/// <param name="Items">How many items the file holds before the trailer.</param>
/// <param name="Complete">Whether the file holds everything that was asked for.</param>
/// <param name="Refused">Whether the caller's access withheld anything.</param>
/// <param name="Reason">Why the file is not complete, in plain words, or <see langword="null"/> when it is.</param>
internal sealed record ExplorerDownloadTrailer(
    string Format,
    long Items,
    bool Complete,
    ExplorerDownloadRefusal Refused,
    string? Reason)
{
    /// <summary>The JSON property names, which an importer reads back.</summary>
    internal static class Names
    {
        /// <summary>The format property.</summary>
        public const string Format = "format";

        /// <summary>The item-count property.</summary>
        public const string Items = "items";

        /// <summary>The completeness property.</summary>
        public const string Complete = "complete";

        /// <summary>The refusal property.</summary>
        public const string Refused = "refused";

        /// <summary>The reason property.</summary>
        public const string Reason = "reason";
    }

    /// <summary>The trailer's <see cref="Refused"/> value as it is written.</summary>
    public string RefusedText => Refused switch
    {
        ExplorerDownloadRefusal.None => "none",
        ExplorerDownloadRefusal.Some => "some",
        ExplorerDownloadRefusal.All => "all",
        _ => "unknown",
    };

    /// <summary>A trailer for a download that stopped before it finished.</summary>
    /// <param name="format">The file's format.</param>
    /// <param name="items">How many items were written before it stopped.</param>
    /// <param name="reason">Why it stopped.</param>
    public static ExplorerDownloadTrailer Stopped(string format, long items, string reason) =>
        new(format, items, Complete: false, ExplorerDownloadRefusal.Unknown, reason);

    /// <summary>Writes the trailer as a JSON object at the writer's current position.</summary>
    /// <param name="writer">The JSON writer.</param>
    public void WriteJson(Utf8JsonWriter writer)
    {
        ArgumentNullException.ThrowIfNull(writer);
        writer.WriteStartObject();
        writer.WriteString(Names.Format, Format);
        writer.WriteNumber(Names.Items, Items);
        writer.WriteBoolean(Names.Complete, Complete);
        writer.WriteString(Names.Refused, RefusedText);
        if (Reason is null)
        {
            writer.WriteNull(Names.Reason);
        }
        else
        {
            writer.WriteString(Names.Reason, Reason);
        }

        writer.WriteEndObject();
    }
}
