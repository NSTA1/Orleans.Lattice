using System.Globalization;
using System.Text;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Import;

/// <summary>
/// Generates the NDJSON files the bounded-memory proof imports, line by line through a
/// buffered writer, so the test process never holds a file either. Every line is padded to
/// <see cref="LineBytes"/>.
/// </summary>
internal static class ImportFiles
{
    /// <summary>The length of every generated line, newline included.</summary>
    public const int LineBytes = 4096;

    /// <summary>
    /// A file to seed a tree from: strictly ascending ids, a small <c>n</c> field the import
    /// stores (through the value pointer <c>/n</c>) and padding it reads and discards. The
    /// padding is what makes the file large while the tree it builds stays small, so a silo's
    /// growth measures what it streamed rather than what it stored.
    /// </summary>
    /// <param name="path">Where to write the file.</param>
    /// <param name="bytes">Its approximate size.</param>
    /// <returns>How many lines were written.</returns>
    public static int WriteSeed(string path, long bytes) =>
        Write(path, bytes, line => $"{{\"id\":\"{line:D8}\",\"n\":{line},\"pad\":\"");

    /// <summary>
    /// A file to upsert: every line is the whole record written, and its id cycles over
    /// <paramref name="keys"/> keys, so the full file streams through every hop to the silos
    /// while the tree keeps only the latest record per key.
    /// </summary>
    /// <param name="path">Where to write the file.</param>
    /// <param name="bytes">Its approximate size.</param>
    /// <param name="keys">How many distinct keys the lines cycle over.</param>
    /// <returns>How many lines were written.</returns>
    public static int WriteUpsert(string path, long bytes, int keys) =>
        Write(path, bytes, line => $"{{\"id\":\"{line % keys:D6}\",\"seq\":{line},\"pad\":\"");

    private static int Write(string path, long bytes, Func<int, string> head)
    {
        var lines = (int)(bytes / LineBytes);
        var pad = new string('x', LineBytes);
        using var writer = new StreamWriter(path, append: false, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false), bufferSize: 1 << 16);
        writer.NewLine = "\n";
        for (var line = 0; line < lines; line++)
        {
            var start = head(line);
            const string Tail = "\"}";
            writer.Write(start);
            writer.Write(pad.AsSpan(0, LineBytes - start.Length - Tail.Length - 1));
            writer.Write(Tail);
            writer.Write('\n');
        }

        return lines;
    }

    /// <summary>The count <paramref name="value"/> formatted as the Import dialog shows it.</summary>
    /// <param name="value">The count.</param>
    /// <returns>The text, such as <c>51,200</c>.</returns>
    public static string Count(long value) => value.ToString("N0", CultureInfo.InvariantCulture);
}
