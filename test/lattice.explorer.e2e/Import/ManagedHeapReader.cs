using System.Globalization;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Import;

/// <summary>
/// Reads a silo's live managed heap - the size of every generation after its last garbage
/// collection - from the <c>System.Runtime</c> metrics its <c>/metrics</c> endpoint exports.
/// Beside the working set, it separates memory a process still holds from heap the server
/// garbage collector has simply not returned, so a growth reading can be explained.
/// </summary>
internal sealed class ManagedHeapReader : IDisposable
{
    private const string Metric = "dotnet_gc_last_collection_heap_size";

    private readonly HttpClient _http = new() { Timeout = TimeSpan.FromSeconds(10) };

    /// <summary>The live managed heap of the silo serving <paramref name="httpPort"/>, or <see langword="null"/> when it reports none.</summary>
    /// <param name="httpPort">The silo's internal HTTP port.</param>
    /// <returns>The heap after the last collection, in bytes.</returns>
    public async Task<long?> ReadAsync(int httpPort)
    {
        string text;
        try
        {
            text = await _http.GetStringAsync(new Uri($"http://127.0.0.1:{httpPort}/metrics"));
        }
        catch (Exception exception) when (exception is HttpRequestException or TaskCanceledException)
        {
            return null;
        }

        long total = 0;
        var found = false;
        foreach (var line in text.Split('\n'))
        {
            if (!line.StartsWith(Metric, StringComparison.Ordinal))
            {
                continue;
            }

            var value = line[(line.LastIndexOf(' ') + 1)..].Trim();
            if (double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var bytes))
            {
                total += (long)bytes;
                found = true;
            }
        }

        return found ? total : null;
    }

    /// <inheritdoc />
    public void Dispose() => _http.Dispose();
}
