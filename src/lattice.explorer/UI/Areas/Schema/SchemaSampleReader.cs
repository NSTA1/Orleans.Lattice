using Orleans.Lattice.Explorer.Core.Data;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>
/// Reads a bounded sample of a tree's current values through the state API's
/// data reader: one page of the first keys in key order, whose cursor is released
/// straight after, plus a point read for each value too large for the page's
/// preview. Read-only; nothing is written and no scan is left pinned.
/// </summary>
/// <param name="reader">The data reader, or <see langword="null"/> when the head serves none.</param>
/// <param name="tenant">Reads the tenant the circuit asserts right now.</param>
internal sealed class SchemaSampleReader(IDataReader? reader, Func<string?> tenant)
{
    /// <summary>How many values a sample holds at most.</summary>
    public const int SampleSize = 100;

    /// <summary>How many full values are read at once for previews that were cut short.</summary>
    public const int FullReadConcurrency = 4;

    /// <summary>The reason given when no reader is available.</summary>
    public const string Unavailable = "This Explorer cannot read the tree's values, so there is no sample to learn the shape from or to check against.";

    /// <summary>Whether a sample can be read at all.</summary>
    public bool IsAvailable => reader is not null;

    /// <summary>Reads the sample of <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The tree.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The sample.</returns>
    /// <exception cref="InvalidOperationException">No reader is available.</exception>
    public async Task<SchemaSample> ReadAsync(string treeId, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        var data = reader ?? throw new InvalidOperationException(Unavailable);
        var asserted = tenant();
        var page = await data.ScanAsync(treeId, SampleSize, cancellationToken: cancellationToken).ConfigureAwait(false);
        _ = ReleaseAsync(data, treeId, page.ContinuationToken);

        var entries = page.Entries.Where(entry => !entry.IsTombstone).Take(SampleSize).ToArray();
        var values = new SchemaSampleValue?[entries.Length];
        var unread = 0;
        using var gate = new SemaphoreSlim(FullReadConcurrency);
        var reads = new List<Task>();
        for (var index = 0; index < entries.Length; index++)
        {
            var entry = entries[index];
            if (!entry.Truncated)
            {
                values[index] = new SchemaSampleValue(entry.Key, Body(entry.Value));
                continue;
            }

            var at = index;
            reads.Add(ReadFullAsync(data, gate, treeId, entry.Key, cancellationToken).ContinueWith(
                read =>
                {
                    if (read.IsCompletedSuccessfully && read.Result is { } full)
                    {
                        values[at] = new SchemaSampleValue(entry.Key, Body(full));
                    }
                    else
                    {
                        Interlocked.Increment(ref unread);
                    }
                },
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default));
        }

        await Task.WhenAll(reads).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        return new SchemaSample(
            treeId,
            asserted,
            [.. values.OfType<SchemaSampleValue>()],
            unread,
            page.ContinuationToken is not null || page.Entries.Count > SampleSize);
    }

    /// <summary>The body a policy judges: the value with any version envelope stripped.</summary>
    /// <param name="value">The stored bytes.</param>
    /// <returns>The body.</returns>
    internal static byte[] Body(byte[] value) =>
        LatticeSchemaEnvelope.IsEnveloped(value) ? LatticeSchemaEnvelope.StripToBody(value) : value;

    private static async Task<byte[]?> ReadFullAsync(IDataReader data, SemaphoreSlim gate, string treeId, string key, CancellationToken cancellationToken)
    {
        await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var entry = await data.GetEntryAsync(treeId, key, cancellationToken).ConfigureAwait(false);
            return entry is { IsTombstone: false, Truncated: false } ? entry.Value : null;
        }
        finally
        {
            gate.Release();
        }
    }

    private static async Task ReleaseAsync(IDataReader data, string treeId, string? token)
    {
        if (string.IsNullOrEmpty(token))
        {
            return;
        }

        try
        {
            await data.CancelScanAsync(treeId, token).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // Releasing a cursor is best effort; the server reaps an idle one anyway.
        }
    }
}
