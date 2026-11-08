using Azure;
using Azure.Data.Tables;
using NSubstitute;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// Issue #6017: the retained-byte manifest scan is cancelled when the usage
/// fan-out budget expires. A scan stopped part-way must throw, so the usage
/// aggregator reports the partition as unavailable, rather than return the
/// part it had summed - which would read as a smaller, wrong retained figure.
/// </summary>
[TestFixture]
public sealed class AzureTableWalStorageProviderRetainedScanTests
{
    [Test]
    public async Task SumManifestPayloadBytesAsync_sums_every_manifest_row()
    {
        var table = TableReturning(new QuietlyStoppingPageable(cancelBeforeSecondPage: null));

        var total = await AzureTableWalStorageProvider.SumManifestPayloadBytesAsync(table, "filter", CancellationToken.None);

        Assert.That(total, Is.EqualTo(600L));
    }

    [Test]
    public void SumManifestPayloadBytesAsync_cancelled_between_pages_throws_rather_than_returning_a_partial_sum()
    {
        using var cancellation = new CancellationTokenSource();
        // A pageable that, once cancelled, ends quietly instead of throwing: the
        // shape that would otherwise hand back the first page's sum alone.
        var table = TableReturning(new QuietlyStoppingPageable(cancelBeforeSecondPage: cancellation));

        Assert.That(
            async () => await AzureTableWalStorageProvider.SumManifestPayloadBytesAsync(table, "filter", cancellation.Token),
            Throws.InstanceOf<OperationCanceledException>(),
            "a scan cancelled part-way must never surface the part it summed as the retained figure");
    }

    private static TableClient TableReturning(AsyncPageable<AzureTableWalEntity> pageable)
    {
        var table = Substitute.For<TableClient>();
        table.QueryAsync<AzureTableWalEntity>(
                Arg.Any<string>(), Arg.Any<int?>(), Arg.Any<IEnumerable<string>>(), Arg.Any<CancellationToken>())
            .Returns(pageable);
        return table;
    }

    private static AzureTableWalEntity Row(long payloadBytes) => new() { PayloadBytes = payloadBytes };

    /// <summary>
    /// Two manifest pages (100 + 200, then 300 bytes). When given a source, it
    /// cancels it after the first page and then ends without yielding the second.
    /// </summary>
    private sealed class QuietlyStoppingPageable(CancellationTokenSource? cancelBeforeSecondPage) : AsyncPageable<AzureTableWalEntity>
    {
        public override async IAsyncEnumerable<Page<AzureTableWalEntity>> AsPages(string? continuationToken = null, int? pageSizeHint = null)
        {
            yield return Page<AzureTableWalEntity>.FromValues([Row(100), Row(200)], "next", Substitute.For<Response>());
            await Task.Yield();
            if (cancelBeforeSecondPage is { } source)
            {
                source.Cancel();
                yield break;
            }

            yield return Page<AzureTableWalEntity>.FromValues([Row(300)], null, Substitute.For<Response>());
        }
    }
}
