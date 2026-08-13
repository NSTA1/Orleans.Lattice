using System.Text;
using NSubstitute;
using Orleans.Runtime;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Unit tests pinning that <see cref="RepoContextPortability.EnumerateAsync"/>
/// reads its page through the resilient <c>ScanEntriesAsync</c> wrapper rather
/// than a raw cursor, so a transient <see cref="EnumerationAbortedException"/>
/// (a remote enumerator reclaimed mid-scan by silo failover, cold start, idle
/// expiry, or scale-down) is transparently recovered instead of truncating a
/// snapshot page. The tree is a substitute whose <c>EntriesAsync</c> is scripted
/// to abort on demand, so the resilience is proven without a live cluster.
/// </summary>
[TestFixture]
public sealed class RepoContextPortabilityResilienceTests
{
    private const string Prefix = "repo/acme/file/";

    [Test]
    public async Task EnumerateAsync_recovers_from_a_mid_page_enumerator_abort()
    {
        var tree = Substitute.For<ILattice>();
        var callIndex = 0;
        var starts = new List<string?>();
        tree.EntriesAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                starts.Add(ci.ArgAt<string?>(0));
                var idx = callIndex++;
                return idx == 0
                    ? ScriptedEntries(new[] { (Prefix + "a", "1"), (Prefix + "b", "2") }, abortAfter: 2)
                    : ScriptedEntries(new[] { (Prefix + "c", "3") }, abortAfter: int.MaxValue);
            });

        var page = await RepoContextPortability.EnumerateAsync(
            tree, Prefix, continuationToken: null, pageSize: 10, vectorExport: null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(page.Records.Select(r => r.Key),
                Is.EqualTo(new[] { Prefix + "a", Prefix + "b", Prefix + "c" }),
                "the page must contain every record across the abort with no gap or duplicate");
            Assert.That(page.Records.Select(r => Encoding.UTF8.GetString(r.Value)),
                Is.EqualTo(new[] { "1", "2", "3" }));
            Assert.That(page.HasMore, Is.False);
            Assert.That(page.ContinuationToken, Is.Null);
            Assert.That(callIndex, Is.EqualTo(2), "the aborted scan must be reopened exactly once");
            Assert.That(starts[1], Is.EqualTo(Prefix + "b\u0000"),
                "the reopen must resume at the successor of the last yielded key");
        });
    }

    [Test]
    public async Task EnumerateAsync_reports_has_more_and_continuation_at_the_page_boundary()
    {
        var tree = Substitute.For<ILattice>();
        tree.EntriesAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(_ => ScriptedEntries(
                new[] { (Prefix + "a", "1"), (Prefix + "b", "2"), (Prefix + "c", "3") }, abortAfter: int.MaxValue));

        var page = await RepoContextPortability.EnumerateAsync(
            tree, Prefix, continuationToken: null, pageSize: 2, vectorExport: null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(page.Records.Select(r => r.Key), Is.EqualTo(new[] { Prefix + "a", Prefix + "b" }),
                "the page is capped at pageSize records");
            Assert.That(page.HasMore, Is.True, "a further entry beyond the page bound sets has-more");
            Assert.That(page.ContinuationToken, Is.EqualTo(Prefix + "b"),
                "the continuation token is the last key on the returned page");
        });
    }

    [Test]
    public async Task EnumerateAsync_resolves_a_vector_payload_per_record()
    {
        var tree = Substitute.For<ILattice>();
        tree.EntriesAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(_ => ScriptedEntries(new[] { (Prefix + "a", "1") }, abortAfter: int.MaxValue));

        var seen = new List<string>();
        RepoContextVectorPayload? Export(string key, CancellationToken _)
        {
            seen.Add(key);
            return new RepoContextVectorPayload(new byte[] { 7, 8 }, "space-x");
        }

        var page = await RepoContextPortability.EnumerateAsync(
            tree, Prefix, continuationToken: null, pageSize: 10,
            vectorExport: (key, ct) => new ValueTask<RepoContextVectorPayload?>(Export(key, ct)),
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(seen, Is.EqualTo(new[] { Prefix + "a" }), "the export seam is called once per record");
            Assert.That(page.Records, Has.Count.EqualTo(1));
            Assert.That(page.Records[0].Vector, Is.EqualTo(new byte[] { 7, 8 }));
            Assert.That(page.Records[0].EmbeddingSpace, Is.EqualTo("space-x"));
        });
    }

    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> ScriptedEntries(
        (string Key, string Value)[] entries, int abortAfter)
    {
        var yielded = 0;
        foreach (var (k, v) in entries)
        {
            if (yielded >= abortAfter) throw new EnumerationAbortedException();
            yielded++;
            yield return new KeyValuePair<string, byte[]>(k, Encoding.UTF8.GetBytes(v));
            await Task.Yield();
        }

        if (yielded < abortAfter) yield break;
        throw new EnumerationAbortedException();
    }
}
