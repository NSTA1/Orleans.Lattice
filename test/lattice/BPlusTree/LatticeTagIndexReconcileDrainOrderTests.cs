using System.Runtime.CompilerServices;
using NSubstitute;
using Orleans.Lattice;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Deterministic regression coverage for issue 1351: the tag-index reconcile
/// must not mutate the index tree while it is still streaming a scan over that
/// same tree. Deleting a membership row mid-scan restructures the tree being
/// enumerated, so the paginated / abort-resumed cursor can skip a contiguous
/// tail of rows and strand them (the flaky
/// <c>Restore_reconciles_large_tag_membership_under_concurrent_reads</c>
/// symptom). <see cref="LatticeTagIndexContext.ReconcileSubjectAsync"/> now
/// drains the scan into a buffer before issuing any delete; this pins that
/// drain-then-delete ordering without depending on timing or a live cluster.
/// </summary>
[TestFixture]
public sealed class LatticeTagIndexReconcileDrainOrderTests
{
    private const char Sep = '\0';

    // Posting row: `tag \0 treeId \0 key`.
    private static string PostingRow(string tag, string treeId, string key) =>
        string.Concat(tag, Sep.ToString(), treeId, Sep.ToString(), key);

    // Key-major mirror row: `\0k \0 treeId \0 key \0 tag`.
    private static string MirrorRow(string tag, string treeId, string key) =>
        string.Concat("\0k\0", treeId, Sep.ToString(), key, Sep.ToString(), tag);

    [Test]
    public async Task ReconcileSubjectAsync_defers_every_index_delete_until_the_scan_has_drained()
    {
        const string subjectTreeId = "subj";
        const string tag = "red";

        var kept = Enumerable.Range(0, 20).Select(i => $"keep-{i:D2}").ToArray();
        var gone = Enumerable.Range(0, 30).Select(i => $"gone-{i:D3}").ToArray();

        var index = new InstrumentedIndexTree();
        foreach (var k in kept.Concat(gone))
        {
            index.Data[PostingRow(tag, subjectTreeId, k)] = [1];
            index.Data[MirrorRow(tag, subjectTreeId, k)] = [1];
        }

        // The restored subject retains only the kept keys; every 'gone' posting
        // row is therefore an orphan the reconcile must drop.
        var keptSet = new HashSet<string>(kept, StringComparer.Ordinal);
        var subject = Substitute.For<ILattice>();
        subject.KeysAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(ci => LiveKeys(kept, ci.ArgAt<string?>(0), ci.ArgAt<string?>(1), ci.ArgAt<CancellationToken>(4)));
        subject.ExistsAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult(keptSet.Contains(ci.ArgAt<string>(0))));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(Arg.Any<string>(), Arg.Any<string?>())
            .Returns(ci => string.Equals(ci.ArgAt<string>(0), "tag-idx", StringComparison.Ordinal)
                ? index.Lattice
                : subject);

        var ctx = LatticeTagIndexContext.CreateForCoordinator(grainFactory, "idx");

        var report = await ctx.ReconcileSubjectAsync(subjectTreeId, null, null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            // The fix's core invariant: no write landed on the index tree while
            // its scan enumerator was still open.
            Assert.That(index.DeleteDuringScanObserved, Is.False,
                "The reconcile issued an index-tree delete while the scan enumerator was still open; "
                + "mutating the tree mid-scan is the issue-1351 race that strands a contiguous tail of rows.");

            // Every orphan was dropped - none skipped.
            Assert.That(report.OrphanRowsRemoved, Is.EqualTo(gone.Length));
            foreach (var k in gone)
            {
                Assert.That(index.Deleted, Does.Contain(PostingRow(tag, subjectTreeId, k)),
                    $"Orphan posting row for '{k}' must be dropped.");
            }

            // Kept rows survive.
            foreach (var k in kept)
            {
                Assert.That(index.Deleted, Does.Not.Contain(PostingRow(tag, subjectTreeId, k)),
                    $"Kept posting row for '{k}' must not be dropped.");
            }
        });
    }

    private static async IAsyncEnumerable<string> LiveKeys(
        string[] keys,
        string? start,
        string? end,
        [EnumeratorCancellation] CancellationToken ct)
    {
        // Fully synchronous (no Task.Yield): the reconcile then runs inline on
        // the caller thread with no thread-pool continuation hops, so the
        // drain-before-delete ordering is observed deterministically under load.
        await Task.CompletedTask;
        foreach (var k in keys)
        {
            ct.ThrowIfCancellationRequested();
            if (start is not null && string.CompareOrdinal(k, start) < 0) continue;
            if (end is not null && string.CompareOrdinal(k, end) >= 0) continue;
            yield return k;
        }
    }

    // An NSubstitute-backed index tree over an in-memory sorted store that flags
    // any DeleteAsync issued while a KeysAsync scan enumerator is still open.
    private sealed class InstrumentedIndexTree
    {
        private int _scanDepth;

        public SortedDictionary<string, byte[]> Data { get; } = new(StringComparer.Ordinal);
        public List<string> Deleted { get; } = [];
        public bool DeleteDuringScanObserved { get; private set; }
        public ILattice Lattice { get; }

        public InstrumentedIndexTree()
        {
            var sub = Substitute.For<ILattice>();
            sub.KeysAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
                .Returns(ci => ScanKeys(ci.ArgAt<string?>(0), ci.ArgAt<string?>(1), ci.ArgAt<bool>(2), ci.ArgAt<CancellationToken>(4)));
            sub.DeleteAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                .Returns(ci =>
                {
                    if (Volatile.Read(ref _scanDepth) > 0)
                    {
                        DeleteDuringScanObserved = true;
                    }
                    var key = ci.ArgAt<string>(0);
                    Deleted.Add(key);
                    return Task.FromResult(Data.Remove(key));
                });
            Lattice = sub;
        }

        private async IAsyncEnumerable<string> ScanKeys(
            string? start,
            string? end,
            bool reverse,
            [EnumeratorCancellation] CancellationToken ct)
        {
            Interlocked.Increment(ref _scanDepth);
            try
            {
                // Snapshot the keys at scan start so a mid-scan delete cannot
                // throw a 'collection modified' fault and mask the invariant.
                var keys = Data.Keys.ToList();
                if (reverse) keys.Reverse();
                // Fully synchronous (no Task.Yield) so the reconcile runs inline
                // and the drain-before-delete ordering is deterministic.
                await Task.CompletedTask;
                foreach (var k in keys)
                {
                    ct.ThrowIfCancellationRequested();
                    if (start is not null && string.CompareOrdinal(k, start) < 0) continue;
                    if (end is not null && string.CompareOrdinal(k, end) >= 0) continue;
                    yield return k;
                }
            }
            finally
            {
                Interlocked.Decrement(ref _scanDepth);
            }
        }
    }
}
