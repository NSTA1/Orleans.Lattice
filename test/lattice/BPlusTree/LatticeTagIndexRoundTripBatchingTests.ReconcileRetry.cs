using System.Runtime.CompilerServices;
using NSubstitute;

namespace Orleans.Lattice.Tests.BPlusTree;

public partial class LatticeTagIndexRoundTripBatchingTests
{
    [TestCase(LatticeMergeMode.LwwRegister, true)]
    [TestCase(LatticeMergeMode.LwwRegister, false)]
    [TestCase(LatticeMergeMode.OrFlag, true)]
    [TestCase(LatticeMergeMode.OrFlag, false)]
    [TestCase(LatticeMergeMode.RwFlag, true)]
    [TestCase(LatticeMergeMode.RwFlag, false)]
    public async Task ReconcileSubjectAsync_partial_orphan_removal_retries_both_row_representations(
        LatticeMergeMode mode, bool failMirror)
    {
        var (index, subject, ctx) = CreateWithSubject(mode);
        if (mode != LatticeMergeMode.LwwRegister)
        {
            ConfigureFlagRows(index);
        }
        await SeedOrphansAsync(ctx, subject, keyCount: 2, tagCount: 1);
        var bound = ctx.MultiTree().Tree(SubjectTreeId);
        const string posting = "tag00\0orders\0k000";
        const string mirror = "\0k\0orders\0k000\0tag00";
        var failedRow = failMirror ? mirror : posting;
        var failure = new IOException("transient orphan removal failure");
        var failures = 0;

        if (mode == LatticeMergeMode.LwwRegister)
        {
            index.Lattice.DeleteAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                .Returns(ci =>
                {
                    var row = ci.Arg<string>();
                    if (row == failedRow && failures++ == 0)
                    {
                        return Task.FromException<bool>(failure);
                    }
                    return Task.FromResult(index.Data.Remove(row));
                });
        }
        else
        {
            index.Lattice.ApplyCrdtDeltaAsync(
                    Arg.Any<string>(), Arg.Any<LatticeMergeMode>(),
                    Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
                .Returns(ci =>
                {
                    var row = ci.Arg<string>();
                    if (row == failedRow && failures++ == 0)
                    {
                        return Task.FromException<HybridLogicalClock>(failure);
                    }
                    MergeFlagRow(index, row, ci.Arg<LatticeMergeMode>(), ci.Arg<byte[]>());
                    return Task.FromResult(default(HybridLogicalClock));
                });
        }

        var thrown = Assert.ThrowsAsync<IOException>(async () =>
            await ctx.ReconcileSubjectAsync(SubjectTreeId, null, null, CancellationToken.None));
        var postingAfterFailure = IsStoredRowLive(index, posting, mode);
        var mirrorAfterFailure = IsStoredRowLive(index, mirror, mode);
        var retry = await ctx.ReconcileSubjectAsync(SubjectTreeId, null, null, CancellationToken.None);
        var tags = await bound.Key("k000").GetAsync();
        var query = new List<string>();
        await foreach (var key in bound.WithAnyTags("tag00"))
        {
            query.Add(key);
        }
        var settled = await ctx.ReconcileSubjectAsync(SubjectTreeId, null, null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(thrown, Is.SameAs(failure), "the partial write must surface its original failure");
            Assert.That(postingAfterFailure, Is.True, "the posting must remain discoverable until both removals succeed");
            Assert.That(mirrorAfterFailure, Is.EqualTo(failMirror));
            Assert.That(retry.OrphanRowsRemoved, Is.EqualTo(1), "retry must rediscover the failed pair");
            Assert.That(IsStoredRowLive(index, posting, mode), Is.False, "retry must remove the orphan posting");
            Assert.That(IsStoredRowLive(index, mirror, mode), Is.False, "retry must remove the orphan mirror");
            Assert.That(tags, Is.Empty, "key-to-tags lookup must not retain the orphan mirror");
            Assert.That(query, Is.Empty, "tag-to-key lookup must not retain an orphan posting");
            Assert.That(settled.OrphanRowsRemoved, Is.Zero, "successful retry must converge");
            Assert.That(index.Data.ContainsKey($"\0covered\0{SubjectTreeId}"), Is.True);
        });
    }

    [TestCase(LatticeMergeMode.LwwRegister, true, true)]
    [TestCase(LatticeMergeMode.LwwRegister, true, false)]
    [TestCase(LatticeMergeMode.LwwRegister, false, true)]
    [TestCase(LatticeMergeMode.LwwRegister, false, false)]
    [TestCase(LatticeMergeMode.OrFlag, true, true)]
    [TestCase(LatticeMergeMode.OrFlag, true, false)]
    [TestCase(LatticeMergeMode.OrFlag, false, true)]
    [TestCase(LatticeMergeMode.OrFlag, false, false)]
    [TestCase(LatticeMergeMode.RwFlag, true, true)]
    [TestCase(LatticeMergeMode.RwFlag, true, false)]
    [TestCase(LatticeMergeMode.RwFlag, false, true)]
    [TestCase(LatticeMergeMode.RwFlag, false, false)]
    public async Task ReconcileSubjectAsync_partial_orphan_removal_reverifies_subject_and_repairs_live_membership(
        LatticeMergeMode mode, bool failMirror, bool hiddenFromSnapshot)
    {
        var (index, subject, ctx) = CreateWithSubject(mode);
        if (mode != LatticeMergeMode.LwwRegister)
        {
            ConfigureFlagRows(index);
        }
        await SeedOrphansAsync(ctx, subject, keyCount: 1, tagCount: 1);
        const string posting = "tag00\0orders\0k000";
        const string mirror = "\0k\0orders\0k000\0tag00";
        var failure = new IOException("transient row removal failure");
        var failedRow = failMirror ? mirror : posting;
        var failures = 0;
        if (mode == LatticeMergeMode.LwwRegister)
        {
            index.Lattice.DeleteAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                .Returns(ci =>
                {
                    var row = ci.Arg<string>();
                    if (row == failedRow && failures++ == 0)
                    {
                        return Task.FromException<bool>(failure);
                    }
                    return Task.FromResult(index.Data.Remove(row));
                });
        }
        else
        {
            index.Lattice.ApplyCrdtDeltaAsync(
                    Arg.Any<string>(), mode, Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
                .Returns(ci =>
                {
                    var row = ci.Arg<string>();
                    if (row == failedRow && failures++ == 0)
                    {
                        return Task.FromException<HybridLogicalClock>(failure);
                    }
                    MergeFlagRow(index, row, mode, ci.Arg<byte[]>());
                    return Task.FromResult(default(HybridLogicalClock));
                });
        }

        Assert.ThrowsAsync<IOException>(async () =>
            await ctx.ReconcileSubjectAsync(SubjectTreeId, null, null, CancellationToken.None));

        // A concurrent value write lands either before or after the retry's live snapshot.
        subject.Data["k000"] = [1];
        if (hiddenFromSnapshot)
        {
            subject.HiddenFromScan.Add("k000");
        }
        var probesBefore = subject.GateAccountedCalls;
        var retry = await ctx.ReconcileSubjectAsync(SubjectTreeId, null, null, CancellationToken.None);
        var bound = ctx.MultiTree().Tree(SubjectTreeId);
        var tags = await bound.Key("k000").GetAsync();
        var query = new List<string>();
        await foreach (var key in bound.WithAnyTags("tag00"))
        {
            query.Add(key);
        }
        var mirrorAfterRetry = index.Data[mirror].ToArray();
        await ctx.ReconcileSubjectAsync(SubjectTreeId, null, null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            if (hiddenFromSnapshot)
            {
                Assert.That(subject.GateAccountedCalls, Is.GreaterThan(probesBefore),
                    "retry must re-verify the posting candidate rather than reuse its prior absence");
            }
            Assert.That(retry.OrphanRowsRemoved, Is.Zero);
            Assert.That(IsStoredRowLive(index, posting, mode), Is.True);
            Assert.That(IsStoredRowLive(index, mirror, mode), Is.True);
            Assert.That(tags, Is.EqualTo(new[] { "tag00" }));
            Assert.That(query, Is.EqualTo(new[] { "k000" }));
            Assert.That(index.Data[mirror], Is.EqualTo(mirrorAfterRetry),
                "a converged mirror must not receive another value write or flag enable");
        });
    }

    private static void ConfigureFlagRows(CountingTree index)
    {
        index.Lattice.ApplyCrdtDeltaAsync(
                Arg.Any<string>(), Arg.Any<LatticeMergeMode>(),
                Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                MergeFlagRow(index, ci.Arg<string>(), ci.Arg<LatticeMergeMode>(), ci.Arg<byte[]>());
                return Task.FromResult(default(HybridLogicalClock));
            });
        index.Lattice.EntriesAsync(
                Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(),
                Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(ci => FlagEntries(index, ci.ArgAt<string?>(0), ci.ArgAt<string?>(1), ci.ArgAt<CancellationToken>(4)));
    }

    private static void MergeFlagRow(CountingTree index, string row, LatticeMergeMode mode, byte[] delta)
    {
        index.Data.TryGetValue(row, out var bytes);
        if (mode == LatticeMergeMode.OrFlag)
        {
            var flag = bytes is null ? new OrFlag() : JsonLatticeSerializer<OrFlag>.Default.Deserialize(bytes);
            flag.MergeDelta(JsonLatticeSerializer<OrFlagDelta>.Default.Deserialize(delta));
            index.Data[row] = JsonLatticeSerializer<OrFlag>.Default.Serialize(flag);
        }
        else
        {
            var flag = bytes is null ? new RwFlag() : JsonLatticeSerializer<RwFlag>.Default.Deserialize(bytes);
            flag.MergeDelta(JsonLatticeSerializer<RwFlagDelta>.Default.Deserialize(delta));
            index.Data[row] = JsonLatticeSerializer<RwFlag>.Default.Serialize(flag);
        }
    }

    private static bool IsStoredRowLive(CountingTree index, string row, LatticeMergeMode mode)
    {
        if (!index.Data.TryGetValue(row, out var bytes))
        {
            return false;
        }
        return mode switch
        {
            LatticeMergeMode.OrFlag => JsonLatticeSerializer<OrFlag>.Default.Deserialize(bytes).IsEnabled,
            LatticeMergeMode.RwFlag => JsonLatticeSerializer<RwFlag>.Default.Deserialize(bytes).IsEnabled,
            _ => true,
        };
    }

    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> FlagEntries(
        CountingTree index, string? start, string? end,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await Task.CompletedTask;
        foreach (var entry in index.Data.ToArray())
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (start is not null && string.CompareOrdinal(entry.Key, start) < 0) continue;
            if (end is not null && string.CompareOrdinal(entry.Key, end) >= 0) continue;
            yield return entry;
        }
    }
}
