using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree;

public partial class LatticeTagIndexRoundTripBatchingTests
{
    [TestCase("index-gate")]
    [TestCase("index-recheck-gate")]
    [TestCase("subject-gate")]
    [TestCase("posting-removed")]
    [TestCase("subject-removed")]
    public async Task ReconcileSubjectAsync_missing_mirror_repair_requires_visible_live_subject_and_posting(
        string interference)
    {
        var (index, subject, ctx) = CreateWithSubject();
        await SeedOrphansAsync(ctx, subject, keyCount: 1, tagCount: 1);
        const string posting = "tag00\0orders\0k000";
        const string mirror = "\0k\0orders\0k000\0tag00";
        subject.Data["k000"] = [1];
        index.Data.Remove(mirror);
        var reads = 0;
        index.Lattice.GetManyWithGateAccountingAsync(Arg.Any<List<string>>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                reads++;
                if (reads == 2 && interference == "posting-removed")
                {
                    index.Data.Remove(posting);
                }
                if (reads == 1 && interference == "subject-removed")
                {
                    subject.Data.Clear();
                }
                var pruned = interference == "index-gate"
                    || (interference == "index-recheck-gate" && reads > 1);
                return Task.FromResult(new GatedMultiReadResult
                {
                    Values = ci.Arg<List<string>>()
                        .Where(index.Data.ContainsKey)
                        .ToDictionary(key => key, key => index.Data[key], StringComparer.Ordinal),
                    PrunedByAccessGate = pruned ? 1 : 0,
                });
            });
        subject.PrunedByAccessGate = interference == "subject-gate" ? 1 : 0;
        var writesBefore = index.TotalWriteCalls;

        var report = await ctx.ReconcileSubjectAsync(SubjectTreeId, null, null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(index.Data.ContainsKey(mirror), Is.False, "unverified repair must not resurrect a mirror");
            Assert.That(index.TotalWriteCalls, Is.EqualTo(writesBefore));
            Assert.That(report.OrphanRowsRemoved, Is.Zero);
        });
    }

    [TestCase(LatticeMergeMode.LwwRegister)]
    [TestCase(LatticeMergeMode.OrFlag)]
    [TestCase(LatticeMergeMode.RwFlag)]
    public async Task ReconcileSubjectAsync_failed_mirror_repair_surfaces_and_retries(
        LatticeMergeMode mode)
    {
        var (index, subject, ctx) = CreateWithSubject(mode);
        if (mode != LatticeMergeMode.LwwRegister)
        {
            ConfigureFlagRows(index);
        }
        await SeedOrphansAsync(ctx, subject, keyCount: 1, tagCount: 1);
        const string posting = "tag00\0orders\0k000";
        const string mirror = "\0k\0orders\0k000\0tag00";
        subject.Data["k000"] = [1];
        index.Data.Remove(mirror);
        var failure = new IOException("transient mirror repair failure");
        var attempts = 0;
        if (mode == LatticeMergeMode.LwwRegister)
        {
            index.Lattice.SetAsync(mirror, Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
                .Returns(ci =>
                {
                    if (attempts++ == 0)
                    {
                        return Task.FromException(failure);
                    }
                    index.Data[mirror] = ci.Arg<byte[]>();
                    return Task.CompletedTask;
                });
        }
        else
        {
            index.Lattice.ApplyCrdtDeltaAsync(
                    mirror, mode, Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
                .Returns(ci =>
                {
                    if (attempts++ == 0)
                    {
                        return Task.FromException<HybridLogicalClock>(failure);
                    }
                    MergeFlagRow(index, mirror, mode, ci.Arg<byte[]>());
                    return Task.FromResult(default(HybridLogicalClock));
                });
        }

        var thrown = Assert.ThrowsAsync<IOException>(async () =>
            await ctx.ReconcileSubjectAsync(SubjectTreeId, null, null, CancellationToken.None));
        Assert.Multiple(() =>
        {
            Assert.That(thrown, Is.SameAs(failure));
            Assert.That(IsStoredRowLive(index, posting, mode), Is.True);
            Assert.That(IsStoredRowLive(index, mirror, mode), Is.False);
        });
        var retry = await ctx.ReconcileSubjectAsync(SubjectTreeId, null, null, CancellationToken.None);
        var tags = await ctx.MultiTree().Tree(SubjectTreeId).Key("k000").GetAsync();

        Assert.Multiple(() =>
        {
            Assert.That(retry.OrphanRowsRemoved, Is.Zero, "a repaired live membership is not an orphan removal");
            Assert.That(IsStoredRowLive(index, posting, mode), Is.True);
            Assert.That(IsStoredRowLive(index, mirror, mode), Is.True);
            Assert.That(tags, Is.EqualTo(new[] { "tag00" }));
            Assert.That(attempts, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task ReconcileSubjectAsync_mirror_checks_are_bounded_and_converged_rows_are_not_rewritten()
    {
        var (index, subject, ctx) = CreateWithSubject();
        const int keys = 70;
        const int tags = 2;
        await SeedOrphansAsync(ctx, subject, keys, tags);
        for (var k = 0; k < keys; k++)
        {
            subject.Data[$"k{k:D3}"] = [1];
        }
        var writesBefore = index.TotalWriteCalls;

        await ctx.ReconcileSubjectAsync(SubjectTreeId, null, null, CancellationToken.None);

        var window = LatticeTagIndexContext.OrphanVerifyWindow;
        Assert.Multiple(() =>
        {
            Assert.That(index.GateAccountedCalls, Is.EqualTo((keys * tags + window - 1) / window));
            Assert.That(index.GateAccountedWidths.Sum(), Is.EqualTo(keys * tags));
            Assert.That(index.GateAccountedWidths.Max(), Is.EqualTo(window));
            Assert.That(index.TotalWriteCalls, Is.EqualTo(writesBefore));
            Assert.That(subject.GateAccountedCalls, Is.Zero, "intact mirrors need no extra subject confirmation");
        });
    }

    [Test]
    public async Task ReconcileSubjectAsync_missing_mirror_repairs_respect_tree_and_key_range()
    {
        var (index, subject, ctx) = CreateWithSubject();
        await SeedOrphansAsync(ctx, subject, keyCount: 3, tagCount: 1);
        for (var k = 0; k < 3; k++)
        {
            subject.Data[$"k{k:D3}"] = [1];
            index.Data.Remove($"\0k\0orders\0k{k:D3}\0tag00");
        }
        index.Data["tag00\0another-tree\0k001"] = [1];

        var report = await ctx.ReconcileSubjectAsync(SubjectTreeId, "k001", "k002", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(report.MembershipRowsScanned, Is.EqualTo(1));
            Assert.That(index.Data.ContainsKey("\0k\0orders\0k001\0tag00"), Is.True);
            Assert.That(index.Data.ContainsKey("\0k\0orders\0k000\0tag00"), Is.False);
            Assert.That(index.Data.ContainsKey("\0k\0orders\0k002\0tag00"), Is.False);
            Assert.That(index.Data.ContainsKey("\0k\0another-tree\0k001\0tag00"), Is.False);
        });
    }
}
