using System.Runtime.CompilerServices;
using NSubstitute;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class TagIndexReconcileGrainTests
{
    [TestCase("mirror-index")]
    [TestCase("mirror-index-recheck")]
    [TestCase("mirror-subject")]
    [TestCase("orphan-subject")]
    public async Task ReceiveReminder_incomplete_repair_does_not_bank_a_baseline_and_recovers_with_unchanged_digest(
        string gatedRead)
    {
        const string treeId = "gate-completion";
        const string posting = "red\0gate-completion\0key";
        const string mirror = "\0k\0gate-completion\0key\0red";
        var orphan = gatedRead == "orphan-subject";
        var harness = CreateSweepGrain(
            configureSubjectTrees: factory => StubSubjectTree(factory, treeId, [7, 8, 9]));
        var subject = harness.GrainFactory.GetGrain<ILattice>(treeId);
        var gated = true;
        var indexReads = 0;
        subject.KeysAsync(
                Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(),
                Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(_ => GateCompletionKeys(orphan));
        subject.GetManyWithGateAccountingAsync(Arg.Any<List<string>>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                var pruned = gated && (gatedRead == "mirror-subject" || orphan);
                return Task.FromResult(new GatedMultiReadResult
                {
                    Values = !orphan && !pruned ? new(StringComparer.Ordinal) { ["key"] = [1] } : [],
                    PrunedByAccessGate = pruned ? 1 : 0,
                });
            });
        harness.IndexTree.Data[$"\0covered\0{treeId}"] = [1];
        harness.IndexTree.Data[posting] = [1];
        if (orphan)
        {
            harness.IndexTree.Data[mirror] = [1];
        }
        harness.IndexTree.Lattice.GetManyWithGateAccountingAsync(Arg.Any<List<string>>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                indexReads++;
                var pruned = gated && (gatedRead == "mirror-index"
                    || (gatedRead == "mirror-index-recheck" && indexReads > 1));
                return Task.FromResult(new GatedMultiReadResult
                {
                    Values = pruned ? [] : ci.Arg<List<string>>()
                        .Where(harness.IndexTree.Data.ContainsKey)
                        .ToDictionary(key => key, key => harness.IndexTree.Data[key], StringComparer.Ordinal),
                    PrunedByAccessGate = pruned ? 1 : 0,
                });
            });

        var fingerprint = await harness.Grain.ComputeFingerprintAsync(treeId, CancellationToken.None);
        await harness.Grain.ReceiveReminder("tag-index-reconcile-schedule", new TickStatus());
        await harness.Grain.ProcessNextPhaseAsync();
        Assert.That(harness.State.State.Phase, Is.EqualTo(TagIndexReconcilePhase.Repair));

        InvalidOperationException? incomplete = null;
        try
        {
            await harness.Grain.ProcessNextPhaseAsync();
        }
        catch (InvalidOperationException exception)
        {
            incomplete = exception;
        }
        var prematurelyBanked = harness.State.State.Baselines.ContainsKey(treeId);
        var postingWhileGated = harness.IndexTree.Data.ContainsKey(posting);
        var mirrorWhileGated = harness.IndexTree.Data.ContainsKey(mirror);

        gated = false;
        await DrainGateCompletionSweepAsync(harness);
        // Exercise the next real scheduled probe as well as any in-flight retry:
        // the subject fingerprint is identical, so a premature baseline skips it.
        await harness.Grain.ReceiveReminder("tag-index-reconcile-schedule", new TickStatus());
        await DrainGateCompletionSweepAsync(harness);
        var unchangedFingerprint = await harness.Grain.ComputeFingerprintAsync(treeId, CancellationToken.None);
        var bound = LatticeTagIndexContext.CreateForCoordinator(harness.GrainFactory, IndexName)
            .MultiTree().Tree(treeId);
        var tags = await bound.Key("key").GetAsync();
        var matches = new List<string>();
        await foreach (var key in bound.WithAnyTags("red"))
        {
            matches.Add(key);
        }

        Assert.Multiple(() =>
        {
            Assert.That(incomplete, Is.Not.Null, "a gate-pruned repair must not return a completed report");
            Assert.That(prematurelyBanked, Is.False, "an unverified repair must not certify the subject fingerprint");
            Assert.That(postingWhileGated, Is.True, "the gate must not permit orphan deletion");
            Assert.That(mirrorWhileGated, Is.EqualTo(orphan), "the gate must not permit a mirror repair or removal");
            Assert.That(unchangedFingerprint, Is.EqualTo(fingerprint), "recovery must not depend on subject digest drift");
            Assert.That(harness.IndexTree.Data.ContainsKey(posting), Is.EqualTo(!orphan));
            Assert.That(harness.IndexTree.Data.ContainsKey(mirror), Is.EqualTo(!orphan),
                "a later scheduled sweep must finish the previously gated repair");
            Assert.That(tags, Is.EqualTo(orphan ? Array.Empty<string>() : new[] { "red" }));
            Assert.That(matches, Is.EqualTo(orphan ? Array.Empty<string>() : new[] { "key" }));
            Assert.That(harness.State.State.Baselines[treeId], Is.EqualTo(fingerprint));
        });
    }

    private static async Task DrainGateCompletionSweepAsync(SweepHarness harness)
    {
        for (var turn = 0; turn < 4 && harness.State.State.InProgress; turn++)
        {
            await harness.Grain.ProcessNextPhaseAsync();
        }
        Assert.That(harness.State.State.InProgress, Is.False, "the one-tree sweep must finish within four phase turns");
    }

    private static async IAsyncEnumerable<string> GateCompletionKeys(
        bool orphan, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await Task.CompletedTask;
        cancellationToken.ThrowIfCancellationRequested();
        if (!orphan)
        {
            yield return "key";
        }
    }
}
