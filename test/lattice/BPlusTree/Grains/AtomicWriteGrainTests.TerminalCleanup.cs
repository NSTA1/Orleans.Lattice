using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Terminal-state cleanup regressions for
/// <see cref="Orleans.Lattice.BPlusTree.Grains.AtomicWriteGrain"/>. A completed
/// saga is retained only so an idempotent re-entry (or a reminder-driven
/// reactivation) can re-derive its lightweight outcome; the heavy staged batch
/// (<see cref="AtomicWriteState.Entries"/>, <see cref="AtomicWriteState.PreValues"/>,
/// and the per-entry delta/delete carries) is released on the terminal
/// checkpoint so it does not pin the saga's value payload in the grain store for
/// the whole retention window. These tests pin that release, the retention of
/// the lightweight outcome fields, and that a released terminal record still
/// replays idempotently.
/// </summary>
public partial class AtomicWriteGrainTests
{
    [Test]
    public async Task ExecuteAsync_success_releases_staged_payload_but_retains_outcome()
    {
        var (grain, state, _, _, shard) = CreateGrain();
        StubPreValue(shard, "a", [9, 9]);
        StubPreValue(shard, "b", null);

        await grain.ExecuteAsync(TreeId, MakeEntries(("a", [1]), ("b", [2])));

        Assert.Multiple(() =>
        {
            // The heavy staged batch is released.
            Assert.That(state.State.Entries, Is.Empty,
                "Completed saga must release its staged Entries.");
            Assert.That(state.State.PreValues, Is.Empty,
                "Completed saga must release its captured PreValues.");
            Assert.That(state.State.EntryDeltas, Is.Null);
            Assert.That(state.State.EntryDeletes, Is.Null);
            Assert.That(state.State.Delta, Is.Null);

            // The lightweight outcome fields are retained so an idempotent
            // re-entry and the fingerprint guard keep working.
            Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.Completed));
            Assert.That(state.State.KeyFingerprint, Is.Not.Null,
                "KeyFingerprint drives the re-entry mismatch guard and must survive the release.");
            Assert.That(state.State.TransactionId, Is.Not.EqualTo(Guid.Empty),
                "TransactionId must survive so a resumed terminal broadcast stays correlated.");
            Assert.That(state.State.FailureMessage, Is.Null,
                "A committed saga has no failure message.");
            // Small scalar/metadata fields carry no value bytes, so they are
            // deliberately retained.
            Assert.That(state.State.AtomicBatchSize, Is.EqualTo(2));
            Assert.That(state.State.TouchedShards, Is.Not.Empty);
        });
    }

    [Test]
    public async Task ExecuteAsync_compensated_saga_releases_staged_payload_and_retains_failure()
    {
        var (grain, state, _, lattice, shard) = CreateGrain();
        shard.GetRawEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<LwwEntry?>(null));
        lattice.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Throws(new InvalidOperationException("forward write blew up"));

        Exception? caught = null;
        try
        {
            await grain.ExecuteAsync(TreeId, MakeEntries(("a", [1]), ("b", [2])));
        }
        catch (Exception ex)
        {
            caught = ex;
        }

        Assert.That(caught, Is.Not.Null);
        Assert.Multiple(() =>
        {
            // A rolled-back saga is still terminal (Completed) and still
            // releases its staged payload - the pre-values were consumed by
            // compensation before the terminal checkpoint.
            Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.Completed));
            Assert.That(state.State.Entries, Is.Empty);
            Assert.That(state.State.PreValues, Is.Empty);
            Assert.That(state.State.EntryDeltas, Is.Null);
            Assert.That(state.State.EntryDeletes, Is.Null);
            Assert.That(state.State.Delta, Is.Null);
            // The failure message is a lightweight outcome field and must be
            // retained so a re-entry re-throws the memoized failure.
            Assert.That(state.State.FailureMessage, Is.Not.Null,
                "A compensated saga must retain its FailureMessage for idempotent re-throw.");
        });
    }

    [Test]
    public async Task ExecuteAsync_reentry_after_released_payload_is_idempotent_noop()
    {
        // A completed saga whose staged payload has already been released (empty
        // Entries/PreValues) must still replay as an idempotent success: the
        // re-entry path reads only Phase, FailureMessage, and KeyFingerprint,
        // never the released collections.
        var original = MakeEntries(("k1", [1]), ("k2", [2]));
        var seeded = new FakePersistentState<AtomicWriteState>
        {
            State =
            {
                Phase = AtomicWritePhase.Completed,
                TreeId = TreeId,
                Entries = [],
                PreValues = [],
                KeyFingerprint = AtomicWriteGrain.ComputeKeyFingerprint(original),
                TransactionId = Guid.NewGuid(),
            },
        };

        var (grain, state, _, lattice, _) = CreateGrain(existingState: seeded);

        // Same key set (values may differ on a retry) - must not throw.
        await grain.ExecuteAsync(TreeId, MakeEntries(("k2", [99]), ("k1", [88])));

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.Completed),
                "Re-entry on a released terminal saga is a pure no-op.");
            Assert.That(state.State.Entries, Is.Empty,
                "Re-entry must not re-populate the released staged batch.");
        });
        await lattice.DidNotReceive().SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>());
    }
}
