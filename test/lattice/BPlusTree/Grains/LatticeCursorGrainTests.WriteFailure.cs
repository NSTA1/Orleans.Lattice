using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class LatticeCursorGrainTests
{
    // ----------------------------------------------------------------------
    // Class B "persisted/in-memory divergence on failing WriteStateAsync"
    // regressions. Each grain method that mutates state.State before awaiting
    // state.WriteStateAsync must restore the prior values when the write
    // throws, otherwise the in-memory snapshot is dirtied and disk stays
    // behind. In LatticeCursorGrain every such site is followed by an
    // idempotency-guarded short-circuit (Phase == NotStarted on OpenAsync,
    // Phase == Exhausted on Next*/DeleteRangeStepAsync), so a single failed
    // persist permanently misleads the activation: empty pages are returned,
    // re-open is silently accepted with a stale spec, or DeletedTotal counts
    // are double-reported on retry without ever reaching disk.
    // ----------------------------------------------------------------------

    [Test]
    public void OpenAsync_reverts_state_when_WriteStateAsync_throws()
    {
        var (grain, state, _) = CreateGrain();
        state.ThrowOnWrite = new InvalidOperationException("simulated storage failure");

        var spec = new LatticeCursorSpec { Kind = LatticeCursorKind.Keys };

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.OpenAsync(TreeId, spec));

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Phase, Is.EqualTo(LatticeCursorPhase.NotStarted),
                "Phase must remain NotStarted in-memory so a retry re-enters the guarded branch.");
            Assert.That(state.State.TreeId, Is.Empty,
                "TreeId must remain empty in-memory; otherwise the spec-mismatch guard rejects valid retries.");
            Assert.That(state.State.Spec.Kind, Is.EqualTo(LatticeCursorKind.Keys),
                "Default Spec.Kind is Keys; the in-memory Spec must not retain the unpersisted mutation. " +
                "(This assertion would still hold trivially for a Keys spec; the load-bearing check is on Phase/TreeId.)");
        });
    }

    [Test]
    public void NextKeysAsync_reverts_state_when_WriteStateAsync_throws()
    {
        var existing = new FakePersistentState<LatticeCursorState>
        {
            State =
            {
                TreeId = TreeId,
                Spec = new LatticeCursorSpec { Kind = LatticeCursorKind.Keys },
                Phase = LatticeCursorPhase.Open,
                LastYieldedKey = "k0",
            },
        };
        var (grain, state, lattice) = CreateGrain(existingState: existing);
        lattice.KeysAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>())
            .Returns(_ => ToAsyncEnumerable(new[] { "k1", "k2" }));

        state.ThrowOnWrite = new InvalidOperationException("simulated storage failure");

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.NextKeysAsync(pageSize: 10));

        Assert.Multiple(() =>
        {
            Assert.That(state.State.LastYieldedKey, Is.EqualTo("k0"),
                "LastYieldedKey must remain at k0 when WriteStateAsync throws, otherwise a retry resumes " +
                "past k1 and silently loses k1.");
            Assert.That(state.State.Phase, Is.EqualTo(LatticeCursorPhase.Open),
                "Phase must remain Open in-memory; otherwise the Phase==Exhausted short-circuit at the " +
                "top of NextKeysAsync returns empty pages forever from this activation.");
        });
    }

    [Test]
    public void NextEntriesAsync_reverts_state_when_WriteStateAsync_throws()
    {
        var existing = new FakePersistentState<LatticeCursorState>
        {
            State =
            {
                TreeId = TreeId,
                Spec = new LatticeCursorSpec { Kind = LatticeCursorKind.Entries },
                Phase = LatticeCursorPhase.Open,
                LastYieldedKey = "k0",
            },
        };
        var (grain, state, lattice) = CreateGrain(existingState: existing);
        lattice.EntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>())
            .Returns(_ => ToAsyncEnumerable(new[]
            {
                new KeyValuePair<string, byte[]>("k1", new byte[] { 1 }),
                new KeyValuePair<string, byte[]>("k2", new byte[] { 2 }),
            }));

        state.ThrowOnWrite = new InvalidOperationException("simulated storage failure");

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.NextEntriesAsync(pageSize: 10));

        Assert.Multiple(() =>
        {
            Assert.That(state.State.LastYieldedKey, Is.EqualTo("k0"),
                "LastYieldedKey must remain at k0 when WriteStateAsync throws.");
            Assert.That(state.State.Phase, Is.EqualTo(LatticeCursorPhase.Open),
                "Phase must remain Open; otherwise Exhausted short-circuit returns empty entries pages forever.");
        });
    }

    [Test]
    public void DeleteRangeStepAsync_reverts_Phase_when_empty_probe_WriteStateAsync_throws()
    {
        var existing = new FakePersistentState<LatticeCursorState>
        {
            State =
            {
                TreeId = TreeId,
                Spec = new LatticeCursorSpec
                {
                    Kind = LatticeCursorKind.DeleteRange,
                    StartInclusive = "a",
                    EndExclusive = "z",
                },
                Phase = LatticeCursorPhase.Open,
                LastYieldedKey = null,
                DeletedTotal = 7,
            },
        };
        var (grain, state, lattice) = CreateGrain(existingState: existing);
        // Empty probe so the empty-probe branch is hit (writes Phase=Exhausted only).
        lattice.KeysAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>())
            .Returns(_ => ToAsyncEnumerable(Array.Empty<string>()));

        state.ThrowOnWrite = new InvalidOperationException("simulated storage failure");

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.DeleteRangeStepAsync(maxToDelete: 5));

        Assert.That(state.State.Phase, Is.EqualTo(LatticeCursorPhase.Open),
            "Phase must remain Open when the empty-probe WriteStateAsync throws, otherwise the " +
            "Phase==Exhausted short-circuit at the top of DeleteRangeStepAsync reports IsComplete=true " +
            "for every retry without ever persisting the completion.");
    }

    [Test]
    public void DeleteRangeStepAsync_reverts_state_when_normal_WriteStateAsync_throws()
    {
        var existing = new FakePersistentState<LatticeCursorState>
        {
            State =
            {
                TreeId = TreeId,
                Spec = new LatticeCursorSpec
                {
                    Kind = LatticeCursorKind.DeleteRange,
                    StartInclusive = "a",
                    EndExclusive = "z",
                },
                Phase = LatticeCursorPhase.Open,
                LastYieldedKey = "k0",
                DeletedTotal = 7,
            },
        };
        var (grain, state, lattice) = CreateGrain(existingState: existing);
        // Probe surfaces a final-step's worth of keys (count <= maxToDelete).
        lattice.KeysAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>())
            .Returns(_ => ToAsyncEnumerable(new[] { "k1", "k2" }));
        lattice.DeleteRangeAsync(Arg.Any<string>(), Arg.Any<string>()).Returns(2);

        state.ThrowOnWrite = new InvalidOperationException("simulated storage failure");

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.DeleteRangeStepAsync(maxToDelete: 5));

        Assert.Multiple(() =>
        {
            Assert.That(state.State.LastYieldedKey, Is.EqualTo("k0"),
                "LastYieldedKey must remain at k0; otherwise a retry resumes past k2 and double-deletes a prefix.");
            Assert.That(state.State.Phase, Is.EqualTo(LatticeCursorPhase.Open),
                "Phase must remain Open; otherwise the Exhausted short-circuit reports IsComplete=true " +
                "with a stale DeletedTotal forever.");
            Assert.That(state.State.DeletedTotal, Is.EqualTo(7),
                "DeletedTotal must remain at its pre-call value; otherwise a successful retry's reported " +
                "DeletedTotal double-counts the deletions that already happened in the failed step.");
        });
    }
}