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

/// <summary>
/// Coverage for the two fail-closed choke points a cross-tree atomic write
/// passes through before any tree is mutated: per-leg authorization against the
/// registered <see cref="ILatticeAccessGate"/>, and per-leg schema enforcement /
/// value transformation against the registered
/// <see cref="ILatticeWriteInterceptor"/>.
/// <para>
/// Both short-circuit to a zero-cost no-op under the default null gate /
/// interceptor, which is the configuration every other fixture in this class
/// runs under - so the enforcement bodies themselves are only reachable by
/// registering a real gate / interceptor on the activation's service provider,
/// which is what these tests do.
/// </para>
/// </summary>
public partial class LatticeCrossTreeTxGrainTests
{
    /// <summary>
    /// A gate that records every request it is asked to authorize and answers
    /// with a caller-supplied decision. Hand-written because NSubstitute cannot
    /// mock the <c>in</c> parameter on
    /// <see cref="ILatticeAccessGate.AuthorizeAsync"/>.
    /// </summary>
    private sealed class RecordingGate(Func<LatticeAccessRequest, LatticeAccessDecision>? decide = null)
        : ILatticeAccessGate
    {
        public List<(string TreeId, string Key, LatticeOperation Operation)> Requests { get; } = [];

        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request, CancellationToken cancellationToken = default)
        {
            var copy = request;
            Requests.Add((copy.TreeId, copy.Key ?? string.Empty, copy.Operation));
            return new ValueTask<LatticeAccessDecision>(
                decide is null ? LatticeAccessDecision.Allow() : decide(copy));
        }
    }

    /// <summary>
    /// A write interceptor that records every entry it sees and can rewrite the
    /// value of nominated keys, standing in for a schema/versioning interceptor.
    /// </summary>
    private sealed class RecordingInterceptor(Func<LatticeWriteRequest, LatticeWriteDecision>? decide = null)
        : ILatticeWriteInterceptor
    {
        public List<(string TreeId, string Key)> Seen { get; } = [];

        public ValueTask<LatticeWriteDecision> OnWriteAsync(
            in LatticeWriteRequest request, CancellationToken cancellationToken = default)
        {
            var copy = request;
            Seen.Add((copy.TreeId, copy.Key));
            return new ValueTask<LatticeWriteDecision>(
                decide is null ? LatticeWriteDecision.Accept() : decide(copy));
        }
    }

    /// <summary>
    /// Builds an activation service provider exposing the supplied gate and/or
    /// interceptor, so the coordinator's enforcement bodies run rather than
    /// short-circuiting on the null fallbacks.
    /// </summary>
    private static IServiceProvider Services(
        ILatticeAccessGate? gate = null, ILatticeWriteInterceptor? interceptor = null)
    {
        var services = Substitute.For<IServiceProvider>();
        services.GetService(typeof(ILatticeAccessGate)).Returns(gate);
        services.GetService(typeof(ILatticeWriteInterceptor)).Returns(interceptor);
        return services;
    }

    private static LatticeTreeBatch Batch(
        string treeId,
        List<KeyValuePair<string, byte[]>> entries,
        List<byte[]?>? deltas = null,
        List<bool>? deletes = null) => new(treeId, entries, null, deltas, deletes);

    private static List<KeyValuePair<string, byte[]>> Rows(params (string Key, string Value)[] rows)
    {
        var list = new List<KeyValuePair<string, byte[]>>(rows.Length);
        foreach (var (key, value) in rows)
        {
            list.Add(new KeyValuePair<string, byte[]>(key, System.Text.Encoding.UTF8.GetBytes(value)));
        }

        return list;
    }

    // ---- Per-leg authorization -------------------------------------------

    [Test]
    public async Task CommitAsync_authorizes_every_write_key_of_every_leg_before_dispatch()
    {
        var gate = new RecordingGate();
        var (grain, _, _, participants) = CreateGrain(
            ["inventory", "orders"], activationServices: Services(gate));

        await grain.CommitAsync([
            Batch("orders", Rows(("order:1", "A"), ("order:2", "B"))),
            Batch("inventory", Rows(("sku:1", "C"))),
        ]);

        Assert.That(
            gate.Requests,
            Is.EquivalentTo(new[]
            {
                ("orders", "order:1", LatticeOperation.Write),
                ("orders", "order:2", LatticeOperation.Write),
                ("inventory", "sku:1", LatticeOperation.Write),
            }),
            "every write key of every leg must be authorized as Write");
        await participants["orders"].Received(1).FinalizeAsync(true);
    }

    [Test]
    public async Task CommitAsync_authorizes_a_tombstone_leg_as_Delete_not_Write()
    {
        // The per-entry delete channel (not value-nullness) is what makes an
        // entry a tombstone, and a tombstone must be authorized as Delete.
        var gate = new RecordingGate();
        var (grain, _, _, _) = CreateGrain(["orders"], activationServices: Services(gate));

        await grain.CommitAsync([
            Batch("orders", Rows(("keep:1", "A"), ("gone:1", "")), deletes: [false, true]),
        ]);

        Assert.That(gate.Requests, Is.EquivalentTo(new[]
        {
            ("orders", "keep:1", LatticeOperation.Write),
            ("orders", "gone:1", LatticeOperation.Delete),
        }));
    }

    [Test]
    public async Task CommitAsync_authorizes_an_all_delete_leg_as_Delete_only()
    {
        // Exercises the branch where a leg produces delete keys but no write
        // keys at all, so only the Delete enforcement call is made.
        var gate = new RecordingGate();
        var (grain, _, _, _) = CreateGrain(["orders"], activationServices: Services(gate));

        await grain.CommitAsync([
            Batch("orders", Rows(("gone:1", ""), ("gone:2", "")), deletes: [true, true]),
        ]);

        Assert.That(gate.Requests.Select(r => r.Operation), Is.All.EqualTo(LatticeOperation.Delete));
        Assert.That(gate.Requests, Has.Count.EqualTo(2));
    }

    [Test]
    public void CommitAsync_denied_leg_throws_and_dispatches_no_participant()
    {
        // Fail-closed: a single denied key aborts the whole cross-tree write
        // before any participant saga is contacted.
        var gate = new RecordingGate(r => r.Key == "sku:1"
            ? LatticeAccessDecision.Deny("no grant on inventory")
            : LatticeAccessDecision.Allow());
        var (grain, state, _, participants) = CreateGrain(
            ["inventory", "orders"], activationServices: Services(gate));

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(() => grain.CommitAsync([
            Batch("orders", Rows(("order:1", "A"))),
            Batch("inventory", Rows(("sku:1", "C"))),
        ]));

        Assert.That(state.State.Phase, Is.EqualTo(CrossTreeTxPhase.NotStarted),
            "a denied leg must abort before the saga is ever started");
        Assert.That(
            participants["orders"].ReceivedCalls().Any(c => c.GetMethodInfo().Name == "PrepareForCoordinatorAsync"),
            Is.False,
            "no participant may be prepared once a leg is denied");
    }

    [Test]
    public void CommitAsync_authorization_skips_malformed_legs_and_still_rejects_them()
    {
        // A null-key entry is skipped by the enforcement walk (so the gate never
        // sees a malformed leg) and is then rejected with a precise
        // ArgumentException by BuildParticipants.
        var gate = new RecordingGate();
        var (grain, _, _, _) = CreateGrain(["orders"], activationServices: Services(gate));
        var entries = new List<KeyValuePair<string, byte[]>> { new(null!, [1]) };

        var ex = Assert.ThrowsAsync<ArgumentException>(() => grain.CommitAsync([Batch("orders", entries)]));

        Assert.That(ex!.Message, Does.Contain("null key"));
        Assert.That(gate.Requests, Is.Empty, "a null key must never be handed to the gate");
    }

    [Test]
    public async Task CommitAsync_authorization_skips_an_empty_leg()
    {
        // An empty per-tree slice is dropped rather than authorized: there is
        // nothing to authorize, and BuildParticipants drops it too.
        var gate = new RecordingGate();
        var (grain, state, _, _) = CreateGrain(["orders"], activationServices: Services(gate));

        var outcome = await grain.CommitAsync([
            Batch("empty", []),
            Batch("orders", Rows(("order:1", "A"))),
        ]);

        Assert.That(outcome, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
        Assert.That(gate.Requests.Select(r => r.TreeId), Is.All.EqualTo("orders"));
        Assert.That(state.State.Participants.Select(p => p.TreeId), Is.EqualTo(new[] { "orders" }));
    }

    [Test]
    public void CommitAsync_authorization_skips_a_leg_with_an_empty_tree_id()
    {
        var gate = new RecordingGate();
        var (grain, _, _, _) = CreateGrain(["orders"], activationServices: Services(gate));

        var ex = Assert.ThrowsAsync<ArgumentException>(() => grain.CommitAsync([
            Batch(string.Empty, Rows(("order:1", "A"))),
        ]));

        Assert.That(ex!.Message, Does.Contain("null or empty tree id"));
        Assert.That(gate.Requests, Is.Empty);
    }

    [Test]
    public void CommitAsync_rejects_a_leg_with_a_null_entries_list()
    {
        var gate = new RecordingGate();
        var (grain, _, _, _) = CreateGrain(["orders"], activationServices: Services(gate));

        var ex = Assert.ThrowsAsync<ArgumentException>(() => grain.CommitAsync([
            Batch("orders", null!),
        ]));

        Assert.That(ex!.Message, Does.Contain("null entries list"));
        Assert.That(gate.Requests, Is.Empty);
    }

    [Test]
    public void CommitAsync_rejects_a_null_value_on_a_non_delete_entry()
    {
        var (grain, _, _, _) = CreateGrain(["orders"]);
        var entries = new List<KeyValuePair<string, byte[]>> { new("order:1", null!) };

        var ex = Assert.ThrowsAsync<ArgumentException>(() => grain.CommitAsync([Batch("orders", entries)]));

        Assert.That(ex!.Message, Does.Contain("null value for key 'order:1'"));
    }

    [Test]
    public async Task CommitAsync_accepts_a_null_value_on_a_delete_entry()
    {
        // The counterpart guarantee: a tombstone legitimately carries no value,
        // and is normalised to an empty (non-null) buffer for the fan-out.
        var (grain, state, _, _) = CreateGrain(["orders"]);
        var entries = new List<KeyValuePair<string, byte[]>> { new("gone:1", null!) };

        var outcome = await grain.CommitAsync([Batch("orders", entries, deletes: [true])]);

        Assert.That(outcome, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
        var participant = state.State.Participants.Single();
        Assert.That(participant.Entries[0].Value, Is.Empty);
        Assert.That(participant.EntryDeletes, Is.EqualTo(new[] { true }));
    }

    // ---- Per-leg schema enforcement / value transformation -----------------

    [Test]
    public async Task CommitAsync_offers_every_whole_value_upsert_to_the_interceptor()
    {
        var interceptor = new RecordingInterceptor();
        var (grain, _, _, _) = CreateGrain(
            ["inventory", "orders"], activationServices: Services(interceptor: interceptor));

        await grain.CommitAsync([
            Batch("orders", Rows(("order:1", "A"))),
            Batch("inventory", Rows(("sku:1", "C"))),
        ]);

        Assert.That(interceptor.Seen, Is.EquivalentTo(new[]
        {
            ("orders", "order:1"),
            ("inventory", "sku:1"),
        }));
    }

    [Test]
    public async Task CommitAsync_does_not_offer_deletes_or_deltas_to_the_interceptor()
    {
        // Deletes carry no value and CRDT-delta legs are a delta apply, not a
        // whole-value write; the single-tree choke point skips both, so the
        // cross-tree one must too.
        var interceptor = new RecordingInterceptor();
        var (grain, _, _, _) = CreateGrain(["orders"], activationServices: Services(interceptor: interceptor));

        await grain.CommitAsync([
            Batch(
                "orders",
                Rows(("plain:1", "A"), ("delta:1", "B"), ("gone:1", "")),
                deltas: [null, [7, 7], null],
                deletes: [false, false, true]),
        ]);

        Assert.That(interceptor.Seen, Is.EqualTo(new[] { ("orders", "plain:1") }));
    }

    [Test]
    public async Task CommitAsync_leaves_a_leg_with_no_whole_value_upserts_untouched()
    {
        // A leg made entirely of deletes yields no writes at all, so the
        // interceptor is never called for it and the caller's batch is preserved.
        var interceptor = new RecordingInterceptor();
        var (grain, state, _, _) = CreateGrain(["orders"], activationServices: Services(interceptor: interceptor));

        await grain.CommitAsync([
            Batch("orders", Rows(("gone:1", ""), ("gone:2", "")), deletes: [true, true]),
        ]);

        Assert.That(interceptor.Seen, Is.Empty);
        Assert.That(state.State.Participants.Single().EntryDeletes, Is.EqualTo(new[] { true, true }));
    }

    [Test]
    public async Task CommitAsync_preserves_the_caller_batch_when_no_value_is_transformed()
    {
        var interceptor = new RecordingInterceptor(_ => LatticeWriteDecision.Accept());
        var (grain, state, _, _) = CreateGrain(["orders"], activationServices: Services(interceptor: interceptor));

        await grain.CommitAsync([Batch("orders", Rows(("order:1", "A")))]);

        Assert.That(
            state.State.Participants.Single().Entries[0].Value,
            Is.EqualTo(System.Text.Encoding.UTF8.GetBytes("A")));
    }

    [Test]
    public async Task CommitAsync_stages_the_transformed_value_when_the_interceptor_rewrites_it()
    {
        var stamped = new byte[] { 9, 9, 9 };
        var interceptor = new RecordingInterceptor(_ => LatticeWriteDecision.AcceptTransformed(stamped));
        var (grain, state, _, _) = CreateGrain(["orders"], activationServices: Services(interceptor: interceptor));

        await grain.CommitAsync([Batch("orders", Rows(("order:1", "A")))]);

        Assert.That(state.State.Participants.Single().Entries[0].Value, Is.EqualTo(stamped),
            "the substituted value must be what is staged, not the caller's original");
    }

    [Test]
    public async Task CommitAsync_rebuilds_a_transformed_leg_at_the_original_entry_positions()
    {
        // Only the plain upsert is intercepted; the delete and the delta must
        // stay exactly where they were, with their channels intact.
        var stamped = new byte[] { 4, 2 };
        var interceptor = new RecordingInterceptor(r => r.Key == "plain:1"
            ? LatticeWriteDecision.AcceptTransformed(stamped)
            : LatticeWriteDecision.Accept());
        var (grain, state, _, _) = CreateGrain(["orders"], activationServices: Services(interceptor: interceptor));

        await grain.CommitAsync([
            Batch(
                "orders",
                Rows(("delta:1", "D"), ("plain:1", "A"), ("gone:1", "")),
                deltas: [[7], null, null],
                deletes: [false, false, true]),
        ]);

        var participant = state.State.Participants.Single();
        Assert.Multiple(() =>
        {
            Assert.That(participant.Entries.Select(e => e.Key), Is.EqualTo(new[] { "delta:1", "plain:1", "gone:1" }));
            Assert.That(participant.Entries[1].Value, Is.EqualTo(stamped));
            Assert.That(participant.EntryDeltas![0], Is.EqualTo(new byte[] { 7 }));
            Assert.That(participant.EntryDeletes, Is.EqualTo(new[] { false, false, true }));
        });
    }

    [Test]
    public async Task CommitAsync_copies_the_untransformed_prefix_when_a_later_leg_is_transformed()
    {
        // The rewritten list is allocated lazily at the FIRST transformed leg, so
        // every earlier leg has to be copied across verbatim. This pins that
        // prefix copy: leg 0 is untouched, leg 1 is rewritten, and both survive.
        var stamped = new byte[] { 5, 5 };
        var interceptor = new RecordingInterceptor(r => r.TreeId == "inventory"
            ? LatticeWriteDecision.AcceptTransformed(stamped)
            : LatticeWriteDecision.Accept());
        var (grain, state, _, _) = CreateGrain(
            ["inventory", "orders"], activationServices: Services(interceptor: interceptor));

        // Submission order matters here, not the sorted persisted order: "orders"
        // is leg 0 (never transformed) and "inventory" is leg 1 (transformed).
        await grain.CommitAsync([
            Batch("orders", Rows(("order:1", "A"))),
            Batch("inventory", Rows(("sku:1", "C"))),
        ]);

        var byTree = state.State.Participants.ToDictionary(p => p.TreeId, StringComparer.Ordinal);
        Assert.Multiple(() =>
        {
            Assert.That(byTree["orders"].Entries[0].Value, Is.EqualTo(System.Text.Encoding.UTF8.GetBytes("A")),
                "the untransformed prefix leg must survive the rebuild verbatim");
            Assert.That(byTree["inventory"].Entries[0].Value, Is.EqualTo(stamped));
        });
    }

    [Test]
    public async Task CommitAsync_carries_a_skipped_leg_across_a_rebuild()
    {
        // A malformed / empty leg sitting AFTER the first transformed leg must
        // still be carried into the rebuilt list, or it would silently vanish.
        var stamped = new byte[] { 3 };
        var interceptor = new RecordingInterceptor(_ => LatticeWriteDecision.AcceptTransformed(stamped));
        var (grain, state, _, _) = CreateGrain(
            ["alpha", "orders"], activationServices: Services(interceptor: interceptor));

        var outcome = await grain.CommitAsync([
            Batch("alpha", Rows(("a:1", "A"))),
            Batch("empty", []),
            Batch("orders", Rows(("gone:1", ""))),
        ]);

        Assert.That(outcome, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
        Assert.That(
            state.State.Participants.Select(p => p.TreeId),
            Is.EqualTo(new[] { "alpha", "orders" }),
            "the empty leg is dropped, but the leg after it must survive the rebuild");
    }

    [Test]
    public async Task CommitAsync_carries_an_all_delete_leg_across_a_rebuild()
    {
        // A leg with no whole-value upserts sitting AFTER the first transformed
        // leg must be copied into the rebuilt list. The transformed-first
        // ordering is what makes this distinct from the no-rebuild case above:
        // there the carry is a no-op, here it is load-bearing.
        var stamped = new byte[] { 6 };
        var interceptor = new RecordingInterceptor(r => r.TreeId == "alpha"
            ? LatticeWriteDecision.AcceptTransformed(stamped)
            : LatticeWriteDecision.Accept());
        var (grain, state, _, _) = CreateGrain(
            ["alpha", "orders"], activationServices: Services(interceptor: interceptor));

        await grain.CommitAsync([
            Batch("alpha", Rows(("a:1", "A"))),
            Batch("orders", Rows(("gone:1", "")), deletes: [true]),
        ]);

        var byTree = state.State.Participants.ToDictionary(p => p.TreeId, StringComparer.Ordinal);
        Assert.Multiple(() =>
        {
            Assert.That(byTree["alpha"].Entries[0].Value, Is.EqualTo(stamped));
            Assert.That(byTree["orders"].EntryDeletes, Is.EqualTo(new[] { true }),
                "the all-delete leg must survive the rebuild with its delete channel intact");
        });
    }

    [Test]
    public async Task CommitAsync_carries_an_untransformed_leg_across_a_rebuild()
    {
        // Same shape for a leg the interceptor inspected but did not rewrite:
        // once a rebuild is under way, "no change" still has to be re-added.
        var stamped = new byte[] { 8 };
        var interceptor = new RecordingInterceptor(r => r.TreeId == "alpha"
            ? LatticeWriteDecision.AcceptTransformed(stamped)
            : LatticeWriteDecision.Accept());
        var (grain, state, _, _) = CreateGrain(
            ["alpha", "orders"], activationServices: Services(interceptor: interceptor));

        await grain.CommitAsync([
            Batch("alpha", Rows(("a:1", "A"))),
            Batch("orders", Rows(("order:1", "B"))),
        ]);

        var byTree = state.State.Participants.ToDictionary(p => p.TreeId, StringComparer.Ordinal);
        Assert.Multiple(() =>
        {
            Assert.That(byTree["alpha"].Entries[0].Value, Is.EqualTo(stamped));
            Assert.That(byTree["orders"].Entries[0].Value, Is.EqualTo(System.Text.Encoding.UTF8.GetBytes("B")),
                "an untransformed leg after a rebuild keeps the caller's original value");
        });
    }

    [Test]
    public void CommitAsync_rejected_leg_aborts_the_whole_commit_before_any_tree_is_mutated()
    {
        // atomic: true - a rejected entry throws rather than being dropped, so
        // no participant is ever prepared.
        var interceptor = new RecordingInterceptor(_ => LatticeWriteDecision.Reject("schema mismatch"));
        var (grain, state, _, participants) = CreateGrain(
            ["orders"], activationServices: Services(interceptor: interceptor));

        Assert.ThrowsAsync<LatticeWriteRejectedException>(() => grain.CommitAsync([
            Batch("orders", Rows(("order:1", "A"))),
        ]));

        Assert.That(state.State.Phase, Is.EqualTo(CrossTreeTxPhase.NotStarted));
        Assert.That(
            participants["orders"].ReceivedCalls().Any(c => c.GetMethodInfo().Name == "PrepareForCoordinatorAsync"),
            Is.False);
    }

    [Test]
    public async Task CommitAsync_runs_the_gate_and_the_interceptor_together()
    {
        // Both choke points are registered at once, which is the real deployed
        // shape: authorization runs first, then schema enforcement, and the
        // transformed value is what reaches the participants.
        var gate = new RecordingGate();
        var stamped = new byte[] { 1, 1 };
        var interceptor = new RecordingInterceptor(_ => LatticeWriteDecision.AcceptTransformed(stamped));
        var (grain, state, _, _) = CreateGrain(
            ["orders"], activationServices: Services(gate, interceptor));

        await grain.CommitAsync([Batch("orders", Rows(("order:1", "A")))]);

        Assert.Multiple(() =>
        {
            Assert.That(gate.Requests, Is.EqualTo(new[] { ("orders", "order:1", LatticeOperation.Write) }));
            Assert.That(interceptor.Seen, Is.EqualTo(new[] { ("orders", "order:1") }));
            Assert.That(state.State.Participants.Single().Entries[0].Value, Is.EqualTo(stamped));
        });
    }

    [Test]
    public async Task CommitAsync_skips_both_choke_points_on_a_system_origin_turn()
    {
        // The system-origin bypass is what keeps internal replication and
        // rebuild traffic zero-cost; neither the gate nor the interceptor may be
        // consulted inside that scope.
        var gate = new RecordingGate();
        var interceptor = new RecordingInterceptor();
        var (grain, _, _, _) = CreateGrain(["orders"], activationServices: Services(gate, interceptor));

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await grain.CommitAsync([Batch("orders", Rows(("order:1", "A")))]);
        }

        Assert.Multiple(() =>
        {
            Assert.That(gate.Requests, Is.Empty);
            Assert.That(interceptor.Seen, Is.Empty);
        });
    }
}
