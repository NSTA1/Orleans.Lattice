using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Lattice;
using Orleans.Lattice.Tenancy;
using static Orleans.Lattice.Api.TenantAdmin.Tests.TenantAdminTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests;

/// <summary>
/// Convergence tests for concurrent transitions written by the two parties to a
/// cross-tenant grant. Because both sides act independently by design, a
/// transition landing between one caller's read and its write is a <b>normal
/// operating condition</b> here rather than an edge case, so the pre-write
/// legality check can never be trusted alone.
/// </summary>
/// <remarks>
/// <para>
/// Each case is driven by <see cref="RacingTenantRegistry"/>, which commits a
/// competing write <em>inside</em> the read-to-write window and merges on put
/// exactly as the real registry does, and by <see cref="ScriptedClock"/>, which
/// hands out explicit stamps. There are no threads, no delays, and no wall-clock
/// reads, so every outcome below is a fixed function of the stamps and the merge
/// rule.
/// </para>
/// <para>
/// The stamps are chosen so the <em>losing</em> intent always carries the
/// <em>higher</em> stamp. A plain last-writer-wins merge would therefore reinstate
/// access in each case; only the restrictive state join produces the asserted
/// outcome.
/// </para>
/// </remarks>
public sealed partial class LatticeTenantGrantAdminTests
{
    /// <summary>A clock handing out a scripted sequence of explicit stamps.</summary>
    private sealed class ScriptedClock : ITenantAdminClock
    {
        private readonly long[] _ticks;
        private int _index;

        public ScriptedClock(params long[] ticks) => _ticks = ticks;

        public HybridLogicalClock Next() =>
            new() { WallClockTicks = _ticks[Math.Min(_index++, _ticks.Length - 1)] };
    }

    /// <summary>
    /// The real registry's read-merge-write shape, in the form N1's
    /// <c>MergingTenantRegistry</c> proved out: a read hands out a
    /// <see cref="TenantRecord.Clone"/> (so a caller mutates its own copy), and a
    /// put folds the caller's record into the stored one with the CRDT join and
    /// returns the committed result. The competing write is applied <b>once,
    /// immediately before the first merge</b> - the exact window the real
    /// registry's optimistic read-merge-write leaves open - with explicit stamps,
    /// so there are no threads, no clock reads and no ordering assumptions.
    /// </summary>
    /// <remarks>
    /// This holds a <em>set</em> of records rather than N1's single one, because a
    /// cross-tenant grant necessarily involves two tenants: the authority for
    /// approve and reject is read from the grantee's record while the grant itself
    /// lives on the granter's, and the inbox projection enumerates the registry.
    /// N1's own double could not serve those reads.
    /// </remarks>
    private sealed class MergingTenantRegistry : ITenantRegistry
    {
        private readonly Dictionary<string, TenantRecord> _stored = new(StringComparer.Ordinal);
        private Action<TenantRecord>? _competingWrite;
        private string? _competingWriteTenantId;

        public int Puts { get; private set; }

        public void Seed(TenantRecord record) => _stored[record.Id.Value] = record;

        public TenantRecord? Stored(string tenantId) => _stored.GetValueOrDefault(tenantId);

        /// <summary>
        /// Arms a competing write against <paramref name="tenantId"/>'s stored
        /// record, applied once immediately before that record's first merge.
        /// </summary>
        public void RaceOnFirstWriteTo(string tenantId, Action<TenantRecord> competingWrite)
        {
            _competingWriteTenantId = tenantId;
            _competingWrite = competingWrite;
        }

        public Task<TenantRecord?> GetAsync(TenantId tenant, CancellationToken cancellationToken = default)
            => Task.FromResult(_stored.TryGetValue(tenant.Value, out var record) ? record.Clone() : null);

        public Task<bool> ExistsAsync(TenantId tenant, CancellationToken cancellationToken = default)
            => Task.FromResult(_stored.ContainsKey(tenant.Value));

#pragma warning disable CS1998 // a synchronous fake needs no await
        public async IAsyncEnumerable<TenantRecord> ListAsync(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach (var record in _stored.Values.ToArray())
            {
                yield return record;
            }
        }
#pragma warning restore CS1998

        public Task<TenantRecord> PutAsync(TenantRecord record, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(record);
            Puts++;

            if (!_stored.TryGetValue(record.Id.Value, out var stored))
            {
                _stored[record.Id.Value] = record;
                return Task.FromResult(record);
            }

            if (_competingWrite is { } write
                && string.Equals(_competingWriteTenantId, record.Id.Value, StringComparison.Ordinal))
            {
                _competingWrite = null;
                _competingWriteTenantId = null;
                write(stored);
            }

            return Task.FromResult(stored.MergeFrom(record));
        }

        public Task<bool> DeleteAsync(TenantId tenant, CancellationToken cancellationToken = default) =>
            Task.FromResult(_stored.Remove(tenant.Value));
    }

    private static readonly string GrantId = CrossTenantGrant
        .Create(Grantee, TenantGranteeKind.Tenant, Scope, TenantGrantOperations.None).GrantId;

    private static MergingTenantRegistry RacingRegistry()
    {
        var registry = new MergingTenantRegistry();
        registry.Seed(Tenant(Granter, "alice@acme.example"));
        registry.Seed(Tenant(Grantee, "bob@beta.example"));
        return registry;
    }

    private static LatticeTenantGrantAdmin RacingAdmin(MergingTenantRegistry registry, ITenantAdminClock clock) =>
        new(
            registry,
            new TenantRegionResidencyAuthorizer(
                new FixedGate(allow: true), registry, new FixedMembershipContext(new LatticeSubject("op"))),
            clock,
            Options.Create(new ClusterOptions { ClusterId = "region-a" }));

    private static void SeedPending(MergingTenantRegistry registry, TenantGrantOperations operations) =>
        registry.Stored(Granter)!.OfferGrant(
            CrossTenantGrant.Create(Grantee, TenantGranteeKind.Tenant, Scope, operations), Stamp(10), "granter");

    private static TenantGrantState CommittedState(MergingTenantRegistry registry) =>
        registry.Stored(Granter)!.TryGetGrant(GrantId, out var grant)
            ? grant.State
            : throw new InvalidOperationException("no live grant on the granting tenant's record");

    /// <summary>
    /// The defining race of the two-party design. The grantee reads a pending
    /// grant and approves it; in the read-to-write window the granting tenant sees
    /// the approval propagate and revokes. The approval carries the far higher
    /// stamp, so plain last-writer-wins would reinstate access.
    /// </summary>
    [Test]
    public void A_concurrent_revoke_beats_an_approve_that_carries_a_higher_stamp()
    {
        var registry = RacingRegistry();
        SeedPending(registry, TenantGrantOperations.ReadWrite);
        var admin = RacingAdmin(registry, new ScriptedClock(9_000));

        registry.RaceOnFirstWriteTo(Granter, stored =>
        {
            stored.TransitionGrant(GrantId, TenantGrantState.Active, Stamp(20), "grantee");
            stored.TransitionGrant(GrantId, TenantGrantState.Revoked, Stamp(30), "granter");
        });

        Assert.That(
            async () => await admin.ApproveGrantAsync(Granter, Grantee, Scope),
            Throws.TypeOf<TenantGrantTransitionException>()
                .With.Property(nameof(TenantGrantTransitionException.CurrentState))
                .EqualTo(TenantGrantLifecycleState.Revoked),
            "the approver must be told plainly that its transition did not take effect");

        Assert.Multiple(() =>
        {
            Assert.That(
                CommittedState(registry),
                Is.EqualTo(TenantGrantState.Revoked),
                "convergence must never widen access back to active");
            Assert.That(TenantGrantLifecycle.Authorizes(CommittedState(registry)), Is.False);
        });
    }

    [Test]
    public void A_concurrent_reject_beats_an_approve_that_carries_a_higher_stamp()
    {
        var registry = RacingRegistry();
        SeedPending(registry, TenantGrantOperations.Read);
        var admin = RacingAdmin(registry, new ScriptedClock(9_000));

        registry.RaceOnFirstWriteTo(Granter, stored =>
            stored.TransitionGrant(GrantId, TenantGrantState.Rejected, Stamp(20), "grantee-b"));

        Assert.That(
            async () => await admin.ApproveGrantAsync(Granter, Grantee, Scope),
            Throws.TypeOf<TenantGrantTransitionException>()
                .With.Property(nameof(TenantGrantTransitionException.CurrentState))
                .EqualTo(TenantGrantLifecycleState.Rejected));

        Assert.That(CommittedState(registry), Is.EqualTo(TenantGrantState.Rejected));
    }

    /// <summary>
    /// The mirror of the first case: the party whose intent the join keeps is
    /// <em>not</em> refused. Both racing callers being refused would be a needless
    /// availability loss, and would hide which intent actually won.
    /// </summary>
    [Test]
    public async Task The_party_whose_transition_wins_the_join_still_succeeds()
    {
        var registry = RacingRegistry();
        SeedPending(registry, TenantGrantOperations.Read);
        var admin = RacingAdmin(registry, new ScriptedClock(20));

        // A competing approve lands in the window; this caller is rejecting, and
        // reject is the more restrictive of the two, so its intent survives.
        registry.RaceOnFirstWriteTo(Granter, stored =>
            stored.TransitionGrant(GrantId, TenantGrantState.Active, Stamp(9_000), "grantee-b"));

        var result = await admin.RejectGrantAsync(Granter, Grantee, Scope);

        Assert.Multiple(() =>
        {
            Assert.That(result.Changed, Is.True);
            Assert.That(result.Grant.State, Is.EqualTo(TenantGrantLifecycleState.Rejected));
            Assert.That(CommittedState(registry), Is.EqualTo(TenantGrantState.Rejected));
        });
    }

    /// <summary>
    /// A concurrent revoke that lands while the granting tenant is amending its
    /// offer must not be resurrected into a live agreement.
    /// </summary>
    [Test]
    public void A_concurrent_approve_cannot_activate_terms_it_never_saw()
    {
        var registry = RacingRegistry();
        SeedPending(registry, TenantGrantOperations.Read);
        var admin = RacingAdmin(registry, new ScriptedClock(20));

        // The grantee approves the read-only terms while the granting tenant is
        // amending the offer to read-write. The approval must not attach to the
        // new terms, so the amended offer stays pending.
        registry.RaceOnFirstWriteTo(Granter, stored =>
            stored.TransitionGrant(GrantId, TenantGrantState.Active, Stamp(9_000), "grantee"));

        Assert.That(
            async () => await admin.OfferGrantAsync(Granter, Grantee, Scope, TenantGrantAccess.ReadWrite),
            Throws.Nothing);

        registry.Stored(Granter)!.TryGetGrant(GrantId, out var committed);
        Assert.Multiple(() =>
        {
            Assert.That(
                committed.State,
                Is.EqualTo(TenantGrantState.Pending),
                "an approval of superseded terms must not activate the amended ones");
            Assert.That(committed.Operations, Is.EqualTo(TenantGrantOperations.ReadWrite));
        });
    }

    /// <summary>
    /// The secondary finding: the response must be built from the registry's
    /// committed join, not from the caller's pre-merge local view, or a concurrent
    /// change from another replica is silently missing from what the caller is
    /// told.
    /// </summary>
    [Test]
    public async Task The_result_reports_the_merged_grant_not_the_callers_pre_merge_view()
    {
        var registry = RacingRegistry();
        var admin = RacingAdmin(registry, new ScriptedClock(20));

        // A second admin of the granting tenant offers wider terms in the window,
        // at a higher stamp, so the merge keeps its payload.
        registry.RaceOnFirstWriteTo(Granter, stored => stored.OfferGrant(
            CrossTenantGrant.Create(
                Grantee, TenantGranteeKind.Tenant, Scope, TenantGrantOperations.ReadWrite),
            Stamp(9_000),
            "alice2@acme.example"));

        var result = await admin.OfferGrantAsync(Granter, Grantee, Scope, TenantGrantAccess.Read);

        Assert.That(
            result.Grant.Operations,
            Is.EqualTo(TenantGrantAccess.ReadWrite),
            "the caller must be told the converged terms, not the ones it optimistically wrote");
    }

    [Test]
    public void A_grant_hard_removed_in_the_read_to_write_window_reports_not_found()
    {
        var registry = RacingRegistry();
        SeedPending(registry, TenantGrantOperations.Read);
        var admin = RacingAdmin(registry, new ScriptedClock(20));

        registry.RaceOnFirstWriteTo(Granter, stored =>
            stored.RemoveGrant(GrantId, Stamp(9_000), "granter"));

        Assert.That(
            async () => await admin.ApproveGrantAsync(Granter, Grantee, Scope),
            Throws.TypeOf<TenantGrantNotFoundException>());
    }

    /// <summary>
    /// The escalation path a reviewer found in the first cut of the merge: the
    /// granting tenant's admin offers <em>wider</em> terms while the grant is
    /// already approved on the stored record at the <b>same</b> generation, which
    /// happens whenever its read predates the slot's creation. Publishing the
    /// pending offer's terms under the approved slot's state would bind the
    /// grantee to terms it never approved - and the registry write happens before
    /// the post-merge refusal, so the refusal alone would not contain it.
    /// </summary>
    [Test]
    public void An_offer_racing_an_already_approved_grant_cannot_widen_it()
    {
        var registry = RacingRegistry();
        var admin = RacingAdmin(registry, new ScriptedClock(9_000));

        // The offer's read sees no slot at all, so it takes the create path and
        // writes at generation zero - the same generation the stored, already
        // approved grant sits at.
        registry.RaceOnFirstWriteTo(Granter, stored =>
        {
            stored.OfferGrant(
                CrossTenantGrant.Create(
                    Grantee, TenantGranteeKind.Tenant, Scope, TenantGrantOperations.Read),
                Stamp(10),
                "granter");
            stored.TransitionGrant(GrantId, TenantGrantState.Active, Stamp(20), "grantee");
        });

        try
        {
            _ = admin.OfferGrantAsync(Granter, Grantee, Scope, TenantGrantAccess.ReadWrite)
                .GetAwaiter().GetResult();
        }
        catch (TenantGrantTransitionException)
        {
            // Expected: the committed state is not the Pending this call asked for.
        }

        registry.Stored(Granter)!.TryGetGrant(GrantId, out var committed);
        Assert.Multiple(() =>
        {
            Assert.That(
                committed.Operations,
                Is.EqualTo(TenantGrantOperations.Read),
                "the grantee approved read-only terms and must not be bound to wider ones");
            Assert.That(
                committed.State is TenantGrantState.Active
                    && committed.Operations == TenantGrantOperations.ReadWrite,
                Is.False,
                "an unapproved widening became a live cross-tenant grant");
        });
    }

    /// <summary>
    /// The refusal terminates: a caller that retries after losing the race is
    /// stopped by the pre-write guard, before it writes anything.
    /// </summary>
    [Test]
    public void A_retry_after_losing_the_race_is_refused_without_a_further_write()
    {
        var registry = RacingRegistry();
        SeedPending(registry, TenantGrantOperations.Read);
        var admin = RacingAdmin(registry, new ScriptedClock(9_000));

        registry.RaceOnFirstWriteTo(Granter, stored =>
        {
            stored.TransitionGrant(GrantId, TenantGrantState.Active, Stamp(20), "grantee");
            stored.TransitionGrant(GrantId, TenantGrantState.Revoked, Stamp(30), "granter");
        });

        Assert.That(
            async () => await admin.ApproveGrantAsync(Granter, Grantee, Scope),
            Throws.TypeOf<TenantGrantTransitionException>());

        var putsAfterRace = registry.Puts;

        Assert.That(
            async () => await admin.ApproveGrantAsync(Granter, Grantee, Scope),
            Throws.TypeOf<TenantGrantTransitionException>());

        Assert.That(
            registry.Puts,
            Is.EqualTo(putsAfterRace),
            "the retry is stopped by the pre-write guard, so it cannot loop writing");
    }
}
