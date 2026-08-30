namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="TenantGrantLifecycle"/>, the single source of truth
/// for the cross-tenant grant state machine: which state authorizes, which
/// transitions are legal, and how two concurrently-written states converge.
/// </summary>
/// <remarks>
/// The merge join is the load-bearing part. Two tenants act on a grant
/// independently, so a concurrent approve and revoke is a normal operating
/// condition rather than an edge case, and the join must resolve it to the
/// terminal, access-denying outcome regardless of which side's stamp happens to
/// be higher. Every case here is driven by exact enum values, so nothing depends
/// on threading, ordering, or the wall clock.
/// </remarks>
public sealed class TenantGrantLifecycleTests
{
    private static readonly TenantGrantState[] AllStates =
    [
        TenantGrantState.Pending,
        TenantGrantState.Active,
        TenantGrantState.Rejected,
        TenantGrantState.Revoked,
    ];

    // ---- Authorizes: the load-bearing gate ---------------------------------

    [Test]
    public void Authorizes_is_true_only_for_active()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenantGrantLifecycle.Authorizes(TenantGrantState.Active), Is.True);
            Assert.That(TenantGrantLifecycle.Authorizes(TenantGrantState.Pending), Is.False);
            Assert.That(TenantGrantLifecycle.Authorizes(TenantGrantState.Rejected), Is.False);
            Assert.That(TenantGrantLifecycle.Authorizes(TenantGrantState.Revoked), Is.False);
        });
    }

    [Test]
    public void Authorizes_is_false_for_an_unrecognised_state_from_a_newer_peer()
    {
        Assert.That(TenantGrantLifecycle.Authorizes((TenantGrantState)99), Is.False);
    }

    [Test]
    public void IsTerminal_is_true_only_for_rejected_and_revoked()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenantGrantLifecycle.IsTerminal(TenantGrantState.Rejected), Is.True);
            Assert.That(TenantGrantLifecycle.IsTerminal(TenantGrantState.Revoked), Is.True);
            Assert.That(TenantGrantLifecycle.IsTerminal(TenantGrantState.Pending), Is.False);
            Assert.That(TenantGrantLifecycle.IsTerminal(TenantGrantState.Active), Is.False);
        });
    }

    // ---- the legal-transition set -----------------------------------------

    [Test]
    public void IsLegalTransition_admits_exactly_approve_reject_and_revoke()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                TenantGrantLifecycle.IsLegalTransition(TenantGrantState.Pending, TenantGrantState.Active),
                Is.True,
                "approve");
            Assert.That(
                TenantGrantLifecycle.IsLegalTransition(TenantGrantState.Pending, TenantGrantState.Rejected),
                Is.True,
                "reject");
            Assert.That(
                TenantGrantLifecycle.IsLegalTransition(TenantGrantState.Active, TenantGrantState.Revoked),
                Is.True,
                "revoke");
        });
    }

    /// <summary>
    /// Every pair outside the three legal transitions is refused. Enumerated
    /// exhaustively rather than sampled, so a future state added without a
    /// transition rule cannot slip through untested.
    /// </summary>
    [Test]
    public void IsLegalTransition_refuses_every_other_pair()
    {
        var legal = new HashSet<(TenantGrantState, TenantGrantState)>
        {
            (TenantGrantState.Pending, TenantGrantState.Active),
            (TenantGrantState.Pending, TenantGrantState.Rejected),
            (TenantGrantState.Active, TenantGrantState.Revoked),
        };

        Assert.Multiple(() =>
        {
            foreach (var from in AllStates)
            {
                foreach (var to in AllStates)
                {
                    if (legal.Contains((from, to)))
                    {
                        continue;
                    }

                    Assert.That(
                        TenantGrantLifecycle.IsLegalTransition(from, to),
                        Is.False,
                        $"{from} -> {to} must be refused");
                }
            }
        });
    }

    [Test]
    public void IsLegalTransition_refuses_the_identity_pair_so_callers_treat_it_as_a_no_op()
    {
        Assert.Multiple(() =>
        {
            foreach (var state in AllStates)
            {
                Assert.That(TenantGrantLifecycle.IsLegalTransition(state, state), Is.False, $"{state} -> {state}");
            }
        });
    }

    // ---- offers ------------------------------------------------------------

    [Test]
    public void IsLegalOffer_admits_a_pending_or_terminally_closed_grant()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenantGrantLifecycle.IsLegalOffer(TenantGrantState.Pending), Is.True);
            Assert.That(TenantGrantLifecycle.IsLegalOffer(TenantGrantState.Rejected), Is.True);
            Assert.That(TenantGrantLifecycle.IsLegalOffer(TenantGrantState.Revoked), Is.True);
        });
    }

    [Test]
    public void IsLegalOffer_refuses_a_live_agreement_the_grantee_already_approved()
    {
        // Re-offering new terms over an active grant would let the granting tenant
        // redefine what the grantee approved, without the grantee approving again.
        Assert.That(TenantGrantLifecycle.IsLegalOffer(TenantGrantState.Active), Is.False);
    }

    // ---- the merge join: the full concurrent-pair table --------------------

    [Test]
    public void Join_resolves_concurrent_approve_and_revoke_to_revoked()
    {
        // The case the two-party design makes routine: an approve written on one
        // replica meeting a revoke written on the other. The terminal, denying
        // outcome must win regardless of argument order or stamp.
        Assert.Multiple(() =>
        {
            Assert.That(
                TenantGrantLifecycle.Join(TenantGrantState.Active, TenantGrantState.Revoked),
                Is.EqualTo(TenantGrantState.Revoked));
            Assert.That(
                TenantGrantLifecycle.Join(TenantGrantState.Revoked, TenantGrantState.Active),
                Is.EqualTo(TenantGrantState.Revoked));
        });
    }

    [Test]
    public void Join_resolves_concurrent_approve_and_reject_to_rejected()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                TenantGrantLifecycle.Join(TenantGrantState.Active, TenantGrantState.Rejected),
                Is.EqualTo(TenantGrantState.Rejected));
            Assert.That(
                TenantGrantLifecycle.Join(TenantGrantState.Rejected, TenantGrantState.Active),
                Is.EqualTo(TenantGrantState.Rejected));
        });
    }

    [Test]
    public void Join_lets_an_approval_beat_a_stale_pending()
    {
        // Approval is the one widening transition and must survive the merge, or
        // an approve could be silently lost to a replica that had not seen it.
        Assert.Multiple(() =>
        {
            Assert.That(
                TenantGrantLifecycle.Join(TenantGrantState.Pending, TenantGrantState.Active),
                Is.EqualTo(TenantGrantState.Active));
            Assert.That(
                TenantGrantLifecycle.Join(TenantGrantState.Active, TenantGrantState.Pending),
                Is.EqualTo(TenantGrantState.Active));
        });
    }

    [Test]
    public void Join_lets_a_terminal_state_beat_a_pending()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                TenantGrantLifecycle.Join(TenantGrantState.Pending, TenantGrantState.Rejected),
                Is.EqualTo(TenantGrantState.Rejected));
            Assert.That(
                TenantGrantLifecycle.Join(TenantGrantState.Pending, TenantGrantState.Revoked),
                Is.EqualTo(TenantGrantState.Revoked));
        });
    }

    [Test]
    public void Join_of_the_two_terminal_states_is_deterministic_and_denies()
    {
        var forward = TenantGrantLifecycle.Join(TenantGrantState.Rejected, TenantGrantState.Revoked);
        var reverse = TenantGrantLifecycle.Join(TenantGrantState.Revoked, TenantGrantState.Rejected);

        Assert.Multiple(() =>
        {
            Assert.That(forward, Is.EqualTo(reverse));
            Assert.That(TenantGrantLifecycle.Authorizes(forward), Is.False);
        });
    }

    /// <summary>
    /// The whole point of the join, stated as an invariant over every pair: a
    /// merge can never invent the one state that authorizes. It may only produce
    /// <see cref="TenantGrantState.Active"/> when a side actually approved, and
    /// never when either side closed the agreement. If this fails, convergence can
    /// widen access.
    /// </summary>
    [Test]
    public void Join_only_produces_active_when_a_side_approved_and_neither_side_closed()
    {
        Assert.Multiple(() =>
        {
            foreach (var left in AllStates)
            {
                foreach (var right in AllStates)
                {
                    if (TenantGrantLifecycle.Join(left, right) != TenantGrantState.Active)
                    {
                        continue;
                    }

                    Assert.That(
                        left is TenantGrantState.Active || right is TenantGrantState.Active,
                        Is.True,
                        $"join({left}, {right}) invented an Active neither side held");
                    Assert.That(
                        TenantGrantLifecycle.IsTerminal(left) || TenantGrantLifecycle.IsTerminal(right),
                        Is.False,
                        $"join({left}, {right}) widened to Active over a closed agreement");
                }
            }
        });
    }

    [Test]
    public void Join_never_loses_a_terminal_state()
    {
        Assert.Multiple(() =>
        {
            foreach (var left in AllStates)
            {
                foreach (var right in AllStates)
                {
                    if (!TenantGrantLifecycle.IsTerminal(left) && !TenantGrantLifecycle.IsTerminal(right))
                    {
                        continue;
                    }

                    Assert.That(
                        TenantGrantLifecycle.IsTerminal(TenantGrantLifecycle.Join(left, right)),
                        Is.True,
                        $"join({left}, {right}) dropped a terminal state");
                }
            }
        });
    }

    // ---- the join is a valid CRDT merge ------------------------------------

    [Test]
    public void Join_is_commutative_over_every_pair()
    {
        Assert.Multiple(() =>
        {
            foreach (var left in AllStates)
            {
                foreach (var right in AllStates)
                {
                    Assert.That(
                        TenantGrantLifecycle.Join(left, right),
                        Is.EqualTo(TenantGrantLifecycle.Join(right, left)),
                        $"({left}, {right})");
                }
            }
        });
    }

    [Test]
    public void Join_is_associative_over_every_triple()
    {
        Assert.Multiple(() =>
        {
            foreach (var a in AllStates)
            {
                foreach (var b in AllStates)
                {
                    foreach (var c in AllStates)
                    {
                        Assert.That(
                            TenantGrantLifecycle.Join(TenantGrantLifecycle.Join(a, b), c),
                            Is.EqualTo(TenantGrantLifecycle.Join(a, TenantGrantLifecycle.Join(b, c))),
                            $"({a}, {b}, {c})");
                    }
                }
            }
        });
    }

    [Test]
    public void Join_is_idempotent()
    {
        Assert.Multiple(() =>
        {
            foreach (var state in AllStates)
            {
                Assert.That(TenantGrantLifecycle.Join(state, state), Is.EqualTo(state));
            }
        });
    }

    [Test]
    public void Join_ranks_an_unrecognised_state_above_active_so_it_cannot_widen_access()
    {
        var unknown = (TenantGrantState)99;

        Assert.Multiple(() =>
        {
            Assert.That(TenantGrantLifecycle.Join(TenantGrantState.Active, unknown), Is.EqualTo(unknown));
            Assert.That(TenantGrantLifecycle.Join(unknown, TenantGrantState.Active), Is.EqualTo(unknown));
        });
    }

    [Test]
    public void Join_of_two_unrecognised_states_stays_commutative()
    {
        var first = (TenantGrantState)98;
        var second = (TenantGrantState)99;

        Assert.That(
            TenantGrantLifecycle.Join(first, second),
            Is.EqualTo(TenantGrantLifecycle.Join(second, first)));
    }
}
