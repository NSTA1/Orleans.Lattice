using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Coverage of issue #5791: a shadow build refused because another alias operation
/// holds the target tree's reservation is a transient refusal ("retry after it
/// completes"), not a build failure. A replication re-seed holds that reservation
/// while its bootstrap copy builds, so a coordinated restore that prepared during one
/// aborted on every cluster after three back-to-back attempts in about 50 ms (the
/// single-tree path) or at once (the set path). The participant now waits the
/// reservation out on a backoff, bounded by a ceiling, without spending the
/// build-attempt budget; past the ceiling it aborts at once.
/// </summary>
public partial class RestoreParticipantTests
{
    private static LatticeAliasOperationInProgressException Reserved(string tree = TargetTree) =>
        new(tree, "resize:bootstrap-copy");

    private static RestoreParticipant WaitingParticipant(
        ILatticeCoordinatedRestoreEngine engine, TimeSpan ceiling, ILatticeBackupSetResolver? setResolver = null) =>
        new(
            engine,
            restoreService: null,
            Capacity(true),
            FactoryFor(Substitute.For<ISagaWriteFenceGrain>()),
            NullLogger<RestoreParticipant>.Instance,
            setResolver)
        {
            AliasWaitBackoff = [TimeSpan.FromMilliseconds(1)],
            AliasWaitCeiling = ceiling,
        };

    [Test]
    public async Task PrepareAsync_waits_out_an_alias_reservation_beyond_the_build_attempt_budget_and_votes_commit()
    {
        // More refusals than the build-attempt budget (3), so a participant that
        // spent that budget on them would vote abort.
        var engine = HealthyEngine();
        var refusals = 0;
        engine.BuildShadowAsync(Arg.Any<LatticeRestoreRequest>(), Arg.Any<CancellationToken>())
            .Returns(ci => ++refusals <= 4
                ? Task.FromException<LatticeRestoreResult>(Reserved())
                : Task.FromResult(ResultFor(TargetTree)));
        var participant = WaitingParticipant(engine, TimeSpan.FromMinutes(1));

        var result = await participant.PrepareAsync(Request());

        Assert.That(result.Vote, Is.EqualTo(SagaVote.Commit),
            "a build refused while another alias operation holds the tree's reservation must wait for it, "
            + "not count the refusal as a failed build: " + result.Detail);
        await engine.Received(5).BuildShadowAsync(Arg.Any<LatticeRestoreRequest>(), Arg.Any<CancellationToken>());
        await engine.DidNotReceive().DeleteShadowAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task PrepareAsync_set_member_waits_out_an_alias_reservation_and_votes_commit()
    {
        var engine = HealthyEngine();
        var refusedOnce = false;
        engine.BuildShadowAsync(Arg.Any<LatticeRestoreRequest>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                var tree = TreeOf(ci, TreeA);
                if (tree == TreeB && !refusedOnce)
                {
                    refusedOnce = true;
                    return Task.FromException<LatticeRestoreResult>(Reserved(TreeB));
                }

                return Task.FromResult(ResultFor(tree));
            });
        var participant = WaitingParticipant(engine, TimeSpan.FromMinutes(1), TwoMemberResolver());

        var result = await participant.PrepareAsync(SetRequestFor(SetId));

        Assert.That(result.Vote, Is.EqualTo(SagaVote.Commit),
            "a set member refused by another alias operation must wait for it, not abort the whole set: " + result.Detail);
        Assert.That(refusedOnce, Is.True, "PRECONDITION: the member's first build was refused");
        await engine.Received(3).BuildShadowAsync(Arg.Any<LatticeRestoreRequest>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task PrepareAsync_alias_reservation_held_past_the_ceiling_aborts_without_respending_the_budget()
    {
        // A zero ceiling admits no wait, so the first refusal is final. It must abort
        // and clean up at once: the build-attempt budget is for failed builds, and
        // re-entering the wait on each of its attempts would multiply the ceiling.
        var engine = HealthyEngine();
        engine.BuildShadowAsync(Arg.Any<LatticeRestoreRequest>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(Reserved());
        var participant = WaitingParticipant(engine, TimeSpan.Zero);

        var result = await participant.PrepareAsync(Request());

        Assert.That(result.Vote, Is.EqualTo(SagaVote.Abort));
        Assert.That(result.Detail, Does.Contain("Shadow build failed"));
        await engine.Received(1).BuildShadowAsync(Arg.Any<LatticeRestoreRequest>(), Arg.Any<CancellationToken>());
        await engine.Received(1).DeleteShadowAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task PrepareAsync_set_members_share_one_alias_wait_budget_and_abort_at_its_ceiling()
    {
        // Each member is refused for longer than half the ceiling. Waited out member
        // by member, each would fit under the ceiling and the set would commit, so a
        // set of refused members could wait the ceiling once per member and outlive the
        // coordinator's prepare deadline. One budget for the whole prepare aborts it
        // while the second member waits.
        var ceiling = TimeSpan.FromSeconds(2);
        var refusedFor = TimeSpan.FromSeconds(1.1);
        var firstRefusal = new Dictionary<string, System.Diagnostics.Stopwatch>(StringComparer.Ordinal);
        var engine = HealthyEngine();
        engine.BuildShadowAsync(Arg.Any<LatticeRestoreRequest>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                var tree = TreeOf(ci, TreeA);
                if (!firstRefusal.TryGetValue(tree, out var clock))
                {
                    firstRefusal[tree] = clock = System.Diagnostics.Stopwatch.StartNew();
                }

                return clock.Elapsed < refusedFor
                    ? Task.FromException<LatticeRestoreResult>(Reserved(tree))
                    : Task.FromResult(ResultFor(tree));
            });
        var participant = new RestoreParticipant(
            engine,
            restoreService: null,
            Capacity(true),
            FactoryFor(Substitute.For<ISagaWriteFenceGrain>()),
            NullLogger<RestoreParticipant>.Instance,
            TwoMemberResolver())
        {
            AliasWaitBackoff = [TimeSpan.FromMilliseconds(50)],
            AliasWaitCeiling = ceiling,
        };

        var result = await participant.PrepareAsync(SetRequestFor(SetId));

        Assert.That(firstRefusal.Keys, Is.EquivalentTo(new[] { TreeA, TreeB }),
            "PRECONDITION: the first member waited its refusal out and the second member was reached");
        Assert.That(result.Vote, Is.EqualTo(SagaVote.Abort),
            "the members' waits together pass the one ceiling the prepare is allowed, so it must abort");
        Assert.That(result.Detail, Does.Contain("build failed"));
        Assert.That(participant.TryGetLocalSetResult(SetId, out _), Is.False, "an aborted set caches no group");
    }

    [Test]
    public void PrepareAsync_cancelled_while_waiting_for_an_alias_reservation_rethrows_cancellation()
    {
        var engine = HealthyEngine();
        engine.BuildShadowAsync(Arg.Any<LatticeRestoreRequest>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(Reserved());
        var participant = WaitingParticipant(engine, TimeSpan.FromMinutes(1));
        using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));

        Assert.That(
            async () => await participant.PrepareAsync(Request(), cancellation.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }
}
