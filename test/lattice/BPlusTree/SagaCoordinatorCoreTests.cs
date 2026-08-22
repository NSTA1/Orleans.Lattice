using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Fast, dependency-free unit tests for <see cref="SagaCoordinatorCore"/> - the
/// shared correctness core the production atomic-write saga coordinator
/// (<c>AtomicWriteGrain</c>) and the Coyote saga model both execute to decide
/// commit vs abort. These pin the exact transition and decision truth table so a
/// change to the rule is caught here (and by the Coyote model) rather than only
/// by a slow atomic-commit chaos run.
/// </summary>
[TestFixture]
public sealed class SagaCoordinatorCoreTests
{
    private static SagaDecision Decide(params SagaParticipantOutcome[] outcomes) =>
        SagaCoordinatorCore.Decide(outcomes);

    [Test]
    public void OnParticipantResult_records_the_vote_in_the_named_slot()
    {
        var outcomes = new SagaParticipantOutcome[3];

        SagaCoordinatorCore.OnParticipantResult(outcomes, 1, SagaParticipantOutcome.PreparedAck);

        Assert.Multiple(() =>
        {
            Assert.That(outcomes[0], Is.EqualTo(SagaParticipantOutcome.Pending));
            Assert.That(outcomes[1], Is.EqualTo(SagaParticipantOutcome.PreparedAck));
            Assert.That(outcomes[2], Is.EqualTo(SagaParticipantOutcome.Pending));
        });
    }

    [Test]
    public void OnParticipantResult_is_idempotent_on_a_repeated_identical_vote()
    {
        var outcomes = new SagaParticipantOutcome[2];

        SagaCoordinatorCore.OnParticipantResult(outcomes, 0, SagaParticipantOutcome.PreparedNack);
        SagaCoordinatorCore.OnParticipantResult(outcomes, 0, SagaParticipantOutcome.PreparedNack);

        Assert.That(outcomes[0], Is.EqualTo(SagaParticipantOutcome.PreparedNack));
    }

    [Test]
    public void OnParticipantResult_last_writer_overwrites_the_slot()
    {
        var outcomes = new SagaParticipantOutcome[1];

        SagaCoordinatorCore.OnParticipantResult(outcomes, 0, SagaParticipantOutcome.Pending);
        SagaCoordinatorCore.OnParticipantResult(outcomes, 0, SagaParticipantOutcome.PreparedAck);

        Assert.That(outcomes[0], Is.EqualTo(SagaParticipantOutcome.PreparedAck));
    }

    [Test]
    public void OnParticipantResult_negative_index_throws()
    {
        Assert.That(
            () =>
            {
                var outcomes = new SagaParticipantOutcome[2];
                SagaCoordinatorCore.OnParticipantResult(outcomes, -1, SagaParticipantOutcome.PreparedAck);
            },
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void OnParticipantResult_index_at_or_past_length_throws()
    {
        Assert.That(
            () =>
            {
                var outcomes = new SagaParticipantOutcome[2];
                SagaCoordinatorCore.OnParticipantResult(outcomes, 2, SagaParticipantOutcome.PreparedAck);
            },
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void Decide_empty_participant_set_commits_vacuously()
    {
        Assert.That(SagaCoordinatorCore.Decide(ReadOnlySpan<SagaParticipantOutcome>.Empty), Is.EqualTo(SagaDecision.Commit));
    }

    [Test]
    public void Decide_all_pending_is_collecting()
    {
        Assert.That(
            Decide(SagaParticipantOutcome.Pending, SagaParticipantOutcome.Pending),
            Is.EqualTo(SagaDecision.Collecting));
    }

    [Test]
    public void Decide_some_acked_some_pending_is_collecting()
    {
        Assert.That(
            Decide(SagaParticipantOutcome.PreparedAck, SagaParticipantOutcome.Pending),
            Is.EqualTo(SagaDecision.Collecting));
    }

    [Test]
    public void Decide_all_acked_commits()
    {
        Assert.That(
            Decide(SagaParticipantOutcome.PreparedAck, SagaParticipantOutcome.PreparedAck, SagaParticipantOutcome.PreparedAck),
            Is.EqualTo(SagaDecision.Commit));
    }

    [Test]
    public void Decide_single_acked_commits()
    {
        Assert.That(Decide(SagaParticipantOutcome.PreparedAck), Is.EqualTo(SagaDecision.Commit));
    }

    [Test]
    public void Decide_one_nack_among_acks_aborts()
    {
        Assert.That(
            Decide(SagaParticipantOutcome.PreparedAck, SagaParticipantOutcome.PreparedNack, SagaParticipantOutcome.PreparedAck),
            Is.EqualTo(SagaDecision.Abort));
    }

    [Test]
    public void Decide_one_unreachable_among_acks_aborts()
    {
        Assert.That(
            Decide(SagaParticipantOutcome.PreparedAck, SagaParticipantOutcome.Unreachable),
            Is.EqualTo(SagaDecision.Abort));
    }

    [Test]
    public void Decide_failure_is_decisive_even_while_a_participant_is_pending()
    {
        // A single nack aborts immediately, before the still-pending sibling votes.
        Assert.That(
            Decide(SagaParticipantOutcome.Pending, SagaParticipantOutcome.PreparedNack),
            Is.EqualTo(SagaDecision.Abort));
    }

    [Test]
    public void Decide_never_commits_and_aborts_the_same_outcome_set()
    {
        // Commit requires no failures; abort requires a failure - the two are
        // mutually exclusive over any single outcome set.
        var commitSet = new[] { SagaParticipantOutcome.PreparedAck, SagaParticipantOutcome.PreparedAck };
        var abortSet = new[] { SagaParticipantOutcome.PreparedAck, SagaParticipantOutcome.Unreachable };

        Assert.Multiple(() =>
        {
            Assert.That(SagaCoordinatorCore.Decide(commitSet), Is.EqualTo(SagaDecision.Commit));
            Assert.That(SagaCoordinatorCore.Decide(abortSet), Is.EqualTo(SagaDecision.Abort));
        });
    }

    [Test]
    public void Decide_is_order_independent_over_a_folded_vote_sequence()
    {
        // Folding the same votes in two different orders yields the same verdict.
        var forward = new SagaParticipantOutcome[3];
        SagaCoordinatorCore.OnParticipantResult(forward, 0, SagaParticipantOutcome.PreparedAck);
        SagaCoordinatorCore.OnParticipantResult(forward, 1, SagaParticipantOutcome.PreparedNack);
        SagaCoordinatorCore.OnParticipantResult(forward, 2, SagaParticipantOutcome.PreparedAck);

        var reverse = new SagaParticipantOutcome[3];
        SagaCoordinatorCore.OnParticipantResult(reverse, 2, SagaParticipantOutcome.PreparedAck);
        SagaCoordinatorCore.OnParticipantResult(reverse, 1, SagaParticipantOutcome.PreparedNack);
        SagaCoordinatorCore.OnParticipantResult(reverse, 0, SagaParticipantOutcome.PreparedAck);

        Assert.That(SagaCoordinatorCore.Decide(forward), Is.EqualTo(SagaCoordinatorCore.Decide(reverse)));
    }
}
