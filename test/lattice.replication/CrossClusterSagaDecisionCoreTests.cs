namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit tests for <see cref="CrossClusterSagaDecisionCore"/>: commit only on a
/// unanimous commit vote, and report the first dissenting participant.
/// </summary>
[TestFixture]
public sealed class CrossClusterSagaDecisionCoreTests
{
    [Test]
    public void Decide_commits_when_every_vote_commits()
    {
        var commit = CrossClusterSagaDecisionCore.Decide(
            [SagaVote.Commit, SagaVote.Commit, SagaVote.Commit], out var dissent);

        Assert.Multiple(() =>
        {
            Assert.That(commit, Is.True);
            Assert.That(dissent, Is.EqualTo(-1));
        });
    }

    [Test]
    public void Decide_aborts_on_one_abort_and_names_it()
    {
        var commit = CrossClusterSagaDecisionCore.Decide(
            [SagaVote.Commit, SagaVote.Abort, SagaVote.Abort], out var dissent);

        Assert.Multiple(() =>
        {
            Assert.That(commit, Is.False);
            Assert.That(dissent, Is.EqualTo(1));
        });
    }

    [Test]
    public void Decide_treats_a_missing_vote_as_dissent()
    {
        var commit = CrossClusterSagaDecisionCore.Decide([SagaVote.None, SagaVote.Commit], out var dissent);

        Assert.Multiple(() =>
        {
            Assert.That(commit, Is.False);
            Assert.That(dissent, Is.Zero);
        });
    }

    [Test]
    public void Decide_commits_vacuously_with_no_participant()
    {
        var commit = CrossClusterSagaDecisionCore.Decide([], out var dissent);

        Assert.Multiple(() =>
        {
            Assert.That(commit, Is.True);
            Assert.That(dissent, Is.EqualTo(-1));
        });
    }
}
