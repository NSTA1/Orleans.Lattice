using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Indexing;

/// <summary>
/// Tests for <see cref="RepoIndexPacing"/> and <see cref="RepoIndexPaceState"/>, the
/// pacing snapshot <c>index_status</c> carries (issue #3447). The snapshot crosses a
/// grain boundary on <see cref="RepoIndexProgress.Pacing"/>, so its wire shape is
/// pinned by an Orleans round trip.
/// </summary>
[TestFixture]
public sealed class RepoIndexPacingTests
{
    private static readonly Serializer Serializer = new ServiceCollection()
        .AddSerializer()
        .BuildServiceProvider()
        .GetRequiredService<Serializer>();

    [Test]
    public void RepoIndexProgress_with_pacing_round_trips_through_the_orleans_serializer()
    {
        var since = new DateTimeOffset(2025, 1, 2, 3, 4, 5, TimeSpan.Zero);
        var progress = new RepoIndexProgress
        {
            RepoId = "acme",
            Status = RepoIndexStatus.Running,
            Phase = RepoIndexPhase.Vectorising,
            Pacing = new RepoIndexPacing
            {
                State = RepoIndexPaceState.Backoff,
                Reason = "an embedding batch failed",
                BatchDelayMilliseconds = 500,
                Since = since,
                ForegroundRequests = 2,
            },
        };

        var copy = Serializer.Deserialize<RepoIndexProgress>(Serializer.SerializeToArray(progress));

        Assert.That(copy.Pacing, Is.EqualTo(progress.Pacing));
    }

    [Test]
    public void RepoIndexProgress_without_pacing_round_trips_as_null()
    {
        var progress = new RepoIndexProgress { RepoId = "acme", Status = RepoIndexStatus.Completed, Phase = RepoIndexPhase.Done };

        var copy = Serializer.Deserialize<RepoIndexProgress>(Serializer.SerializeToArray(progress));

        Assert.That(copy.Pacing, Is.Null, "A job that is not being paced carries no snapshot.");
    }

    [Test]
    public void RepoIndexPacing_optional_members_default_to_nothing_in_flight()
    {
        var pacing = new RepoIndexPacing { State = RepoIndexPaceState.Idle, Reason = "no recent batches" };

        Assert.Multiple(() =>
        {
            Assert.That(pacing.BatchDelayMilliseconds, Is.Zero);
            Assert.That(pacing.Since, Is.Null);
            Assert.That(pacing.ForegroundRequests, Is.Zero);
        });
    }

    [Test]
    public void RepoIndexPaceState_values_are_stable_wire_ordinals()
    {
        Assert.That(
            Enum.GetValues<RepoIndexPaceState>().Select(v => (int)v),
            Is.EqualTo(new[] { 0, 1, 2, 3, 4, 5, 6 }),
            "The enum is serialized by value, so an existing ordinal must never be renumbered.");
    }
}
