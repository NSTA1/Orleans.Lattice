using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Vector.Tests.Persistence;

[TestFixture]
public sealed class VectorSearchOutcomeTests
{
    [Test]
    public void An_outcome_carries_the_count_and_the_path_that_answered()
    {
        var outcome = new VectorSearchOutcome(5, VectorSearchMode.Approximate);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Count, Is.EqualTo(5));
            Assert.That(outcome.Mode, Is.EqualTo(VectorSearchMode.Approximate));
        });
    }

    [Test]
    public void Two_outcomes_with_the_same_content_are_equal()
    {
        Assert.That(
            new VectorSearchOutcome(3, VectorSearchMode.Exhaustive),
            Is.EqualTo(new VectorSearchOutcome(3, VectorSearchMode.Exhaustive)));
    }
}
