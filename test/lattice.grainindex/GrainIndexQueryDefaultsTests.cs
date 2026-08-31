namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// The query defaults and the execution-mode enum, both of which are part of the
/// public contract: a caller reads them to know what an unconfigured query does.
/// </summary>
[TestFixture]
public sealed class GrainIndexQueryDefaultsTests
{
    [Test]
    public void The_default_page_size_is_a_bounded_positive_slice()
    {
        Assert.That(GrainIndexQueryDefaults.PageSize, Is.EqualTo(256));
    }

    [Test]
    public void The_default_execution_is_the_durable_cursor()
    {
        Assert.That(GrainIndexQueryDefaults.Execution, Is.EqualTo(GrainIndexQueryExecution.DurableCursor));
    }

    [Test]
    public void The_durable_cursor_is_the_zero_value_so_it_is_the_unset_default()
    {
        Assert.That((int)GrainIndexQueryExecution.DurableCursor, Is.Zero);
    }

    [Test]
    public void Every_execution_mode_is_distinct_and_declared()
    {
        var modes = Enum.GetValues<GrainIndexQueryExecution>();

        Assert.Multiple(() =>
        {
            Assert.That(modes, Is.EquivalentTo(new[]
            {
                GrainIndexQueryExecution.DurableCursor,
                GrainIndexQueryExecution.Stream,
                GrainIndexQueryExecution.SnapshotCursor,
            }));
            Assert.That(modes.Distinct().Count(), Is.EqualTo(modes.Length));
        });
    }
}
