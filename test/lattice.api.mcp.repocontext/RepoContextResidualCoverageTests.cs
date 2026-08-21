namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Small targeted unit tests closing residual coverage gaps: the two-argument
/// (message and inner exception) <see cref="EmbeddingSpaceMismatchException"/>
/// constructor, and the absent-register branch of
/// <see cref="RepoContextValues.ReadHlcWallTicks(BoundedRegister)"/>.
/// </summary>
[TestFixture]
public sealed class RepoContextResidualCoverageTests
{
    [Test]
    public void EmbeddingSpaceMismatchException_carries_message_and_inner_exception()
    {
        var inner = new InvalidOperationException("cause");

        var exception = new EmbeddingSpaceMismatchException("spaces diverged", inner);

        Assert.Multiple(() =>
        {
            Assert.That(exception.Message, Is.EqualTo("spaces diverged"));
            Assert.That(exception.InnerException, Is.SameAs(inner));
        });
    }

    [Test]
    public void ReadHlcWallTicks_of_an_unwritten_register_is_null()
    {
        var register = new BoundedRegister();

        Assert.That(RepoContextValues.ReadHlcWallTicks(register), Is.Null,
            "a register that was never written carries no wall-clock anchor");
    }

    [Test]
    public void ReadHlcWallTicks_reads_the_wall_component_of_a_written_register()
    {
        var register = RepoContextValues.Lww("value", new HybridLogicalClock { WallClockTicks = 12345, Counter = 7 });

        Assert.That(RepoContextValues.ReadHlcWallTicks(register), Is.EqualTo(12345));
    }

    [Test]
    public void ReadHlcWallTicks_rejects_a_null_register()
        => Assert.Throws<ArgumentNullException>(() => RepoContextValues.ReadHlcWallTicks(null!));
}
