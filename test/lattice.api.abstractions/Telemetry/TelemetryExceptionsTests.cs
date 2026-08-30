using Orleans.Lattice.Api.Telemetry;

namespace Orleans.Lattice.Api.Abstractions.Tests.Telemetry;

/// <summary>
/// Exercises the hand-written constructors of the telemetry group's typed
/// exceptions and pins the fail-closed contract they encode: an unknown query and
/// an unoffered query are the same outcome, and a bounds rejection carries a typed
/// reason a transport binding can map without parsing the message.
/// </summary>
[TestFixture]
public sealed class TelemetryExceptionsTests
{
    [Test]
    public void QueryNotFound_queryId_ctor_composes_message_and_captures_the_id()
    {
        var ex = new TelemetryQueryNotFoundException("tree.write.ops");

        Assert.Multiple(() =>
        {
            Assert.That(ex.QueryId, Is.EqualTo("tree.write.ops"));
            Assert.That(ex.Message, Does.Contain("tree.write.ops"));
            Assert.That(ex.Message, Does.Contain("not available"),
                "The message must not distinguish an unknown query from an unoffered one.");
        });
    }

    [Test]
    public void QueryNotFound_message_ctor_uses_the_custom_message()
    {
        var ex = new TelemetryQueryNotFoundException("tree.write.ops", "explicit text");

        Assert.Multiple(() =>
        {
            Assert.That(ex.QueryId, Is.EqualTo("tree.write.ops"));
            Assert.That(ex.Message, Is.EqualTo("explicit text"));
        });
    }

    [Test]
    public void QueryBounds_violation_ctor_composes_message_and_captures_all_context()
    {
        var ex = new TelemetryQueryBoundsException("tree.write.ops", TelemetryBoundsViolation.RangeTooLong);

        Assert.Multiple(() =>
        {
            Assert.That(ex.QueryId, Is.EqualTo("tree.write.ops"));
            Assert.That(ex.Violation, Is.EqualTo(TelemetryBoundsViolation.RangeTooLong));
            Assert.That(ex.Message, Does.Contain("tree.write.ops"));
            Assert.That(ex.Message, Does.Contain(nameof(TelemetryBoundsViolation.RangeTooLong)));
        });
    }

    [Test]
    public void QueryBounds_message_ctor_uses_the_custom_message_and_keeps_the_typed_reason()
    {
        var ex = new TelemetryQueryBoundsException(
            "tree.write.ops",
            TelemetryBoundsViolation.TooManyPoints,
            "explicit text");

        Assert.Multiple(() =>
        {
            Assert.That(ex.QueryId, Is.EqualTo("tree.write.ops"));
            Assert.That(ex.Violation, Is.EqualTo(TelemetryBoundsViolation.TooManyPoints));
            Assert.That(ex.Message, Is.EqualTo("explicit text"));
        });
    }

    [Test]
    public void Both_exceptions_derive_directly_from_the_base_exception_type()
    {
        Assert.Multiple(() =>
        {
            Assert.That(typeof(TelemetryQueryNotFoundException).BaseType, Is.EqualTo(typeof(Exception)),
                "Deriving directly from System.Exception is what keeps a same-silo deep copy safe if the "
                + "type is ever marked serializable.");
            Assert.That(typeof(TelemetryQueryBoundsException).BaseType, Is.EqualTo(typeof(Exception)));
        });
    }
}
