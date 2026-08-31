namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// The unprojected-property failure, whose whole point is to be actionable:
/// it must name the offending property and the set that is available.
/// </summary>
[TestFixture]
public sealed class GrainIndexPropertyNotIndexedExceptionTests
{
    [Test]
    public void The_context_constructor_names_the_property_and_the_projected_set()
    {
        var exception = new GrainIndexPropertyNotIndexedException("Subjects", "Secret", ["Age", "Country"]);

        Assert.Multiple(() =>
        {
            Assert.That(exception.IndexName, Is.EqualTo("Subjects"));
            Assert.That(exception.PropertyName, Is.EqualTo("Secret"));
            Assert.That(exception.IndexedProperties, Is.EqualTo(new[] { "Age", "Country" }));
            Assert.That(exception.Message, Does.Contain("Subjects").And.Contain("Secret").And.Contain("Age, Country"));
        });
    }

    [Test]
    public void An_index_projecting_nothing_is_described_rather_than_left_blank()
    {
        var exception = new GrainIndexPropertyNotIndexedException("Subjects", "Secret", []);

        Assert.That(exception.Message, Does.Contain("(none)"));
    }

    [Test]
    public void The_parameterless_constructor_leaves_the_context_empty()
    {
        var exception = new GrainIndexPropertyNotIndexedException();

        Assert.Multiple(() =>
        {
            Assert.That(exception.IndexName, Is.Empty);
            Assert.That(exception.PropertyName, Is.Empty);
            Assert.That(exception.IndexedProperties, Is.Empty);
        });
    }

    [Test]
    public void The_message_constructor_keeps_the_message_and_empties_the_context()
    {
        var exception = new GrainIndexPropertyNotIndexedException("boom");

        Assert.Multiple(() =>
        {
            Assert.That(exception.Message, Is.EqualTo("boom"));
            Assert.That(exception.IndexName, Is.Empty);
            Assert.That(exception.PropertyName, Is.Empty);
            Assert.That(exception.IndexedProperties, Is.Empty);
        });
    }

    [Test]
    public void The_inner_exception_constructor_wraps_the_cause()
    {
        var cause = new InvalidOperationException("cause");

        var exception = new GrainIndexPropertyNotIndexedException("boom", cause);

        Assert.Multiple(() =>
        {
            Assert.That(exception.Message, Is.EqualTo("boom"));
            Assert.That(exception.InnerException, Is.SameAs(cause));
            Assert.That(exception.IndexedProperties, Is.Empty);
        });
    }

    [Test]
    public void The_context_constructor_rejects_null_arguments()
    {
        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(
                () => new GrainIndexPropertyNotIndexedException(null!, "Secret", []));
            Assert.Throws<ArgumentNullException>(
                () => new GrainIndexPropertyNotIndexedException("Subjects", null!, []));
            Assert.Throws<ArgumentNullException>(
                () => new GrainIndexPropertyNotIndexedException("Subjects", "Secret", null!));
        });
    }

    [Test]
    public void The_type_derives_directly_from_exception_so_orleans_can_copy_it()
    {
        Assert.That(typeof(GrainIndexPropertyNotIndexedException).BaseType, Is.EqualTo(typeof(Exception)));
    }
}
