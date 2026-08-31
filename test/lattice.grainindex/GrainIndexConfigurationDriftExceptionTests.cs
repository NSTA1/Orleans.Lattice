namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// Covers <see cref="GrainIndexConfigurationDriftException"/>: the typed failure
/// that blocks silo start when a declaration has drifted on a drift-breaking
/// field under the default reject policy.
/// </summary>
[TestFixture]
public sealed class GrainIndexConfigurationDriftExceptionTests
{
    private static readonly IReadOnlyList<GrainIndexDefinitionField> TwoFields =
    [
        GrainIndexDefinitionField.Properties,
        GrainIndexDefinitionField.KeyCodec,
    ];

    [Test]
    public void Parameterless_constructor_leaves_empty_context()
    {
        var exception = new GrainIndexConfigurationDriftException();

        Assert.Multiple(() =>
        {
            Assert.That(exception.IndexName, Is.Empty);
            Assert.That(exception.ChangedFields, Is.Empty);
        });
    }

    [Test]
    public void Message_constructor_keeps_the_message_and_leaves_empty_context()
    {
        var exception = new GrainIndexConfigurationDriftException("boom");

        Assert.Multiple(() =>
        {
            Assert.That(exception.Message, Is.EqualTo("boom"));
            Assert.That(exception.IndexName, Is.Empty);
            Assert.That(exception.ChangedFields, Is.Empty);
        });
    }

    [Test]
    public void Inner_exception_constructor_wraps_the_cause()
    {
        var cause = new InvalidOperationException("cause");
        var exception = new GrainIndexConfigurationDriftException("boom", cause);

        Assert.Multiple(() =>
        {
            Assert.That(exception.Message, Is.EqualTo("boom"));
            Assert.That(exception.InnerException, Is.SameAs(cause));
            Assert.That(exception.IndexName, Is.Empty);
            Assert.That(exception.ChangedFields, Is.Empty);
        });
    }

    [Test]
    public void Context_constructor_carries_the_index_and_the_changed_fields()
    {
        var exception = new GrainIndexConfigurationDriftException("users", TwoFields);

        Assert.Multiple(() =>
        {
            Assert.That(exception.IndexName, Is.EqualTo("users"));
            Assert.That(exception.ChangedFields, Is.EqualTo(TwoFields));
        });
    }

    [Test]
    public void Message_names_the_index_and_every_changed_field()
    {
        var message = new GrainIndexConfigurationDriftException("users", TwoFields).Message;

        Assert.Multiple(() =>
        {
            Assert.That(message, Does.Contain("users"));
            Assert.That(message, Does.Contain(nameof(GrainIndexDefinitionField.Properties)));
            Assert.That(message, Does.Contain(nameof(GrainIndexDefinitionField.KeyCodec)));
        });
    }

    [Test]
    public void Message_states_the_remediation()
    {
        var message = new GrainIndexConfigurationDriftException("users", TwoFields).Message;

        Assert.Multiple(() =>
        {
            Assert.That(message, Does.Contain("revert"),
                "An operator hitting this at start-up needs to be told the two ways out.");
            Assert.That(message, Does.Contain(nameof(GrainIndexDriftPolicy.Rebuild)));
        });
    }

    [Test]
    public void An_empty_changed_field_list_still_produces_a_readable_message()
    {
        Assert.That(
            new GrainIndexConfigurationDriftException("users", []).Message,
            Does.Contain("users"),
            "A caller passing no fields must still get the index name rather than a dangling list.");
    }

    [Test]
    public void A_null_index_name_is_rejected()
    {
        Assert.That(
            () => new GrainIndexConfigurationDriftException(null!, TwoFields),
            Throws.ArgumentNullException);
    }

    [Test]
    public void A_null_changed_field_list_is_rejected()
    {
        Assert.That(
            () => new GrainIndexConfigurationDriftException(
                "users", (IReadOnlyList<GrainIndexDefinitionField>)null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void The_type_derives_directly_from_exception_so_orleans_can_deep_copy_it()
    {
        Assert.That(typeof(GrainIndexConfigurationDriftException).BaseType, Is.EqualTo(typeof(Exception)),
            "Orleans registers a same-silo deep copier for System.Exception but not for its BCL "
            + "subclasses, so a [GenerateSerializer] exception must derive directly from it.");
    }
}
