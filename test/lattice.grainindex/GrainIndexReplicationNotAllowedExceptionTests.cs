namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// Covers <see cref="GrainIndexReplicationNotAllowedException"/>: the typed
/// failure the startup replication guard raises when an index tree is
/// configured to replicate without the index having opted in.
/// </summary>
[TestFixture]
public sealed class GrainIndexReplicationNotAllowedExceptionTests
{
    [Test]
    public void Parameterless_constructor_leaves_empty_context()
    {
        var exception = new GrainIndexReplicationNotAllowedException();

        Assert.Multiple(() =>
        {
            Assert.That(exception.IndexName, Is.Empty);
            Assert.That(exception.TreeName, Is.Empty);
            Assert.That(exception.MergeMode, Is.EqualTo(default(LatticeMergeMode)));
        });
    }

    [Test]
    public void Message_constructor_keeps_the_message_and_leaves_empty_context()
    {
        var exception = new GrainIndexReplicationNotAllowedException("boom");

        Assert.Multiple(() =>
        {
            Assert.That(exception.Message, Is.EqualTo("boom"));
            Assert.That(exception.IndexName, Is.Empty);
            Assert.That(exception.TreeName, Is.Empty);
        });
    }

    [Test]
    public void Inner_exception_constructor_wraps_the_cause()
    {
        var cause = new InvalidOperationException("cause");
        var exception = new GrainIndexReplicationNotAllowedException("boom", cause);

        Assert.Multiple(() =>
        {
            Assert.That(exception.Message, Is.EqualTo("boom"));
            Assert.That(exception.InnerException, Is.SameAs(cause));
            Assert.That(exception.TreeName, Is.Empty);
        });
    }

    [Test]
    public void Context_constructor_carries_the_index_the_tree_and_the_mode()
    {
        var exception = new GrainIndexReplicationNotAllowedException(
            "users", "__grainindex/users", LatticeMergeMode.OrSet);

        Assert.Multiple(() =>
        {
            Assert.That(exception.IndexName, Is.EqualTo("users"));
            Assert.That(exception.TreeName, Is.EqualTo("__grainindex/users"));
            Assert.That(exception.MergeMode, Is.EqualTo(LatticeMergeMode.OrSet));
        });
    }

    [Test]
    public void Message_names_the_tree_and_the_allow_replication_remedy()
    {
        var message = new GrainIndexReplicationNotAllowedException(
            "users", "__grainindex/users", LatticeMergeMode.LwwRegister).Message;

        Assert.Multiple(() =>
        {
            Assert.That(message, Does.Contain("users"));
            Assert.That(message, Does.Contain("__grainindex/users"));
            Assert.That(message, Does.Contain("AllowReplication"),
                "The message must name the opt-in an operator would set to accept the "
                + "configuration deliberately.");
            Assert.That(message, Does.Contain(nameof(LatticeMergeMode.LwwRegister)));
        });
    }

    [Test]
    public void A_null_index_name_is_rejected()
    {
        Assert.That(
            () => new GrainIndexReplicationNotAllowedException(null!, "tree", LatticeMergeMode.LwwRegister),
            Throws.ArgumentNullException);
    }

    [Test]
    public void A_null_tree_name_is_rejected()
    {
        Assert.That(
            () => new GrainIndexReplicationNotAllowedException("users", null!, LatticeMergeMode.LwwRegister),
            Throws.ArgumentNullException);
    }

    [Test]
    public void The_type_derives_directly_from_exception_so_orleans_can_deep_copy_it()
    {
        Assert.That(
            typeof(GrainIndexReplicationNotAllowedException).BaseType,
            Is.EqualTo(typeof(Exception)),
            "Orleans registers a same-silo deep copier for System.Exception but not for its BCL "
            + "subclasses, so a [GenerateSerializer] exception must derive directly from it.");
    }
}
