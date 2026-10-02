namespace Orleans.Lattice.Api.Auth.Tests;

/// <summary>
/// Unit coverage for <see cref="LatticeTenantOwnedGroupException"/>: the factory the
/// cluster facade raises, its guards, and the framework constructors.
/// </summary>
[TestFixture]
public sealed class LatticeTenantOwnedGroupExceptionTests
{
    [Test]
    public void Rejected_carries_the_group_id_and_parameter_and_names_the_tenant_directory_facade()
    {
        var ex = LatticeTenantOwnedGroupException.Rejected("t/acme/readers", "group");

        Assert.Multiple(() =>
        {
            Assert.That(ex.GroupId, Is.EqualTo("t/acme/readers"));
            Assert.That(ex.ParamName, Is.EqualTo("group"));
            Assert.That(ex.Message, Does.Contain("t/acme/readers"));
            Assert.That(ex.Message, Does.Contain("ILatticeTenantDirectoryAdmin"));
            Assert.That(ex, Is.InstanceOf<ArgumentException>(), "bindings map an ArgumentException to invalid-argument");
        });
    }

    [Test]
    public void Rejected_refuses_a_null_group_id() =>
        Assert.Throws<ArgumentNullException>(() => LatticeTenantOwnedGroupException.Rejected(null!, "group"));

    [Test]
    public void Rejected_refuses_a_null_parameter_name() =>
        Assert.Throws<ArgumentNullException>(() => LatticeTenantOwnedGroupException.Rejected("t/acme/x", null!));

    [Test]
    public void Parameterless_constructor_has_an_empty_group_id() =>
        Assert.That(new LatticeTenantOwnedGroupException().GroupId, Is.Empty);

    [Test]
    public void Message_constructor_keeps_the_message_with_an_empty_group_id()
    {
        var ex = new LatticeTenantOwnedGroupException("refused");

        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("refused"));
            Assert.That(ex.GroupId, Is.Empty);
        });
    }

    [Test]
    public void Inner_exception_constructor_keeps_the_cause()
    {
        var inner = new InvalidOperationException("cause");

        var ex = new LatticeTenantOwnedGroupException("refused", inner);

        Assert.Multiple(() =>
        {
            Assert.That(ex.InnerException, Is.SameAs(inner));
            Assert.That(ex.GroupId, Is.Empty);
        });
    }
}
