namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeTenantGroupNestingException"/>: the framework
/// constructors, and the context-carrying form the directory raises for each
/// nesting violation.
/// </summary>
[TestFixture]
public sealed class LatticeTenantGroupNestingExceptionTests
{
    [Test]
    public void Parameterless_constructor_has_empty_ids()
    {
        var ex = new LatticeTenantGroupNestingException();

        Assert.Multiple(() =>
        {
            Assert.That(ex.GroupId, Is.Empty);
            Assert.That(ex.MemberId, Is.Empty);
            Assert.That(ex, Is.InstanceOf<ArgumentException>());
        });
    }

    [Test]
    public void Message_constructor_carries_the_message_and_empty_ids()
    {
        var ex = new LatticeTenantGroupNestingException("nope");

        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("nope"));
            Assert.That(ex.GroupId, Is.Empty);
            Assert.That(ex.MemberId, Is.Empty);
        });
    }

    [Test]
    public void Inner_exception_constructor_wraps_the_cause()
    {
        var inner = new InvalidOperationException("cause");

        var ex = new LatticeTenantGroupNestingException("nope", inner);

        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("nope"));
            Assert.That(ex.InnerException, Is.SameAs(inner));
            Assert.That(ex.GroupId, Is.Empty);
            Assert.That(ex.MemberId, Is.Empty);
        });
    }

    [TestCase(nameof(TenantGroupNestingViolation.MalformedTenantMember), "memberId", "not a valid tenant group id")]
    [TestCase(nameof(TenantGroupNestingViolation.MalformedTenantGroup), "groupId", "not a valid tenant group id")]
    [TestCase(nameof(TenantGroupNestingViolation.TenantGroupInClusterGroup), "memberId", "cluster group")]
    [TestCase(nameof(TenantGroupNestingViolation.TenantGroupInOtherTenantGroup), "memberId", "different tenant")]
    public void Create_carries_the_edge_and_names_the_offending_parameter(
        string violation,
        string paramName,
        string messageFragment)
    {
        var ex = LatticeTenantGroupNestingException.Create(Enum.Parse<TenantGroupNestingViolation>(violation), "g", "m");

        Assert.Multiple(() =>
        {
            Assert.That(ex.GroupId, Is.EqualTo("g"));
            Assert.That(ex.MemberId, Is.EqualTo("m"));
            Assert.That(ex.ParamName, Is.EqualTo(paramName));
            Assert.That(ex.Message, Does.Contain(messageFragment));
        });
    }

    [Test]
    public void Create_for_no_violation_throws()
    {
        Assert.That(
            () => LatticeTenantGroupNestingException.Create(TenantGroupNestingViolation.None, "g", "m"),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }
}
