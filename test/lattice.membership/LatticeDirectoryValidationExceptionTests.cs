namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeDirectoryValidationException"/>: the
/// context-carrying factory methods, their argument guards, and the standard
/// framework constructors.
/// </summary>
public class LatticeDirectoryValidationExceptionTests
{
    [Test]
    public void Unresolved_carries_the_id_expected_kind_and_null_resolved_kind()
    {
        var ex = LatticeDirectoryValidationException.Unresolved("u-1", DirectoryPrincipalKind.User, "memberId");

        Assert.Multiple(() =>
        {
            Assert.That(ex.PrincipalId, Is.EqualTo("u-1"));
            Assert.That(ex.ExpectedKind, Is.EqualTo(DirectoryPrincipalKind.User));
            Assert.That(ex.ResolvedKind, Is.Null);
            Assert.That(ex.ParamName, Is.EqualTo("memberId"));
            Assert.That(ex.Message, Does.Contain("u-1"));
        });
    }

    [Test]
    public void KindMismatch_carries_the_expected_and_resolved_kinds()
    {
        var ex = LatticeDirectoryValidationException.KindMismatch(
            "g-1", DirectoryPrincipalKind.Group, DirectoryPrincipalKind.User, "group");

        Assert.Multiple(() =>
        {
            Assert.That(ex.PrincipalId, Is.EqualTo("g-1"));
            Assert.That(ex.ExpectedKind, Is.EqualTo(DirectoryPrincipalKind.Group));
            Assert.That(ex.ResolvedKind, Is.EqualTo(DirectoryPrincipalKind.User));
            Assert.That(ex.ParamName, Is.EqualTo("group"));
        });
    }

    [Test]
    public void Unresolved_rejects_null_arguments()
    {
        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(
                () => LatticeDirectoryValidationException.Unresolved(null!, DirectoryPrincipalKind.User, "memberId"));
            Assert.Throws<ArgumentNullException>(
                () => LatticeDirectoryValidationException.Unresolved("u-1", DirectoryPrincipalKind.User, null!));
        });
    }

    [Test]
    public void KindMismatch_rejects_null_arguments()
    {
        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(
                () => LatticeDirectoryValidationException.KindMismatch(
                    null!, DirectoryPrincipalKind.Group, DirectoryPrincipalKind.User, "group"));
            Assert.Throws<ArgumentNullException>(
                () => LatticeDirectoryValidationException.KindMismatch(
                    "g-1", DirectoryPrincipalKind.Group, DirectoryPrincipalKind.User, null!));
        });
    }

    [Test]
    public void Derives_from_argument_exception()
    {
        Assert.That(
            LatticeDirectoryValidationException.Unresolved("u-1", DirectoryPrincipalKind.User, "memberId"),
            Is.InstanceOf<ArgumentException>());
    }

    [Test]
    public void Framework_constructors_preserve_message_and_inner_exception()
    {
        var inner = new InvalidOperationException("boom");

        var parameterless = new LatticeDirectoryValidationException();
        var withMessage = new LatticeDirectoryValidationException("bad");
        var withInner = new LatticeDirectoryValidationException("bad", inner);

        Assert.Multiple(() =>
        {
            Assert.That(parameterless.PrincipalId, Is.Empty);
            Assert.That(withMessage.Message, Does.Contain("bad"));
            Assert.That(withInner.InnerException, Is.SameAs(inner));
        });
    }
}
