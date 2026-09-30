using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.UI.Areas.Access;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access;

/// <summary>
/// Reading an app-owned rule's owner from its id, and classifying the auth
/// facade's faults, typed or arriving over gRPC as plain argument errors.
/// </summary>
[TestFixture]
public sealed class AccessAppRuleAndFailureTests
{
    [Test]
    [TestCase("app:crm:viewer:0123", "crm", "viewer")]
    [TestCase("app:crm", "crm", null)]
    [TestCase("app:crm:", "crm", null)]
    [TestCase("app::viewer:1", "", "viewer")]
    public void An_app_owned_id_names_its_app_and_role(string ruleId, string slug, string? role)
    {
        Assert.Multiple(() =>
        {
            Assert.That(AccessAppRule.TryParse(ruleId, out var owner), Is.True);
            Assert.That(owner, Is.EqualTo(new AccessAppRule(slug, role)));
            Assert.That(owner.HasSlug, Is.EqualTo(slug.Length > 0));
        });
    }

    [Test]
    [TestCase("orders-read")]
    [TestCase("App:crm:x")]
    [TestCase(null)]
    public void Any_other_id_is_authored(string? ruleId)
    {
        Assert.That(AccessAppRule.TryParse(ruleId, out _), Is.False);
    }

    [Test]
    public void Faults_are_classified_for_where_they_are_shown()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AccessFailure.From(new OperationCanceledException()), Is.Null);
            Assert.That(Kind(new LatticeAuthorizationDeniedException("t", LatticeOperation.Admin, "s", "r")), Is.EqualTo(AccessFailureKind.Denied));
            Assert.That(Kind(LatticeDirectoryValidationException.Unresolved("x", DirectoryPrincipalKind.User, "p")), Is.EqualTo(AccessFailureKind.DirectoryValidation));
            Assert.That(Kind(new ArgumentException("Directory validation failed: the id 'x' resolves to a User principal, but a Group was expected.")), Is.EqualTo(AccessFailureKind.DirectoryValidation));
            Assert.That(Kind(LatticeAppOwnedRuleException.Rejected("app:x", "rule")), Is.EqualTo(AccessFailureKind.AppOwned));
            Assert.That(Kind(new ArgumentException(LatticeAppOwnedRuleException.Rejected("app:x", "rule").Message)), Is.EqualTo(AccessFailureKind.AppOwned));
            Assert.That(Kind(new ArgumentException("bad")), Is.EqualTo(AccessFailureKind.Invalid));
            Assert.That(Kind(new KeyNotFoundException()), Is.EqualTo(AccessFailureKind.NotFound));
            Assert.That(Kind(new NotSupportedException()), Is.EqualTo(AccessFailureKind.Unavailable));
            Assert.That(Kind(new InvalidOperationException()), Is.EqualTo(AccessFailureKind.Unavailable));
            Assert.That(Kind(new TimeoutException()), Is.EqualTo(AccessFailureKind.Unavailable));
            Assert.That(AccessFailure.From(new ArgumentException("bad"))!.Message, Is.EqualTo("bad"));
        });
    }

    private static AccessFailureKind Kind(Exception exception) => AccessFailure.From(exception)!.Kind;
}
