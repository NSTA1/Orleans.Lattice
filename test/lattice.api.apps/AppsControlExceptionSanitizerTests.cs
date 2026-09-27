namespace Orleans.Lattice.Api.Apps.Tests;

[TestFixture]
public sealed class AppsControlExceptionSanitizerTests
{
    [TestCase("t/acme/a/crm/contacts", "crm", "contacts")]
    [TestCase("a/crm/contacts", "crm", "contacts")]
    [TestCase("a/billing/ledger", "crm", "billing:ledger")]
    [TestCase("t/acme/a/billing/ledger", null, "billing:ledger")]
    [TestCase("t/acme/legacy-contacts", "crm", "legacy-contacts")]
    [TestCase("t/acme/a/crm/", "crm", "crm")]
    [TestCase("tree 'a/crm/x_y' and 't/t1/a/crm/z'.", "crm", "tree 'x_y' and 'z'.")]
    public void SanitizeText_rewrites_composed_ids_to_app_local_names(string text, string? ownApp, string expected)
    {
        Assert.That(AppsControlExceptionSanitizer.SanitizeText(text, ownApp), Is.EqualTo(expected));
    }

    [TestCase("data/crm/contacts")]
    [TestCase("format/x")]
    [TestCase("No ids at all.")]
    public void SanitizeText_leaves_non_composed_text_unchanged(string text)
    {
        Assert.That(AppsControlExceptionSanitizer.SanitizeText(text, "crm"), Is.SameAs(text));
        Assert.That(AppsControlExceptionSanitizer.ContainsComposedId(text), Is.False);
    }

    [Test]
    public void SanitizeText_null_returns_null()
    {
        Assert.That(AppsControlExceptionSanitizer.SanitizeText(null, "crm"), Is.Null);
        Assert.That(AppsControlExceptionSanitizer.ContainsComposedId(null), Is.False);
    }

    [Test]
    public void ContainsComposedId_detects_app_and_tenant_ids()
    {
        Assert.That(AppsControlExceptionSanitizer.ContainsComposedId("x a/crm/contacts"), Is.True);
        Assert.That(AppsControlExceptionSanitizer.ContainsComposedId("x t/acme/legacy"), Is.True);
    }

    [Test]
    public void TryRewrite_clean_exception_is_not_rewritten()
    {
        Assert.That(AppsControlExceptionSanitizer.TryRewrite(new InvalidOperationException("clean"), "crm", out var sanitized), Is.False);
        Assert.That(sanitized, Is.Null);
    }

    private static IEnumerable<TestCaseData> Categories()
    {
        const string leak = "at t/acme/a/crm/contacts";
        yield return new TestCaseData(new ArgumentNullException("p", leak), typeof(ArgumentException));
        yield return new TestCaseData(new KeyNotFoundException(leak), typeof(KeyNotFoundException));
        yield return new TestCaseData(new TimeoutException(leak), typeof(TimeoutException));
        yield return new TestCaseData(new LatticeTenantAccessDeniedException(leak), typeof(LatticeTenantAccessDeniedException));
        yield return new TestCaseData(new LatticeAuthorizationDeniedException(leak), typeof(LatticeAuthorizationDeniedException));
        yield return new TestCaseData(new OperationCanceledException(leak), typeof(OperationCanceledException));
        yield return new TestCaseData(new ObjectDisposedException(leak), typeof(InvalidOperationException));
        yield return new TestCaseData(new FormatException(leak), typeof(InvalidOperationException));
    }

    [TestCaseSource(nameof(Categories))]
    public void TryRewrite_preserves_the_exception_category(Exception original, Type expected)
    {
        Assert.That(AppsControlExceptionSanitizer.TryRewrite(original, "crm", out var sanitized), Is.True);
        Assert.That(sanitized, Is.TypeOf(expected));
        Assert.That(sanitized!.Message, Does.Not.Contain("t/acme").And.Not.Contain("a/crm/"));
    }

    [Test]
    public void TryRewrite_clean_message_with_leaky_inner_drops_the_inner_chain()
    {
        var original = new InvalidOperationException("outer", new AggregateException(new TimeoutException("t/acme/a/crm/x")));

        Assert.That(AppsControlExceptionSanitizer.TryRewrite(original, "crm", out var sanitized), Is.True);
        Assert.That(sanitized!.Message, Is.EqualTo("outer"));
        Assert.That(sanitized.InnerException, Is.Null);
    }
}
