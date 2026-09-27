namespace Orleans.Lattice.Api.Apps.Tests;

/// <summary>
/// The sanitizer inspects the whole bounded exception graph, not just the first chain: a
/// composed id carried by any aggregated exception, at any depth within the bound, forces a
/// sanitized replacement.
/// </summary>
[TestFixture]
public sealed class AppsControlExceptionSanitizerGraphTests
{
    [Test]
    public void TryRewrite_detects_a_composed_id_nested_under_a_later_aggregated_exception()
    {
        var leaked = new AggregateException(
            new InvalidOperationException("clean"),
            new InvalidOperationException("wrapper", new InvalidOperationException("tree 't/acme/a/crm/contacts' failed")));

        var rewritten = AppsControlExceptionSanitizer.TryRewrite(leaked, "crm", out var sanitized);

        Assert.That(rewritten, Is.True);
        Assert.That(sanitized!.InnerException, Is.Null);
        Assert.That(sanitized.ToString(), Does.Not.Contain("t/acme"));
    }

    [Test]
    public void TryRewrite_detects_a_composed_id_under_a_nested_aggregate()
    {
        var leaked = new InvalidOperationException(
            "outer",
            new AggregateException(new ArgumentException("ok"), new AggregateException(new TimeoutException("a/crm/contacts"))));

        Assert.That(AppsControlExceptionSanitizer.TryRewrite(leaked, "crm", out _), Is.True);
    }

    [Test]
    public void TryRewrite_leaves_a_clean_exception_graph_alone()
    {
        var clean = new AggregateException(new InvalidOperationException("one"), new InvalidOperationException("two", new TimeoutException("three")));

        Assert.That(AppsControlExceptionSanitizer.TryRewrite(clean, "crm", out var sanitized), Is.False);
        Assert.That(sanitized, Is.Null);
    }

    [Test]
    public void TryRewrite_treats_an_exception_graph_beyond_the_inspection_bound_as_unsafe()
    {
        // A graph too large to inspect fully cannot be certified clean, so it is replaced.
        Exception deep = new InvalidOperationException("bottom");
        for (var i = 0; i < 64; i++)
            deep = new InvalidOperationException($"level {i}", deep);

        Assert.That(AppsControlExceptionSanitizer.TryRewrite(deep, "crm", out var sanitized), Is.True);
        Assert.That(sanitized!.InnerException, Is.Null);
    }
}
