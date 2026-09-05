namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Source;

/// <summary>
/// Tests for the transport's failure signal. It is a control-flow exception inside
/// the host process, converted by the source into a failed preparation, so what
/// matters is that it preserves the already-redacted message and the underlying
/// cause for diagnosis.
/// </summary>
[TestFixture]
public sealed class RepoContextGitSourceExceptionTests
{
    [Test]
    public void The_message_is_preserved()
    {
        var ex = new RepoContextGitSourceException("the ref did not resolve");

        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("the ref did not resolve"));
            Assert.That(ex.InnerException, Is.Null);
        });
    }

    [Test]
    public void The_underlying_transport_failure_is_preserved()
    {
        var cause = new InvalidOperationException("native failure");

        var ex = new RepoContextGitSourceException("the fetch failed", cause);

        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("the fetch failed"));
            Assert.That(ex.InnerException, Is.SameAs(cause));
        });
    }

    [Test]
    public void A_null_cause_is_accepted()
    {
        var ex = new RepoContextGitSourceException("the fetch failed", innerException: null);

        Assert.That(ex.InnerException, Is.Null);
    }

    [Test]
    public void It_is_an_ordinary_exception_that_never_crosses_a_wire()
    {
        Assert.Multiple(() =>
        {
            Assert.That(typeof(RepoContextGitSourceException).BaseType, Is.EqualTo(typeof(Exception)));
            Assert.That(
                typeof(RepoContextGitSourceException)
                    .GetCustomAttributes(typeof(GenerateSerializerAttribute), inherit: false),
                Is.Empty,
                "The source converts it into a failed preparation rather than serializing it.");
        });
    }
}
