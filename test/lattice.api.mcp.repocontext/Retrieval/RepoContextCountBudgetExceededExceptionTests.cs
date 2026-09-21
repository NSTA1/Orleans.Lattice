namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// The exception that carries "I could not count" rather than a number (#2447).
/// </summary>
/// <remarks>
/// Most of what matters about this type is that it EXISTS as a distinct signal, so
/// the fixtures below pin the two properties that make it one: the diagnostic detail
/// survives onto the instance, and the message names the repository and the budget,
/// because the only place this is read by a human is a log line written at the moment
/// a build decided to repair an index it could not measure.
/// </remarks>
[TestFixture]
public sealed class RepoContextCountBudgetExceededExceptionTests
{
    [Test]
    public void The_detail_constructor_carries_the_diagnosis_onto_the_instance()
    {
        var thrown = new RepoContextCountBudgetExceededException("acme", 4_096, TimeSpan.FromSeconds(10));

        Assert.Multiple(() =>
        {
            Assert.That(thrown.RepoId, Is.EqualTo("acme"));
            Assert.That(thrown.Counted, Is.EqualTo(4_096));
            Assert.That(thrown.Budget, Is.EqualTo(TimeSpan.FromSeconds(10)));
        });
    }

    [Test]
    public void The_message_names_the_repository_the_progress_and_the_budget()
    {
        // A bounded walk is a policy decision, and a log line that does not say
        // WHICH repository, HOW far it got, and AGAINST WHAT budget cannot be acted
        // on: the operator's next question is always whether the budget is too small
        // for that corpus, and only these three numbers answer it.
        var message = new RepoContextCountBudgetExceededException("acme", 4_096, TimeSpan.FromSeconds(10)).Message;

        Assert.Multiple(() =>
        {
            Assert.That(message, Does.Contain("acme"));
            Assert.That(message, Does.Contain("4096").Or.Contain("4,096"));
            Assert.That(message, Does.Contain("10"));
        });
    }

    [Test]
    public void The_partial_figure_is_described_as_unknown_rather_than_as_a_count()
    {
        // The message is where the design decision is explained to whoever reads it
        // at three in the morning, so it must not read as though a count was
        // produced. Someone acting on "walked 4096 keys" as a count is the exact
        // under-count this type exists to prevent.
        var message = new RepoContextCountBudgetExceededException("acme", 4_096, TimeSpan.FromSeconds(10)).Message;

        Assert.That(message, Does.Contain("unknown"),
            "the message must say the figure is unknown, not merely report how far the walk got");
    }

    [Test]
    public void The_parameterless_constructor_still_explains_itself()
    {
        var thrown = new RepoContextCountBudgetExceededException();

        Assert.Multiple(() =>
        {
            Assert.That(thrown.Message, Is.Not.Empty);
            Assert.That(thrown.Message, Does.Contain("budget"));
            Assert.That(thrown.RepoId, Is.Null, "no detail was supplied, so none may be invented");
            Assert.That(thrown.Counted, Is.Zero);
            Assert.That(thrown.Budget, Is.EqualTo(TimeSpan.Zero));
        });
    }

    [Test]
    public void The_message_and_cause_constructors_behave_as_an_exception_should()
    {
        var cause = new InvalidOperationException("underlying");
        var withMessage = new RepoContextCountBudgetExceededException("bespoke");
        var withCause = new RepoContextCountBudgetExceededException("bespoke", cause);

        Assert.Multiple(() =>
        {
            Assert.That(withMessage.Message, Is.EqualTo("bespoke"));
            Assert.That(withMessage.InnerException, Is.Null);
            Assert.That(withCause.Message, Is.EqualTo("bespoke"));
            Assert.That(withCause.InnerException, Is.SameAs(cause));
        });
    }

    [Test]
    public void It_is_not_an_enumeration_abort_and_must_not_be_caught_as_one()
    {
        // The distinction is the point of having a separate type. Orleans'
        // EnumerationAbortedException means the store lost the enumerator - a
        // transient fault of the infrastructure. This means the caller chose to stop
        // - a local policy decision. They are handled identically at the one site
        // that catches both, and conflating them anywhere else would tell the next
        // reader the store failed when it did not.
        var thrown = new RepoContextCountBudgetExceededException("acme", 1, TimeSpan.FromSeconds(1));

        Assert.That(thrown, Is.Not.InstanceOf<Orleans.Runtime.EnumerationAbortedException>());
    }
}
