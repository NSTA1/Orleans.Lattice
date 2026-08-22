using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Unit tests for <see cref="FaultBudget"/>, the bounded fault ledger the Coyote
/// liveness models draw drop / duplicate / restart allowances from. These are
/// plain deterministic unit tests (no Coyote engine): the nondeterministic
/// decision source is a scripted delegate.
/// </summary>
[TestFixture]
public sealed class FaultBudgetTests
{
    private static Func<bool> Always(bool value) => () => value;

    [Test]
    public void Constructor_rejects_a_negative_drop_budget()
    {
        Assert.That(
            () => new FaultBudget(drops: -1, duplicates: 0, restarts: 0),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void Constructor_rejects_a_negative_duplicate_budget()
    {
        Assert.That(
            () => new FaultBudget(drops: 0, duplicates: -1, restarts: 0),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void Constructor_rejects_a_negative_restart_budget()
    {
        Assert.That(
            () => new FaultBudget(drops: 0, duplicates: 0, restarts: -1),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void Fresh_budget_reports_its_remaining_allowances()
    {
        var budget = new FaultBudget(drops: 2, duplicates: 3, restarts: 1);

        Assert.Multiple(() =>
        {
            Assert.That(budget.DropsRemaining, Is.EqualTo(2));
            Assert.That(budget.DuplicatesRemaining, Is.EqualTo(3));
            Assert.That(budget.RestartsRemaining, Is.EqualTo(1));
            Assert.That(budget.IsExhausted, Is.False);
        });
    }

    [Test]
    public void Try_drop_consumes_one_drop_when_the_decision_accepts()
    {
        var budget = new FaultBudget(drops: 1, duplicates: 0, restarts: 0);

        Assert.That(budget.TryDrop(Always(true)), Is.True);
        Assert.That(budget.DropsRemaining, Is.EqualTo(0));
    }

    [Test]
    public void Try_drop_consumes_nothing_when_the_decision_declines()
    {
        var budget = new FaultBudget(drops: 1, duplicates: 0, restarts: 0);

        Assert.That(budget.TryDrop(Always(false)), Is.False);
        Assert.That(budget.DropsRemaining, Is.EqualTo(1));
    }

    [Test]
    public void Try_drop_returns_false_and_never_consults_the_decision_once_exhausted()
    {
        var budget = new FaultBudget(drops: 0, duplicates: 0, restarts: 0);
        var consulted = false;

        var injected = budget.TryDrop(() =>
        {
            consulted = true;
            return true;
        });

        Assert.Multiple(() =>
        {
            Assert.That(injected, Is.False);
            Assert.That(consulted, Is.False);
        });
    }

    [Test]
    public void Try_duplicate_and_try_restart_consume_their_own_budgets_independently()
    {
        var budget = new FaultBudget(drops: 0, duplicates: 1, restarts: 1);

        Assert.Multiple(() =>
        {
            Assert.That(budget.TryDuplicate(Always(true)), Is.True);
            Assert.That(budget.TryRestart(Always(true)), Is.True);
            Assert.That(budget.DuplicatesRemaining, Is.EqualTo(0));
            Assert.That(budget.RestartsRemaining, Is.EqualTo(0));
            Assert.That(budget.IsExhausted, Is.True);
        });
    }

    [Test]
    public void Try_drop_rejects_a_null_decision()
    {
        var budget = new FaultBudget(drops: 1, duplicates: 0, restarts: 0);

        Assert.That(() => budget.TryDrop(null!), Throws.ArgumentNullException);
    }
}
