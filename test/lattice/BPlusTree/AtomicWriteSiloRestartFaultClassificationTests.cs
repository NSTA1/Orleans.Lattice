namespace Orleans.Lattice.Tests.BPlusTree;

[TestFixture]
public sealed class AtomicWriteSiloRestartFaultClassificationTests
{
    private const string ReminderUnavailableMessage = "The reminder service is not currently available.";

    [Test]
    public void Silo_churn_classification_accepts_the_departing_in_memory_reminder_table_fault()
    {
        var fault = new InvalidOperationException(ReminderUnavailableMessage);

        Assert.That(AtomicWriteSiloRestartChaosTests.IsSiloChurnFault(fault), Is.True);
    }

    [Test]
    public void Silo_churn_classification_accepts_a_wrapped_departing_reminder_table_fault()
    {
        var fault = new AggregateException(
            new InvalidOperationException("unrelated"),
            new Exception("saga failed", new InvalidOperationException(ReminderUnavailableMessage)));

        Assert.That(AtomicWriteSiloRestartChaosTests.IsSiloChurnFault(fault), Is.True);
    }

    [TestCase("The reminder service is not currently available. Unexpected extra failure.")]
    [TestCase("unrelated")]
    public void Silo_churn_classification_rejects_other_invalid_operations(string message)
    {
        Assert.That(AtomicWriteSiloRestartChaosTests.IsSiloChurnFault(
            new InvalidOperationException(message)), Is.False);
    }

    [Test]
    public void Silo_churn_classification_rejects_other_exception_types_with_the_same_message()
    {
        Assert.That(AtomicWriteSiloRestartChaosTests.IsSiloChurnFault(
            new Exception(ReminderUnavailableMessage)), Is.False);
    }
}
