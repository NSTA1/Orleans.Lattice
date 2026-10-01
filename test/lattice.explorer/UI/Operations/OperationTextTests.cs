using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Operations;

namespace Orleans.Lattice.Explorer.Tests.UI.Operations;

/// <summary>
/// The shared words for a long-running operation (#4122): the state, the phase,
/// the step and the units read the same in every area, and never invent a
/// percentage the cluster did not report.
/// </summary>
[TestFixture]
public sealed class OperationTextTests
{
    [Test]
    [TestCase(LatticeOperationState.Queued, false, "Queued")]
    [TestCase(LatticeOperationState.Running, false, "Running")]
    [TestCase(LatticeOperationState.Queued, true, "Cancelling")]
    [TestCase(LatticeOperationState.Running, true, "Cancelling")]
    [TestCase(LatticeOperationState.Succeeded, false, "Succeeded")]
    [TestCase(LatticeOperationState.Failed, false, "Failed")]
    [TestCase(LatticeOperationState.Cancelled, true, "Cancelled")]
    [TestCase((LatticeOperationState)99, false, "Unknown")]
    public void The_state_reads_as_one_word_and_a_cancel_request_reads_cancelling(LatticeOperationState state, bool cancelRequested, string expected)
    {
        var status = OperationTestStatus.Of(state) with { CancelRequested = cancelRequested };

        Assert.That(OperationText.State(status), Is.EqualTo(expected));
    }

    [Test]
    [TestCase(LatticeOperationState.Succeeded, LtStateRole.Healthy)]
    [TestCase(LatticeOperationState.Failed, LtStateRole.Failed)]
    [TestCase(LatticeOperationState.Cancelled, LtStateRole.Stalled)]
    [TestCase(LatticeOperationState.Queued, LtStateRole.Lagging)]
    [TestCase(LatticeOperationState.Running, LtStateRole.Lagging)]
    [TestCase((LatticeOperationState)99, LtStateRole.Unknown)]
    public void Each_state_has_a_pill_role(LatticeOperationState state, LtStateRole expected)
    {
        Assert.That(OperationText.Role(state), Is.EqualTo(expected));
    }

    [Test]
    [TestCase("CapturingMembers", "Capturing members")]
    [TestCase("Restoring", "Restoring")]
    [TestCase("Already words", "Already words")]
    [TestCase("", "")]
    [TestCase(null, "")]
    public void A_phase_name_reads_as_words(string? phase, string expected)
    {
        Assert.That(OperationText.Phase(phase), Is.EqualTo(expected));
    }

    [Test]
    public void Units_read_as_a_count_of_a_total_or_a_count_so_far_and_nothing_without_a_unit()
    {
        var running = OperationTestStatus.Of(LatticeOperationState.Running);

        Assert.Multiple(() =>
        {
            Assert.That(OperationText.Units(running with { UnitName = "entries", CompletedUnits = 1200, TotalUnits = 5000 }), Is.EqualTo("1,200 of 5,000 entries"));
            Assert.That(OperationText.Units(running with { UnitName = "entries", CompletedUnits = 1200 }), Is.EqualTo("1,200 entries so far"));
            Assert.That(OperationText.Units(running with { CompletedUnits = 1200, TotalUnits = 5000 }), Is.Null, "no unit name means no units are reported");
        });
    }

    [Test]
    public void The_step_reads_only_when_the_index_is_inside_the_declared_phases()
    {
        var running = OperationTestStatus.Of(LatticeOperationState.Running);

        Assert.Multiple(() =>
        {
            Assert.That(OperationText.Step(running with { PhaseIndex = 1, PhaseCount = 3 }), Is.EqualTo("Step 2 of 3"));
            Assert.That(OperationText.Step(running with { PhaseIndex = 3, PhaseCount = 3 }), Is.Null);
            Assert.That(OperationText.Step(running with { PhaseIndex = -1, PhaseCount = 3 }), Is.Null);
            Assert.That(OperationText.Step(running with { PhaseIndex = 1 }), Is.Null);
            Assert.That(OperationText.Step(running), Is.Null);
        });
    }

    [Test]
    public void A_count_has_group_separators_whatever_the_culture()
    {
        Assert.That(OperationText.Count(1234567), Is.EqualTo("1,234,567"));
    }

    [Test]
    public void The_status_readers_need_a_status()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => OperationText.State(null!), Throws.ArgumentNullException);
            Assert.That(() => OperationText.Units(null!), Throws.ArgumentNullException);
            Assert.That(() => OperationText.Step(null!), Throws.ArgumentNullException);
        });
    }
}
