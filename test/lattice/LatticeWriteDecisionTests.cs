namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeWriteDecision"/>: the
/// <see cref="LatticeWriteDecision.Accept"/> /
/// <see cref="LatticeWriteDecision.AcceptTransformed"/> /
/// <see cref="LatticeWriteDecision.Reject"/> /
/// <see cref="LatticeWriteDecision.DeadLetter"/> factories and their members.
/// </summary>
[TestFixture]
public class LatticeWriteDecisionTests
{
    [Test]
    public void Accept_has_accept_kind_with_no_payload_or_reason()
    {
        var decision = LatticeWriteDecision.Accept();

        Assert.Multiple(() =>
        {
            Assert.That(decision.Kind, Is.EqualTo(LatticeWriteDecisionKind.Accept));
            Assert.That(decision.TransformedValue, Is.Null);
            Assert.That(decision.Reason, Is.Null);
        });
    }

    [Test]
    public void AcceptTransformed_carries_the_replacement_value()
    {
        var replacement = new byte[] { 9, 8, 7 };

        var decision = LatticeWriteDecision.AcceptTransformed(replacement);

        Assert.Multiple(() =>
        {
            Assert.That(decision.Kind, Is.EqualTo(LatticeWriteDecisionKind.AcceptTransformed));
            Assert.That(decision.TransformedValue, Is.SameAs(replacement));
            Assert.That(decision.Reason, Is.Null);
        });
    }

    [Test]
    public void AcceptTransformed_rejects_a_null_value()
    {
        Assert.That(() => LatticeWriteDecision.AcceptTransformed(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void AcceptTransformed_accepts_an_empty_value()
    {
        var decision = LatticeWriteDecision.AcceptTransformed([]);

        Assert.Multiple(() =>
        {
            Assert.That(decision.Kind, Is.EqualTo(LatticeWriteDecisionKind.AcceptTransformed));
            Assert.That(decision.TransformedValue, Is.Empty);
        });
    }

    [Test]
    public void Reject_has_reject_kind_and_carries_the_reason()
    {
        var decision = LatticeWriteDecision.Reject("schema mismatch");

        Assert.Multiple(() =>
        {
            Assert.That(decision.Kind, Is.EqualTo(LatticeWriteDecisionKind.Reject));
            Assert.That(decision.Reason, Is.EqualTo("schema mismatch"));
            Assert.That(decision.TransformedValue, Is.Null);
        });
    }

    [Test]
    public void Reject_rejects_a_null_reason()
    {
        Assert.That(() => LatticeWriteDecision.Reject(null!), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void Reject_rejects_an_empty_reason()
    {
        Assert.That(() => LatticeWriteDecision.Reject(string.Empty), Throws.ArgumentException);
    }

    [Test]
    public void DeadLetter_has_dead_letter_kind_and_carries_the_reason()
    {
        var decision = LatticeWriteDecision.DeadLetter("quarantined");

        Assert.Multiple(() =>
        {
            Assert.That(decision.Kind, Is.EqualTo(LatticeWriteDecisionKind.DeadLetter));
            Assert.That(decision.Reason, Is.EqualTo("quarantined"));
            Assert.That(decision.TransformedValue, Is.Null);
        });
    }

    [Test]
    public void DeadLetter_rejects_a_null_reason()
    {
        Assert.That(() => LatticeWriteDecision.DeadLetter(null!), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void DeadLetter_rejects_an_empty_reason()
    {
        Assert.That(() => LatticeWriteDecision.DeadLetter(string.Empty), Throws.ArgumentException);
    }
}
