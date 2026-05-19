namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class ReceiverFlowControlHintTests
{
    [Test]
    public void None_is_default_value()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ReceiverFlowControlHint.None.SuggestedBatchSize, Is.Null);
            Assert.That(ReceiverFlowControlHint.None.PauseForMs, Is.Null);
            Assert.That(ReceiverFlowControlHint.None, Is.EqualTo(default(ReceiverFlowControlHint)));
        });
    }

    [Test]
    public void Init_assigns_both_fields()
    {
        var hint = new ReceiverFlowControlHint
        {
            SuggestedBatchSize = 12,
            PauseForMs = 750,
        };

        Assert.Multiple(() =>
        {
            Assert.That(hint.SuggestedBatchSize, Is.EqualTo(12));
            Assert.That(hint.PauseForMs, Is.EqualTo(750));
        });
    }

    [Test]
    public void Equality_uses_value_semantics()
    {
        var a = new ReceiverFlowControlHint { SuggestedBatchSize = 1, PauseForMs = 1 };
        var b = new ReceiverFlowControlHint { SuggestedBatchSize = 1, PauseForMs = 1 };
        var c = new ReceiverFlowControlHint { SuggestedBatchSize = 2, PauseForMs = 1 };
        var d = new ReceiverFlowControlHint { SuggestedBatchSize = 1, PauseForMs = 2 };

        Assert.Multiple(() =>
        {
            Assert.That(a, Is.EqualTo(b));
            Assert.That(a, Is.Not.EqualTo(c));
            Assert.That(a, Is.Not.EqualTo(d));
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }
}
