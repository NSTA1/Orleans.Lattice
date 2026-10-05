using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree;

[TestFixture]
public sealed class LatticeFreshStampContextTests
{
    [TearDown]
    public void TearDown() => RequestContext.Clear();

    [Test]
    public void A_scope_marks_the_override_fresh_and_restores_the_previous_marking()
    {
        Assert.That(LatticeFreshStampContext.IsActive, Is.False);
        using (LatticeFreshStampContext.Begin())
        {
            Assert.That(LatticeFreshStampContext.IsActive, Is.True);
            using (LatticeFreshStampContext.Begin())
            {
                Assert.That(LatticeFreshStampContext.IsActive, Is.True);
            }

            Assert.That(LatticeFreshStampContext.IsActive, Is.True, "an inner scope restores the outer marking");
        }

        Assert.That(LatticeFreshStampContext.IsActive, Is.False);
    }
}
