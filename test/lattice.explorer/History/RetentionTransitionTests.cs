using Orleans.Lattice;
using Orleans.Lattice.Explorer.Core.History;

namespace Orleans.Lattice.Explorer.Tests.History;

/// <summary>
/// Direct unit tests for <see cref="RetentionTransition"/>: its human-readable
/// label and the retention-shape descriptor across every retention mode, so the
/// History-tab divider text is covered on its own.
/// </summary>
[TestFixture]
public class RetentionTransitionTests
{
    [Test]
    public void Label_describes_both_endpoints()
    {
        var transition = new RetentionTransition
        {
            From = HistoryRetentionMode.FullValue,
            FromValueRetained = true,
            To = HistoryRetentionMode.MetadataOnly,
            ToValueRetained = false,
        };

        Assert.That(transition.Label(), Is.EqualTo("retention changed: full-value -> metadata-only"));
    }

    [Test]
    public void Describe_covers_every_retention_shape()
    {
        Assert.Multiple(() =>
        {
            Assert.That(RetentionTransition.Describe(HistoryRetentionMode.FullValue, true), Is.EqualTo("full-value"));
            Assert.That(RetentionTransition.Describe(HistoryRetentionMode.MetadataOnly, false), Is.EqualTo("metadata-only"));
            Assert.That(RetentionTransition.Describe(HistoryRetentionMode.Hybrid, true), Is.EqualTo("hybrid (value)"));
            Assert.That(RetentionTransition.Describe(HistoryRetentionMode.Hybrid, false), Is.EqualTo("hybrid (metadata)"));
        });
    }
}
