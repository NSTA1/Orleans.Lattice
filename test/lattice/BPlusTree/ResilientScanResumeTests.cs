namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Covers <see cref="ResilientScanResume"/>, the resume policy every resilient
/// scan wrapper applies when it reopens a faulted stream.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class ResilientScanResumeTests
{
    [Test]
    public void Bounds_keeps_the_original_range_before_any_key_is_yielded()
    {
        Assert.That(ResilientScanResume.Bounds("a", "z", null, reverse: false), Is.EqualTo(("a", "z")));
    }

    [Test]
    public void Bounds_resumes_a_forward_scan_after_the_last_key()
    {
        Assert.That(ResilientScanResume.Bounds("a", "z", "m", reverse: false), Is.EqualTo(("m\u0000", "z")));
    }

    [Test]
    public void Bounds_resumes_a_reverse_scan_below_the_last_key()
    {
        Assert.That(ResilientScanResume.Bounds("a", "z", "m", reverse: true), Is.EqualTo(("a", "m")));
    }

    [Test]
    public void ReconnectDelayMs_is_immediate_first_then_ramps_to_a_cap()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ResilientScanResume.ReconnectDelayMs(1), Is.Zero);
            Assert.That(ResilientScanResume.ReconnectDelayMs(2), Is.EqualTo(20));
            Assert.That(ResilientScanResume.ReconnectDelayMs(50), Is.EqualTo(100));
        });
    }
}
