namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the null-default merge seams
/// (<see cref="NullLatticeValueDecoder"/> and
/// <see cref="NullLatticeMergeObserver"/>): they must be inert - the decoder
/// never activates and returns the stored bytes unchanged, the observer always
/// accepts verbatim - so the read/merge paths stay byte-for-byte identical when
/// no schema add-on is registered.
/// </summary>
[TestFixture]
public class NullMergeSeamsTests
{
    [Test]
    public void NullValueDecoder_is_never_active()
    {
        var decoder = new NullLatticeValueDecoder();

        Assert.That(decoder.IsActive("any-tree"), Is.False);
        Assert.That(decoder.IsActive(string.Empty), Is.False);
    }

    [Test]
    public async Task NullValueDecoder_decode_returns_stored_bytes_unchanged()
    {
        var decoder = new NullLatticeValueDecoder();
        var stored = new byte[] { 1, 2, 3 };

        var decoded = await decoder.DecodeAsync("t", stored, CancellationToken.None);

        Assert.That(decoded, Is.SameAs(stored));
    }

    [Test]
    public async Task NullMergeObserver_accepts_verbatim()
    {
        var observer = new NullLatticeMergeObserver();
        var ctx = new LatticeMergeContext("k", LatticeMergeMode.LwwRegister, null, null, new byte[] { 9 });

        var outcome = await observer.OnMergedAsync(in ctx, CancellationToken.None);

        Assert.That(outcome.Kind, Is.EqualTo(MergeOutcomeKind.Accept));
        Assert.That(outcome.TransformedValue, Is.Null);
        Assert.That(outcome.EventReason, Is.Null);
    }
}
