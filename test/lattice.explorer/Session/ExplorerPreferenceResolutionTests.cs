using Orleans.Lattice.Explorer.Core.Session;

namespace Orleans.Lattice.Explorer.Tests.Session;

/// <summary>
/// The shared shape every restore returns, so no caller has to invent its own
/// fall-back-and-explain handling.
/// </summary>
[TestFixture]
public sealed class ExplorerPreferenceResolutionTests
{
    [Test]
    public void Restored_IsRestoredAndSaysNothing()
    {
        var resolution = ExplorerPreferenceResolution<string>.Restored("orders");

        Assert.Multiple(() =>
        {
            Assert.That(resolution.Value, Is.EqualTo("orders"));
            Assert.That(resolution.IsRestored, Is.True);
            Assert.That(resolution.WasAbandoned, Is.False);
            Assert.That(resolution.Reason, Is.EqualTo(ExplorerPreferenceFallbackReason.None));
            Assert.That(resolution.Explanation, Is.Null);
        });
    }

    [TestCase(ExplorerPreferenceFallbackReason.NotStored)]
    [TestCase(ExplorerPreferenceFallbackReason.NotLoaded)]
    public void FellBack_CarriesTheFallbackAndSaysNothing(ExplorerPreferenceFallbackReason reason)
    {
        var resolution = ExplorerPreferenceResolution<string>.FellBack("explore", reason);

        Assert.Multiple(() =>
        {
            Assert.That(resolution.Value, Is.EqualTo("explore"));
            Assert.That(resolution.IsRestored, Is.False);
            Assert.That(resolution.WasAbandoned, Is.False);
            Assert.That(resolution.Reason, Is.EqualTo(reason));
            Assert.That(resolution.Explanation, Is.Null);
        });
    }

    [Test]
    public void Abandoned_CarriesTheFallbackAndTheExplanation()
    {
        var resolution = ExplorerPreferenceResolution<string>.Abandoned("explore", "it went away");

        Assert.Multiple(() =>
        {
            Assert.That(resolution.Value, Is.EqualTo("explore"));
            Assert.That(resolution.IsRestored, Is.False);
            Assert.That(resolution.WasAbandoned, Is.True);
            Assert.That(resolution.Reason, Is.EqualTo(ExplorerPreferenceFallbackReason.NotResolvable));
            Assert.That(resolution.Explanation, Is.EqualTo("it went away"));
        });
    }

    [Test]
    public void Restored_WithAValueType_CarriesItWithoutBoxingAway()
    {
        var resolution = ExplorerPreferenceResolution<bool>.Restored(true);

        Assert.That(resolution.Value, Is.True);
    }

    [Test]
    public void Restored_WithADefaultValue_IsStillMarkedRestored()
    {
        // A remembered 'false' or a remembered empty string is a real answer, not
        // an absence, so the reason must not be inferred from the value.
        var resolution = ExplorerPreferenceResolution<bool>.Restored(false);

        Assert.Multiple(() =>
        {
            Assert.That(resolution.Value, Is.False);
            Assert.That(resolution.IsRestored, Is.True);
        });
    }

    [Test]
    public void Equality_IsByValue()
    {
        Assert.That(
            ExplorerPreferenceResolution<string>.Restored("a"),
            Is.EqualTo(ExplorerPreferenceResolution<string>.Restored("a")));
    }
}
