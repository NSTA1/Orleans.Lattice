using Orleans.Lattice.Explorer.Core.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Navigation;

/// <summary>
/// The one sentence the epic's state model turns on: an explicit URL always
/// wins; a bare address restores what was remembered.
/// </summary>
[TestFixture]
public sealed class ExplorerShellEntryPolicyTests
{
    private static readonly ExplorerRoute Remembered = ExplorerRoute.Home
        .WithSelection(ExplorerRouteSegments.Trees, "orders")
        .WithSurface("data");

    [Test]
    public void Decide_BareAddressWithSomethingRemembered_Restores()
    {
        var entry = ExplorerShellEntryPolicy.Decide(
            ExplorerRouteStatus.Bare,
            ExplorerRoute.Root,
            Remembered);

        Assert.Multiple(() =>
        {
            Assert.That(entry.Action, Is.EqualTo(ExplorerShellEntryAction.RestoreRemembered));
            Assert.That(entry.Route, Is.SameAs(Remembered));
        });
    }

    [Test]
    public void Decide_BareAddressWithNothingRemembered_ShowsTheAddress()
    {
        var entry = ExplorerShellEntryPolicy.Decide(
            ExplorerRouteStatus.Bare,
            ExplorerRoute.Root,
            ExplorerRoute.Root);

        Assert.Multiple(() =>
        {
            Assert.That(entry.Action, Is.EqualTo(ExplorerShellEntryAction.ShowAddress));
            Assert.That(entry.Route, Is.SameAs(ExplorerRoute.Root));
        });
    }

    [Test]
    public void Decide_ExplicitAddress_OverridesWhatWasRemembered()
    {
        var explicitRoute = ExplorerRoute.Home.WithArea("tenants");

        var entry = ExplorerShellEntryPolicy.Decide(
            ExplorerRouteStatus.Canonical,
            explicitRoute,
            Remembered);

        Assert.Multiple(() =>
        {
            Assert.That(entry.Action, Is.EqualTo(ExplorerShellEntryAction.ShowAddress));
            Assert.That(entry.Route, Is.SameAs(explicitRoute));
        });
    }

    [Test]
    public void Decide_NormalizedAddress_Canonicalizes()
    {
        var entry = ExplorerShellEntryPolicy.Decide(
            ExplorerRouteStatus.Normalized,
            ExplorerRoute.Home,
            Remembered);

        Assert.Multiple(() =>
        {
            Assert.That(entry.Action, Is.EqualTo(ExplorerShellEntryAction.Canonicalize));
            Assert.That(entry.Route, Is.SameAs(ExplorerRoute.Home));
        });
    }

    [Test]
    public void Decide_MalformedAddress_Canonicalizes()
    {
        var entry = ExplorerShellEntryPolicy.Decide(
            ExplorerRouteStatus.Malformed,
            ExplorerRoute.Home,
            Remembered);

        Assert.That(entry.Action, Is.EqualTo(ExplorerShellEntryAction.Canonicalize));
    }

    [Test]
    public void Decide_NullCurrent_Throws()
    {
        Assert.That(
            () => ExplorerShellEntryPolicy.Decide(ExplorerRouteStatus.Bare, null!, Remembered),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Decide_NullRemembered_Throws()
    {
        Assert.That(
            () => ExplorerShellEntryPolicy.Decide(ExplorerRouteStatus.Bare, ExplorerRoute.Root, null!),
            Throws.ArgumentNullException);
    }
}
