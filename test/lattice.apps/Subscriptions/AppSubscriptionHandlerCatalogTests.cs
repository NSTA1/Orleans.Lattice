using NSubstitute;
using static Orleans.Lattice.Apps.Tests.SubscriptionTestData;

namespace Orleans.Lattice.Apps.Tests;

[TestFixture]
public sealed class AppSubscriptionHandlerCatalogTests
{
    private static readonly IServiceProvider Services = Substitute.For<IServiceProvider>();

    [Test]
    public void TryResolve_creates_the_registered_handler_once()
    {
        var created = 0;
        var catalog = new AppSubscriptionHandlerCatalog(Services, [new(Notes, "feed", _ => { created++; return new RecordingChangeFeedHandler(); })]);

        Assert.That(catalog.TryResolve(Notes, "feed", out var first, out var error), Is.True);
        Assert.That(catalog.TryResolve(Notes, "feed", out var second, out _), Is.True);
        Assert.That(error, Is.Null);
        Assert.That(second, Is.SameAs(first));
        Assert.That(created, Is.EqualTo(1));
    }

    [Test]
    public void TryResolve_reports_an_unregistered_pair()
    {
        var catalog = new AppSubscriptionHandlerCatalog(Services, [new(Notes, "feed", _ => new RecordingChangeFeedHandler())]);

        Assert.That(catalog.TryResolve(Billing, "feed", out var handler, out var error), Is.False);
        Assert.That(handler, Is.Null);
        Assert.That(error, Does.Contain("No change-feed handler").And.Contain("billing"));
    }

    [Test]
    public void TryResolve_refuses_an_ambiguous_pair()
    {
        var catalog = new AppSubscriptionHandlerCatalog(Services,
        [
            new(Notes, "feed", _ => new RecordingChangeFeedHandler()),
            new(Notes, "feed", _ => new RecordingChangeFeedHandler()),
        ]);

        Assert.That(catalog.TryResolve(Notes, "feed", out _, out var error), Is.False);
        Assert.That(error, Does.Contain("More than one"));
    }

    [Test]
    public void TryResolve_reports_a_failing_or_null_factory()
    {
        var catalog = new AppSubscriptionHandlerCatalog(Services,
        [
            new(Notes, "throws", _ => throw new InvalidOperationException("no ctor")),
            new(Notes, "null", _ => null!),
            null!,
        ]);

        Assert.That(catalog.TryResolve(Notes, "throws", out _, out var thrown), Is.False);
        Assert.That(thrown, Does.Contain("no ctor"));
        Assert.That(catalog.TryResolve(Notes, "null", out _, out var nulled), Is.False);
        Assert.That(nulled, Does.Contain("returned null"));
    }

    [Test]
    public void Constructor_rejects_null_arguments()
    {
        Assert.Throws<ArgumentNullException>(() => new AppSubscriptionHandlerCatalog(null!, []));
        Assert.Throws<ArgumentNullException>(() => new AppSubscriptionHandlerCatalog(Services, null!));
    }
}
