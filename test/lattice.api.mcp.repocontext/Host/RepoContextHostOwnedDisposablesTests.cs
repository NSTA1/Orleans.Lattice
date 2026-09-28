using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Unit coverage for <see cref="RepoContextHostOwnedDisposables"/>, the owner that
/// ties the host builder's eagerly-constructed listeners and meters to the host's
/// lifetime (issue #3792).
/// </summary>
[TestFixture]
public sealed class RepoContextHostOwnedDisposablesTests
{
    [Test]
    public void Add_returns_the_instance_it_takes_ownership_of()
    {
        using var owner = new RepoContextHostOwnedDisposables();
        var probe = new Probe("a", []);

        Assert.Multiple(() =>
        {
            Assert.That(owner.Add(probe), Is.SameAs(probe));
            Assert.That(owner.Count, Is.EqualTo(1));
        });
    }

    [Test]
    public void Add_rejects_null()
    {
        using var owner = new RepoContextHostOwnedDisposables();

        Assert.Throws<ArgumentNullException>(() => owner.Add<IDisposable>(null!));
    }

    [Test]
    public void Dispose_releases_every_owned_instance_most_recent_first()
    {
        var order = new List<string>();
        var owner = new RepoContextHostOwnedDisposables();
        owner.Add(new Probe("first", order));
        owner.Add(new Probe("second", order));
        owner.Add(new Probe("third", order));

        owner.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(order, Is.EqualTo(new[] { "third", "second", "first" }));
            Assert.That(owner.Count, Is.Zero);
        });
    }

    [Test]
    public void Dispose_is_idempotent()
    {
        var order = new List<string>();
        var owner = new RepoContextHostOwnedDisposables();
        owner.Add(new Probe("only", order));

        owner.Dispose();
        owner.Dispose();

        Assert.That(order, Is.EqualTo(new[] { "only" }), "A second Dispose must not release an instance again.");
    }

    [Test]
    public void Add_after_dispose_is_refused()
    {
        var owner = new RepoContextHostOwnedDisposables();
        owner.Dispose();

        Assert.Throws<ObjectDisposedException>(() => owner.Add(new Probe("late", [])));
    }

    [Test]
    public void A_throwing_instance_does_not_stop_the_others_being_released()
    {
        var order = new List<string>();
        var owner = new RepoContextHostOwnedDisposables();
        owner.Add(new Probe("first", order));
        owner.Add(new Probe("throws", order, fail: true));
        owner.Add(new Probe("third", order));

        var thrown = Assert.Throws<AggregateException>(owner.Dispose);

        Assert.Multiple(() =>
        {
            Assert.That(order, Is.EqualTo(new[] { "third", "throws", "first" }));
            Assert.That(thrown!.InnerExceptions, Has.Count.EqualTo(1));
            Assert.That(thrown.InnerExceptions[0], Is.TypeOf<InvalidOperationException>());
        });
    }

    private sealed class Probe(string name, List<string> order, bool fail = false) : IDisposable
    {
        public void Dispose()
        {
            order.Add(name);
            if (fail)
            {
                throw new InvalidOperationException(name);
            }
        }
    }
}
