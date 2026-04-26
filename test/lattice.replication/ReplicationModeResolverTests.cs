using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class ReplicationModeResolverTests
{
    private static IOptionsMonitor<LatticeReplicationOptions> Monitor(
        Func<string, LatticeReplicationOptions> getter,
        Action<Action<LatticeReplicationOptions, string?>>? captureChange = null,
        IDisposable? subscription = null)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(call => getter(call.Arg<string>()));
        monitor.OnChange(Arg.Any<Action<LatticeReplicationOptions, string?>>()).Returns(call =>
        {
            captureChange?.Invoke(call.Arg<Action<LatticeReplicationOptions, string?>>());
            return subscription ?? Substitute.For<IDisposable>();
        });
        return monitor;
    }

    [Test]
    public void Constructor_throws_on_null_options()
    {
        Assert.That(() => new ReplicationModeResolver(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Resolve_throws_on_null_tree_id()
    {
        using var resolver = new ReplicationModeResolver(Monitor(_ => new LatticeReplicationOptions { ClusterId = "x" }));
        Assert.That(() => resolver.Resolve(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Resolve_returns_null_when_replicated_trees_is_null()
    {
        using var resolver = new ReplicationModeResolver(Monitor(_ => new LatticeReplicationOptions
        {
            ClusterId = "x",
            ReplicatedTrees = null,
        }));

        Assert.That(resolver.Resolve("anything"), Is.Null);
    }

    [Test]
    public void Resolve_returns_null_for_undeclared_tree()
    {
        using var resolver = new ReplicationModeResolver(Monitor(_ => new LatticeReplicationOptions
        {
            ClusterId = "x",
            ReplicatedTrees = new Dictionary<string, ReplicationMode>
            {
                ["declared"] = ReplicationMode.LwwRegister,
            },
        }));

        Assert.That(resolver.Resolve("undeclared"), Is.Null);
    }

    [Test]
    public void Resolve_returns_declared_mode()
    {
        using var resolver = new ReplicationModeResolver(Monitor(_ => new LatticeReplicationOptions
        {
            ClusterId = "x",
            ReplicatedTrees = new Dictionary<string, ReplicationMode>
            {
                ["t"] = ReplicationMode.LwwRegister,
            },
        }));

        Assert.That(resolver.Resolve("t"), Is.EqualTo(ReplicationMode.LwwRegister));
    }

    [Test]
    public void Resolve_caches_outcome_per_tree_id()
    {
        var calls = 0;
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "x",
            ReplicatedTrees = new Dictionary<string, ReplicationMode>
            {
                ["t"] = ReplicationMode.LwwRegister,
            },
        };
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(_ => { calls++; return opts; });
        monitor.OnChange(Arg.Any<Action<LatticeReplicationOptions, string?>>()).Returns(Substitute.For<IDisposable>());

        using var resolver = new ReplicationModeResolver(monitor);

        resolver.Resolve("t");
        resolver.Resolve("t");
        resolver.Resolve("t");

        Assert.That(calls, Is.EqualTo(1));
    }

    [Test]
    public void Resolve_caches_independently_per_tree_id()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(new LatticeReplicationOptions
        {
            ClusterId = "x",
            ReplicatedTrees = new Dictionary<string, ReplicationMode>
            {
                ["t1"] = ReplicationMode.LwwRegister,
            },
        });
        monitor.OnChange(Arg.Any<Action<LatticeReplicationOptions, string?>>()).Returns(Substitute.For<IDisposable>());

        using var resolver = new ReplicationModeResolver(monitor);

        Assert.Multiple(() =>
        {
            Assert.That(resolver.Resolve("t1"), Is.EqualTo(ReplicationMode.LwwRegister));
            Assert.That(resolver.Resolve("t2"), Is.Null);
        });
    }

    [Test]
    public void OnChange_invalidates_cache()
    {
        Action<LatticeReplicationOptions, string?>? changeCallback = null;
        var deny = new LatticeReplicationOptions { ClusterId = "x", ReplicatedTrees = null };
        var allow = new LatticeReplicationOptions
        {
            ClusterId = "x",
            ReplicatedTrees = new Dictionary<string, ReplicationMode>
            {
                ["t"] = ReplicationMode.LwwRegister,
            },
        };
        var current = deny;
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(_ => current);
        monitor.OnChange(Arg.Any<Action<LatticeReplicationOptions, string?>>()).Returns(call =>
        {
            changeCallback = call.Arg<Action<LatticeReplicationOptions, string?>>();
            return Substitute.For<IDisposable>();
        });

        using var resolver = new ReplicationModeResolver(monitor);

        Assert.That(resolver.Resolve("t"), Is.Null);

        current = allow;
        Assert.That(changeCallback, Is.Not.Null);
        changeCallback!.Invoke(allow, null);

        Assert.That(resolver.Resolve("t"), Is.EqualTo(ReplicationMode.LwwRegister));
    }

    [Test]
    public void Dispose_releases_options_change_subscription()
    {
        var subscription = Substitute.For<IDisposable>();
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(new LatticeReplicationOptions { ClusterId = "x" });
        monitor.OnChange(Arg.Any<Action<LatticeReplicationOptions, string?>>()).Returns(subscription);

        var resolver = new ReplicationModeResolver(monitor);
        resolver.Dispose();

        subscription.Received(1).Dispose();
    }

    [Test]
    public void Dispose_is_idempotent_when_subscription_is_null()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(new LatticeReplicationOptions { ClusterId = "x" });
        monitor.OnChange(Arg.Any<Action<LatticeReplicationOptions, string?>>()).Returns((IDisposable?)null);

        var resolver = new ReplicationModeResolver(monitor);

        Assert.DoesNotThrow(() => resolver.Dispose());
        Assert.DoesNotThrow(() => resolver.Dispose());
    }
}
