using Orleans.Lattice.BPlusTree.Grains;
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
    public void Resolve_distinct_tree_ids_do_not_grow_the_cache_without_bound()
    {
        using var resolver = new ConfiguredLatticeMergeModeResolver(
            Monitor(_ => new LatticeReplicationOptions { ClusterId = "x", ReplicatedTrees = null }));

        // The key is a caller-supplied tree id and an unreplicated id still caches
        // a null sentinel, so an unbounded cache grew silo memory without limit as
        // distinct tree ids accumulated (CWE-770).
        for (var i = 0; i < ConfiguredLatticeMergeModeResolver.MaxCachedTrees + 500; i++)
        {
            Assert.That(resolver.Resolve("tree-" + i), Is.Null);
        }

        Assert.That(
            resolver.CachedTreeCount,
            Is.LessThanOrEqualTo(ConfiguredLatticeMergeModeResolver.MaxCachedTrees));
    }

    [Test]
    public void Resolve_still_returns_the_configured_mode_once_the_cache_is_full()
    {
        using var resolver = new ConfiguredLatticeMergeModeResolver(Monitor(_ => new LatticeReplicationOptions
        {
            ClusterId = "x",
            ReplicatedTrees = new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal)
            {
                ["orders"] = LatticeMergeMode.OrSet,
            },
        }));

        for (var i = 0; i < ConfiguredLatticeMergeModeResolver.MaxCachedTrees + 10; i++)
        {
            _ = resolver.Resolve("filler-" + i);
        }

        // A refused insert must only cost an options read, never change the answer.
        Assert.That(resolver.Resolve("orders"), Is.EqualTo(LatticeMergeMode.OrSet));
    }

    [Test]
    public void Origin_resolver_distinct_tree_ids_do_not_grow_the_cache_without_bound()
    {
        using var resolver = new ConfiguredLatticeOriginClusterIdResolver(
            Monitor(_ => new LatticeReplicationOptions { ClusterId = "cluster-a" }));

        for (var i = 0; i < ConfiguredLatticeOriginClusterIdResolver.MaxCachedTrees + 500; i++)
        {
            Assert.That(resolver.Resolve("tree-" + i), Is.EqualTo("cluster-a"));
        }

        Assert.That(
            resolver.CachedTreeCount,
            Is.LessThanOrEqualTo(ConfiguredLatticeOriginClusterIdResolver.MaxCachedTrees));
    }

    [Test]
    public void Constructor_throws_on_null_options()
    {
        Assert.That(() => new ConfiguredLatticeMergeModeResolver(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Resolve_throws_on_null_tree_id()
    {
        using var resolver = new ConfiguredLatticeMergeModeResolver(Monitor(_ => new LatticeReplicationOptions { ClusterId = "x" }));
        Assert.That(() => resolver.Resolve(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Resolve_returns_null_when_replicated_trees_is_null()
    {
        using var resolver = new ConfiguredLatticeMergeModeResolver(Monitor(_ => new LatticeReplicationOptions
        {
            ClusterId = "x",
            ReplicatedTrees = null,
        }));

        Assert.That(resolver.Resolve("anything"), Is.Null);
    }

    [Test]
    public void Resolve_returns_null_for_undeclared_tree()
    {
        using var resolver = new ConfiguredLatticeMergeModeResolver(Monitor(_ => new LatticeReplicationOptions
        {
            ClusterId = "x",
            ReplicatedTrees = new Dictionary<string, LatticeMergeMode>
            {
                ["declared"] = LatticeMergeMode.OrSet,
            },
        }));

        Assert.That(resolver.Resolve("undeclared"), Is.Null);
    }

    [Test]
    public void Resolve_returns_declared_mode()
    {
        using var resolver = new ConfiguredLatticeMergeModeResolver(Monitor(_ => new LatticeReplicationOptions
        {
            ClusterId = "x",
            ReplicatedTrees = new Dictionary<string, LatticeMergeMode>
            {
                ["t"] = LatticeMergeMode.OrSet,
            },
        }));

        Assert.That(resolver.Resolve("t"), Is.EqualTo(LatticeMergeMode.OrSet));
    }

    [Test]
    public void Resolve_caches_outcome_per_tree_id()
    {
        var calls = 0;
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "x",
            ReplicatedTrees = new Dictionary<string, LatticeMergeMode>
            {
                ["t"] = LatticeMergeMode.OrSet,
            },
        };
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(_ => { calls++; return opts; });
        monitor.OnChange(Arg.Any<Action<LatticeReplicationOptions, string?>>()).Returns(Substitute.For<IDisposable>());

        using var resolver = new ConfiguredLatticeMergeModeResolver(monitor);

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
            ReplicatedTrees = new Dictionary<string, LatticeMergeMode>
            {
                ["t1"] = LatticeMergeMode.OrSet,
            },
        });
        monitor.OnChange(Arg.Any<Action<LatticeReplicationOptions, string?>>()).Returns(Substitute.For<IDisposable>());

        using var resolver = new ConfiguredLatticeMergeModeResolver(monitor);

        Assert.Multiple(() =>
        {
            Assert.That(resolver.Resolve("t1"), Is.EqualTo(LatticeMergeMode.OrSet));
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
            ReplicatedTrees = new Dictionary<string, LatticeMergeMode>
            {
                ["t"] = LatticeMergeMode.OrSet,
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

        using var resolver = new ConfiguredLatticeMergeModeResolver(monitor);

        Assert.That(resolver.Resolve("t"), Is.Null);

        current = allow;
        Assert.That(changeCallback, Is.Not.Null);
        changeCallback!.Invoke(allow, null);

        Assert.That(resolver.Resolve("t"), Is.EqualTo(LatticeMergeMode.OrSet));
    }

    [Test]
    public void Dispose_releases_options_change_subscription()
    {
        var subscription = Substitute.For<IDisposable>();
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(new LatticeReplicationOptions { ClusterId = "x" });
        monitor.OnChange(Arg.Any<Action<LatticeReplicationOptions, string?>>()).Returns(subscription);

        var resolver = new ConfiguredLatticeMergeModeResolver(monitor);
        resolver.Dispose();

        subscription.Received(1).Dispose();
    }

    [Test]
    public void Dispose_is_idempotent_when_subscription_is_null()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(new LatticeReplicationOptions { ClusterId = "x" });
        monitor.OnChange(Arg.Any<Action<LatticeReplicationOptions, string?>>()).Returns((IDisposable?)null);

        var resolver = new ConfiguredLatticeMergeModeResolver(monitor);

        Assert.DoesNotThrow(() => resolver.Dispose());
        Assert.DoesNotThrow(() => resolver.Dispose());
    }
}
