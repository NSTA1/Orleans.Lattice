using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class ReplicationMutationObserverTests
{
    private const string DefaultTree = "tree";

    private static IOptionsMonitor<LatticeReplicationOptions> Monitor(string clusterId) =>
        Monitor(new LatticeReplicationOptions { ClusterId = clusterId });

    private static IOptionsMonitor<LatticeReplicationOptions> Monitor(LatticeReplicationOptions options)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    /// <summary>
    /// Permissive resolver used by the unit tests that focus on
    /// per-key filters / origin stamping. Opts every tree id in to
    /// <see cref="ReplicationMode.LwwRegister"/>; tests that exercise mode
    /// resolution itself construct an explicit resolver instead.
    /// </summary>
    private sealed class AllowAllResolver : IReplicationModeResolver
    {
        public ReplicationMode? Resolve(string treeId) => ReplicationMode.LwwRegister;
    }

    private static IReplicationModeResolver AllowAll() => new AllowAllResolver();

    private sealed class CapturingSink : IReplogSink
    {
        public List<ReplogEntry> Entries { get; } = new();
        public Task WriteAsync(ReplogEntry entry, CancellationToken cancellationToken)
        {
            Entries.Add(entry);
            return Task.CompletedTask;
        }
    }

    [Test]
    public async Task Set_mutation_emits_entry_with_value_and_local_origin()
    {
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), AllowAll());
        var ts = HybridLogicalClock.Tick(HybridLogicalClock.Zero);

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = DefaultTree,
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1, 2, 3 },
            Timestamp = ts,
            ExpiresAtTicks = 100L,
        }, CancellationToken.None);

        Assert.That(sink.Entries, Has.Count.EqualTo(1));
        var e = sink.Entries[0];
        Assert.Multiple(() =>
        {
            Assert.That(e.TreeId, Is.EqualTo(DefaultTree));
            Assert.That(e.Op, Is.EqualTo(ReplogOp.Set));
            Assert.That(e.Key, Is.EqualTo("k"));
            Assert.That(e.EndExclusiveKey, Is.Null);
            Assert.That(e.Value, Is.EqualTo(new byte[] { 1, 2, 3 }));
            Assert.That(e.Timestamp, Is.EqualTo(ts));
            Assert.That(e.IsTombstone, Is.False);
            Assert.That(e.ExpiresAtTicks, Is.EqualTo(100L));
            Assert.That(e.OriginClusterId, Is.EqualTo("site-a"));
            Assert.That(e.Mode, Is.EqualTo(ReplicationMode.LwwRegister));
        });
    }

    [Test]
    public async Task Delete_mutation_emits_entry_with_tombstone_flag()
    {
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), AllowAll());

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = DefaultTree,
            Kind = MutationKind.Delete,
            Key = "gone",
            IsTombstone = true,
        }, CancellationToken.None);

        var e = sink.Entries.Single();
        Assert.Multiple(() =>
        {
            Assert.That(e.Op, Is.EqualTo(ReplogOp.Delete));
            Assert.That(e.Key, Is.EqualTo("gone"));
            Assert.That(e.IsTombstone, Is.True);
            Assert.That(e.Value, Is.Null);
            Assert.That(e.OriginClusterId, Is.EqualTo("site-a"));
            Assert.That(e.Mode, Is.EqualTo(ReplicationMode.LwwRegister));
        });
    }

    [Test]
    public async Task DeleteRange_mutation_emits_entry_with_end_exclusive_key()
    {
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), AllowAll());

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = DefaultTree,
            Kind = MutationKind.DeleteRange,
            Key = "a",
            EndExclusiveKey = "z",
            IsTombstone = true,
        }, CancellationToken.None);

        var e = sink.Entries.Single();
        Assert.Multiple(() =>
        {
            Assert.That(e.Op, Is.EqualTo(ReplogOp.DeleteRange));
            Assert.That(e.Key, Is.EqualTo("a"));
            Assert.That(e.EndExclusiveKey, Is.EqualTo("z"));
            Assert.That(e.IsTombstone, Is.True);
        });
    }

    [Test]
    public async Task Existing_origin_cluster_id_is_preserved_for_remote_replays()
    {
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), AllowAll());

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = DefaultTree,
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 7 },
            OriginClusterId = "site-b",
        }, CancellationToken.None);

        Assert.That(sink.Entries.Single().OriginClusterId, Is.EqualTo("site-b"));
    }

    [Test]
    public async Task Local_cluster_id_is_forwarded_verbatim_when_origin_unset()
    {
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-x"), AllowAll());

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = DefaultTree,
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 7 },
        }, CancellationToken.None);

        Assert.That(sink.Entries.Single().OriginClusterId, Is.EqualTo("site-x"));
    }

    [Test]
    public void Unknown_mutation_kind_throws_invalid_operation()
    {
        var observer = new ReplicationMutationObserver(new CapturingSink(), Monitor("site-a"), AllowAll());

        Assert.That(
            async () => await observer.OnMutationAsync(
                new LatticeMutation { TreeId = "t", Kind = (MutationKind)999, Key = "k" },
                CancellationToken.None),
            Throws.InvalidOperationException);
    }

    [Test]
    public async Task Sink_cancellation_token_is_forwarded()
    {
        var sink = Substitute.For<IReplogSink>();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), AllowAll());
        using var cts = new CancellationTokenSource();

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = DefaultTree,
            Kind = MutationKind.Set,
            Key = "k",
            Value = Array.Empty<byte>(),
        }, cts.Token);

        await sink.Received(1).WriteAsync(Arg.Any<ReplogEntry>(), cts.Token);
    }

    [Test]
    public async Task Constructor_throws_on_null_sink()
    {
        await Task.CompletedTask;
        Assert.That(
            () => new ReplicationMutationObserver(null!, Monitor("site-a"), AllowAll()),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task Constructor_throws_on_null_options()
    {
        await Task.CompletedTask;
        Assert.That(
            () => new ReplicationMutationObserver(new CapturingSink(), null!, AllowAll()),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task Constructor_throws_on_null_mode_resolver()
    {
        await Task.CompletedTask;
        Assert.That(
            () => new ReplicationMutationObserver(new CapturingSink(), Monitor("site-a"), null!),
            Throws.ArgumentNullException);
    }

    // ------------------------------------------------------------------
    // R-032 — declared replication mode is the gate
    // ------------------------------------------------------------------

    [Test]
    public async Task Mode_resolver_null_skips_mutation()
    {
        var sink = new CapturingSink();
        var resolver = Substitute.For<IReplicationModeResolver>();
        resolver.Resolve(Arg.Any<string>()).Returns((ReplicationMode?)null);

        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), resolver);

        await observer.OnMutationAsync(SetMutation("undeclared", "k"), CancellationToken.None);

        Assert.That(sink.Entries, Is.Empty);
    }

    [Test]
    public async Task Mode_resolver_value_is_stamped_on_entry()
    {
        var sink = new CapturingSink();
        var resolver = Substitute.For<IReplicationModeResolver>();
        resolver.Resolve("declared").Returns(ReplicationMode.LwwRegister);
        resolver.Resolve("undeclared").Returns((ReplicationMode?)null);

        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), resolver);

        await observer.OnMutationAsync(SetMutation("declared", "k"), CancellationToken.None);
        await observer.OnMutationAsync(SetMutation("undeclared", "k"), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(sink.Entries, Has.Count.EqualTo(1));
            Assert.That(sink.Entries[0].TreeId, Is.EqualTo("declared"));
            Assert.That(sink.Entries[0].Mode, Is.EqualTo(ReplicationMode.LwwRegister));
        });
    }

    [Test]
    public async Task Mode_resolver_is_consulted_per_mutation()
    {
        var sink = new CapturingSink();
        var resolver = Substitute.For<IReplicationModeResolver>();
        resolver.Resolve(Arg.Any<string>()).Returns(ReplicationMode.LwwRegister);

        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), resolver);

        await observer.OnMutationAsync(SetMutation("t", "k1"), CancellationToken.None);
        await observer.OnMutationAsync(SetMutation("t", "k2"), CancellationToken.None);

        resolver.Received(2).Resolve("t");
    }

    // ------------------------------------------------------------------
    // R-012 — producer-side per-key filters (mode resolver permissive)
    // ------------------------------------------------------------------

    [Test]
    public async Task Key_filter_rejecting_predicate_skips_mutation()
    {
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor(new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            KeyFilter = static k => k.StartsWith("ok/"),
        }), AllowAll());

        await observer.OnMutationAsync(SetMutation("t", "ok/1"), CancellationToken.None);
        await observer.OnMutationAsync(SetMutation("t", "skip/1"), CancellationToken.None);

        Assert.That(sink.Entries.Select(e => e.Key).ToArray(), Is.EqualTo(new[] { "ok/1" }));
    }

    [Test]
    public async Task Key_filter_null_replicates_every_key()
    {
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor(new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            KeyFilter = null,
        }), AllowAll());

        await observer.OnMutationAsync(SetMutation("t", "anything"), CancellationToken.None);

        Assert.That(sink.Entries, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task Key_prefixes_empty_imposes_no_restriction()
    {
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor(new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            KeyPrefixes = Array.Empty<string>(),
        }), AllowAll());

        await observer.OnMutationAsync(SetMutation("t", "any"), CancellationToken.None);

        Assert.That(sink.Entries, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task Key_prefixes_match_first_prefix_passes()
    {
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor(new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            KeyPrefixes = new[] { "a/", "b/" },
        }), AllowAll());

        await observer.OnMutationAsync(SetMutation("t", "a/1"), CancellationToken.None);
        await observer.OnMutationAsync(SetMutation("t", "b/2"), CancellationToken.None);
        await observer.OnMutationAsync(SetMutation("t", "c/3"), CancellationToken.None);

        Assert.That(
            sink.Entries.Select(e => e.Key).ToArray(),
            Is.EqualTo(new[] { "a/1", "b/2" }));
    }

    [Test]
    public async Task Key_prefix_match_is_ordinal_case_sensitive()
    {
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor(new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            KeyPrefixes = new[] { "Repl/" },
        }), AllowAll());

        await observer.OnMutationAsync(SetMutation("t", "repl/1"), CancellationToken.None);

        Assert.That(sink.Entries, Is.Empty);
    }

    [Test]
    public async Task Filters_combine_with_logical_and()
    {
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor(new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            KeyFilter = static k => k.EndsWith("-keep"),
            KeyPrefixes = new[] { "x/" },
        }), AllowAll());

        await observer.OnMutationAsync(SetMutation("t", "x/a-keep"), CancellationToken.None);
        await observer.OnMutationAsync(SetMutation("t", "x/a-drop"), CancellationToken.None);
        await observer.OnMutationAsync(SetMutation("t", "y/a-keep"), CancellationToken.None);

        Assert.That(
            sink.Entries.Select(e => e.Key).ToArray(),
            Is.EqualTo(new[] { "x/a-keep" }));
    }

    [Test]
    public async Task Filters_resolve_per_tree_named_options_instance()
    {
        // Configure default options to apply a deny-all key filter, but
        // override the named "vip" tree to allow everything. The observer
        // resolves options via Get(treeId) so the per-tree override wins.
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        var defaults = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            KeyFilter = static _ => false,
        };
        var vip = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
        };
        monitor.CurrentValue.Returns(defaults);
        monitor.Get(Arg.Any<string>()).Returns(defaults);
        monitor.Get("vip").Returns(vip);

        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, monitor, AllowAll());

        await observer.OnMutationAsync(SetMutation("vip", "k"), CancellationToken.None);
        await observer.OnMutationAsync(SetMutation("other", "k"), CancellationToken.None);

        Assert.That(sink.Entries.Select(e => e.TreeId).ToArray(), Is.EqualTo(new[] { "vip" }));
    }

    [Test]
    public async Task Filtered_mutation_does_not_invoke_sink()
    {
        var sink = Substitute.For<IReplogSink>();
        var observer = new ReplicationMutationObserver(sink, Monitor(new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            KeyFilter = static _ => false,
        }), AllowAll());

        await observer.OnMutationAsync(SetMutation("t", "k"), CancellationToken.None);

        await sink.DidNotReceive().WriteAsync(Arg.Any<ReplogEntry>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task DeleteRange_filter_evaluates_against_start_key()
    {
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor(new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            KeyPrefixes = new[] { "a/" },
        }), AllowAll());

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = "t",
            Kind = MutationKind.DeleteRange,
            Key = "a/start",
            EndExclusiveKey = "z/end",
            IsTombstone = true,
        }, CancellationToken.None);

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = "t",
            Kind = MutationKind.DeleteRange,
            Key = "z/start",
            EndExclusiveKey = "z/end",
            IsTombstone = true,
        }, CancellationToken.None);

        Assert.That(sink.Entries.Select(e => e.Key).ToArray(), Is.EqualTo(new[] { "a/start" }));
    }

    // ------------------------------------------------------------------
    // R-012 — compiled-filter cache
    // ------------------------------------------------------------------

    [Test]
    public async Task Options_resolution_is_cached_per_tree_id()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        var opts = new LatticeReplicationOptions { ClusterId = "site-a" };
        monitor.CurrentValue.Returns(opts);
        monitor.Get(Arg.Any<string>()).Returns(opts);

        using var observer = new ReplicationMutationObserver(new CapturingSink(), monitor, AllowAll());

        await observer.OnMutationAsync(SetMutation("t1", "k1"), CancellationToken.None);
        await observer.OnMutationAsync(SetMutation("t1", "k2"), CancellationToken.None);
        await observer.OnMutationAsync(SetMutation("t1", "k3"), CancellationToken.None);
        await observer.OnMutationAsync(SetMutation("t2", "k1"), CancellationToken.None);
        await observer.OnMutationAsync(SetMutation("t2", "k2"), CancellationToken.None);

        // Each tree id resolves the named options instance exactly once; the
        // remaining calls hit the compiled-filter cache.
        Assert.Multiple(() =>
        {
            monitor.Received(1).Get("t1");
            monitor.Received(1).Get("t2");
        });
    }

    [Test]
    public async Task Filter_predicate_runs_per_mutation_even_when_cache_is_warm()
    {
        var calls = 0;
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor(new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            KeyFilter = _ => { calls++; return true; },
        }), AllowAll());

        await observer.OnMutationAsync(SetMutation("t", "k1"), CancellationToken.None);
        await observer.OnMutationAsync(SetMutation("t", "k2"), CancellationToken.None);
        await observer.OnMutationAsync(SetMutation("t", "k3"), CancellationToken.None);

        // The cache snapshots the predicate but does not memoise per-key
        // outcomes; every key is evaluated.
        Assert.Multiple(() =>
        {
            Assert.That(calls, Is.EqualTo(3));
            Assert.That(sink.Entries, Has.Count.EqualTo(3));
        });
    }

    [Test]
    public async Task Filter_cache_is_invalidated_on_options_change()
    {
        Action<LatticeReplicationOptions, string?>? changeCallback = null;
        var subscription = Substitute.For<IDisposable>();
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();

        var deny = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            KeyFilter = static _ => false,
        };
        var allow = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
        };
        var current = deny;
        monitor.CurrentValue.Returns(_ => current);
        monitor.Get(Arg.Any<string>()).Returns(_ => current);
        monitor.OnChange(Arg.Any<Action<LatticeReplicationOptions, string?>>()).Returns(call =>
        {
            changeCallback = call.Arg<Action<LatticeReplicationOptions, string?>>();
            return subscription;
        });

        var sink = new CapturingSink();
        using var observer = new ReplicationMutationObserver(sink, monitor, AllowAll());

        await observer.OnMutationAsync(SetMutation("t", "k"), CancellationToken.None);
        Assert.That(sink.Entries, Is.Empty, "Initial deny-all filter should suppress the write.");

        current = allow;
        Assert.That(changeCallback, Is.Not.Null);
        changeCallback!.Invoke(allow, null);

        await observer.OnMutationAsync(SetMutation("t", "k"), CancellationToken.None);
        Assert.That(sink.Entries, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task Cluster_id_is_snapshotted_until_options_change()
    {
        Action<LatticeReplicationOptions, string?>? changeCallback = null;
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();

        var v1 = new LatticeReplicationOptions { ClusterId = "site-a" };
        var v2 = new LatticeReplicationOptions { ClusterId = "site-b" };
        var current = v1;
        monitor.CurrentValue.Returns(_ => current);
        monitor.Get(Arg.Any<string>()).Returns(_ => current);
        monitor.OnChange(Arg.Any<Action<LatticeReplicationOptions, string?>>()).Returns(call =>
        {
            changeCallback = call.Arg<Action<LatticeReplicationOptions, string?>>();
            return Substitute.For<IDisposable>();
        });

        var sink = new CapturingSink();
        using var observer = new ReplicationMutationObserver(sink, monitor, AllowAll());

        await observer.OnMutationAsync(SetMutation("t", "k1"), CancellationToken.None);

        current = v2;
        await observer.OnMutationAsync(SetMutation("t", "k2"), CancellationToken.None);

        changeCallback!.Invoke(v2, null);
        await observer.OnMutationAsync(SetMutation("t", "k3"), CancellationToken.None);

        Assert.That(
            sink.Entries.Select(e => e.OriginClusterId).ToArray(),
            Is.EqualTo(new[] { "site-a", "site-a", "site-b" }));
    }

    [Test]
    public void Dispose_releases_options_change_subscription()
    {
        var subscription = Substitute.For<IDisposable>();
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.CurrentValue.Returns(new LatticeReplicationOptions { ClusterId = "x" });
        monitor.Get(Arg.Any<string>()).Returns(new LatticeReplicationOptions { ClusterId = "x" });
        monitor.OnChange(Arg.Any<Action<LatticeReplicationOptions, string?>>()).Returns(subscription);

        var observer = new ReplicationMutationObserver(new CapturingSink(), monitor, AllowAll());
        observer.Dispose();

        subscription.Received(1).Dispose();
    }

    [Test]
    public void Dispose_is_idempotent_when_subscription_is_null()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.CurrentValue.Returns(new LatticeReplicationOptions { ClusterId = "x" });
        monitor.Get(Arg.Any<string>()).Returns(new LatticeReplicationOptions { ClusterId = "x" });
        monitor.OnChange(Arg.Any<Action<LatticeReplicationOptions, string?>>()).Returns((IDisposable?)null);

        var observer = new ReplicationMutationObserver(new CapturingSink(), monitor, AllowAll());

        Assert.DoesNotThrow(() => observer.Dispose());
        Assert.DoesNotThrow(() => observer.Dispose());
    }

    [Test]
    public async Task Null_prefix_entries_are_ignored_when_compiling_filter()
    {
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor(new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            KeyPrefixes = new[] { null!, "ok/" },
        }), AllowAll());

        await observer.OnMutationAsync(SetMutation("t", "ok/1"), CancellationToken.None);
        await observer.OnMutationAsync(SetMutation("t", "skip/1"), CancellationToken.None);

        Assert.That(sink.Entries.Select(e => e.Key).ToArray(), Is.EqualTo(new[] { "ok/1" }));
    }

    [Test]
    public async Task All_null_prefix_collection_imposes_no_restriction()
    {
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor(new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            KeyPrefixes = new[] { (string)null!, null! },
        }), AllowAll());

        await observer.OnMutationAsync(SetMutation("t", "anything"), CancellationToken.None);

        Assert.That(sink.Entries, Has.Count.EqualTo(1));
    }

    private static LatticeMutation SetMutation(string treeId, string key) => new()
    {
        TreeId = treeId,
        Kind = MutationKind.Set,
        Key = key,
        Value = new byte[] { 1 },
    };
}

