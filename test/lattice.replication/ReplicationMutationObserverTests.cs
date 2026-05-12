using Orleans.Lattice.BPlusTree.Grains;
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
    /// <see cref="LatticeMergeMode.LwwRegister"/>; tests that exercise mode
    /// resolution itself construct an explicit resolver instead.
    /// </summary>
    private sealed class AllowAllResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) => LatticeMergeMode.LwwRegister;
    }

    private static ILatticeMergeModeResolver AllowAll() => new AllowAllResolver();

    private sealed class CapturingSink : IReplogSink
    {
        public List<WalRecord> Entries { get; } = new();
        public Task WriteAsync(WalRecord entry, CancellationToken cancellationToken)
        {
            Entries.Add(entry);
            return Task.CompletedTask;
        }
    }

    [Test]
    public async Task Set_mutation_emits_entry_with_value_and_local_origin()
    {
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), AllowAll(), new LocalVectorClockCache(Substitute.For<IGrainFactory>()));
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
            Assert.That(e.Op, Is.EqualTo(MutationKind.Set));
            Assert.That(e.Key, Is.EqualTo("k"));
            Assert.That(e.EndExclusiveKey, Is.Null);
            Assert.That(e.Value, Is.EqualTo(new byte[] { 1, 2, 3 }));
            Assert.That(e.Timestamp, Is.EqualTo(ts));
            Assert.That(e.IsTombstone, Is.False);
            Assert.That(e.ExpiresAtTicks, Is.EqualTo(100L));
            Assert.That(e.OriginClusterId, Is.EqualTo("site-a"));
            Assert.That(e.Mode, Is.EqualTo(LatticeMergeMode.LwwRegister));
        });
    }

    [Test]
    public async Task Delete_mutation_emits_entry_with_tombstone_flag()
    {
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), AllowAll(), new LocalVectorClockCache(Substitute.For<IGrainFactory>()));

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
            Assert.That(e.Op, Is.EqualTo(MutationKind.Delete));
            Assert.That(e.Key, Is.EqualTo("gone"));
            Assert.That(e.IsTombstone, Is.True);
            Assert.That(e.Value, Is.Null);
            Assert.That(e.OriginClusterId, Is.EqualTo("site-a"));
            Assert.That(e.Mode, Is.EqualTo(LatticeMergeMode.LwwRegister));
        });
    }

    [Test]
    public async Task DeleteRange_mutation_emits_entry_with_end_exclusive_key()
    {
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), AllowAll(), new LocalVectorClockCache(Substitute.For<IGrainFactory>()));

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
            Assert.That(e.Op, Is.EqualTo(MutationKind.DeleteRange));
            Assert.That(e.Key, Is.EqualTo("a"));
            Assert.That(e.EndExclusiveKey, Is.EqualTo("z"));
            Assert.That(e.IsTombstone, Is.True);
        });
    }

    [Test]
    public async Task Existing_origin_cluster_id_is_preserved_for_remote_replays()
    {
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), AllowAll(), new LocalVectorClockCache(Substitute.For<IGrainFactory>()));

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
        var observer = new ReplicationMutationObserver(sink, Monitor("site-x"), AllowAll(), new LocalVectorClockCache(Substitute.For<IGrainFactory>()));

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
        var observer = new ReplicationMutationObserver(new CapturingSink(), Monitor("site-a"), AllowAll(), new LocalVectorClockCache(Substitute.For<IGrainFactory>()));

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
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), AllowAll(), new LocalVectorClockCache(Substitute.For<IGrainFactory>()));
        using var cts = new CancellationTokenSource();

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = DefaultTree,
            Kind = MutationKind.Set,
            Key = "k",
            Value = Array.Empty<byte>(),
        }, cts.Token);

        await sink.Received(1).WriteAsync(Arg.Any<WalRecord>(), cts.Token);
    }

    [Test]
    public async Task Constructor_throws_on_null_sink()
    {
        await Task.CompletedTask;
        Assert.That(
            () => new ReplicationMutationObserver(null!, Monitor("site-a"), AllowAll(), new LocalVectorClockCache(Substitute.For<IGrainFactory>())),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task Constructor_throws_on_null_options()
    {
        await Task.CompletedTask;
        Assert.That(
            () => new ReplicationMutationObserver(new CapturingSink(), null!, AllowAll(), new LocalVectorClockCache(Substitute.For<IGrainFactory>())),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task Constructor_throws_on_null_mode_resolver()
    {
        await Task.CompletedTask;
        Assert.That(
            () => new ReplicationMutationObserver(new CapturingSink(), Monitor("site-a"), null!, new LocalVectorClockCache(Substitute.For<IGrainFactory>())),
            Throws.ArgumentNullException);
    }

    // ------------------------------------------------------------------
    // R-032 - declared replication mode is the gate
    // ------------------------------------------------------------------

    [Test]
    public async Task Mode_resolver_null_skips_mutation()
    {
        var sink = new CapturingSink();
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        resolver.Resolve(Arg.Any<string>()).Returns((LatticeMergeMode?)null);

        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), resolver, new LocalVectorClockCache(Substitute.For<IGrainFactory>()));

        await observer.OnMutationAsync(SetMutation("undeclared", "k"), CancellationToken.None);

        Assert.That(sink.Entries, Is.Empty);
    }

    [Test]
    public async Task Mode_resolver_value_is_stamped_on_entry()
    {
        var sink = new CapturingSink();
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        resolver.Resolve("declared").Returns(LatticeMergeMode.LwwRegister);
        resolver.Resolve("undeclared").Returns((LatticeMergeMode?)null);

        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), resolver, new LocalVectorClockCache(Substitute.For<IGrainFactory>()));

        await observer.OnMutationAsync(SetMutation("declared", "k"), CancellationToken.None);
        await observer.OnMutationAsync(SetMutation("undeclared", "k"), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(sink.Entries, Has.Count.EqualTo(1));
            Assert.That(sink.Entries[0].TreeId, Is.EqualTo("declared"));
            Assert.That(sink.Entries[0].Mode, Is.EqualTo(LatticeMergeMode.LwwRegister));
        });
    }

    [Test]
    public async Task Mode_resolver_is_consulted_per_mutation()
    {
        var sink = new CapturingSink();
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        resolver.Resolve(Arg.Any<string>()).Returns(LatticeMergeMode.LwwRegister);

        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), resolver, new LocalVectorClockCache(Substitute.For<IGrainFactory>()));

        await observer.OnMutationAsync(SetMutation("t", "k1"), CancellationToken.None);
        await observer.OnMutationAsync(SetMutation("t", "k2"), CancellationToken.None);

        resolver.Received(2).Resolve("t");
    }

    // ------------------------------------------------------------------
    // R-012 - producer-side per-key filters (mode resolver permissive)
    // ------------------------------------------------------------------

    [Test]
    public async Task Key_filter_rejecting_predicate_skips_mutation()
    {
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor(new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            KeyFilter = static k => k.StartsWith("ok/"),
        }), AllowAll(), new LocalVectorClockCache(Substitute.For<IGrainFactory>()));

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
        }), AllowAll(), new LocalVectorClockCache(Substitute.For<IGrainFactory>()));

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
        }), AllowAll(), new LocalVectorClockCache(Substitute.For<IGrainFactory>()));

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
        }), AllowAll(), new LocalVectorClockCache(Substitute.For<IGrainFactory>()));

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
        }), AllowAll(), new LocalVectorClockCache(Substitute.For<IGrainFactory>()));

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
        }), AllowAll(), new LocalVectorClockCache(Substitute.For<IGrainFactory>()));

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
        var observer = new ReplicationMutationObserver(sink, monitor, AllowAll(), new LocalVectorClockCache(Substitute.For<IGrainFactory>()));

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
        }), AllowAll(), new LocalVectorClockCache(Substitute.For<IGrainFactory>()));

        await observer.OnMutationAsync(SetMutation("t", "k"), CancellationToken.None);

        await sink.DidNotReceive().WriteAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task DeleteRange_filter_evaluates_against_start_key()
    {
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor(new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            KeyPrefixes = new[] { "a/" },
        }), AllowAll(), new LocalVectorClockCache(Substitute.For<IGrainFactory>()));

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
    // R-012 - compiled-filter cache
    // ------------------------------------------------------------------

    [Test]
    public async Task Options_resolution_is_cached_per_tree_id()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        var opts = new LatticeReplicationOptions { ClusterId = "site-a" };
        monitor.CurrentValue.Returns(opts);
        monitor.Get(Arg.Any<string>()).Returns(opts);

        using var observer = new ReplicationMutationObserver(new CapturingSink(), monitor, AllowAll(), new LocalVectorClockCache(Substitute.For<IGrainFactory>()));

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
        }), AllowAll(), new LocalVectorClockCache(Substitute.For<IGrainFactory>()));

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
        using var observer = new ReplicationMutationObserver(sink, monitor, AllowAll(), new LocalVectorClockCache(Substitute.For<IGrainFactory>()));

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
        using var observer = new ReplicationMutationObserver(sink, monitor, AllowAll(), new LocalVectorClockCache(Substitute.For<IGrainFactory>()));

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

        var observer = new ReplicationMutationObserver(new CapturingSink(), monitor, AllowAll(), new LocalVectorClockCache(Substitute.For<IGrainFactory>()));
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

        var observer = new ReplicationMutationObserver(new CapturingSink(), monitor, AllowAll(), new LocalVectorClockCache(Substitute.For<IGrainFactory>()));

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
        }), AllowAll(), new LocalVectorClockCache(Substitute.For<IGrainFactory>()));

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
        }), AllowAll(), new LocalVectorClockCache(Substitute.For<IGrainFactory>()));

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

    // ------------------------------------------------------------------
    // Causal-plus vector-clock stamping (commit-time frontier capture)
    // ------------------------------------------------------------------

    [Test]
    public async Task Vector_clock_from_mutation_is_stamped_on_emitted_entry()
    {
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), AllowAll(), new LocalVectorClockCache(Substitute.For<IGrainFactory>()));
        var vc = new VersionVector();
        var ticked = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        vc.Entries["site-a"] = ticked;

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = "t",
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1 },
            VectorClock = vc,
        }, CancellationToken.None);

        var entry = sink.Entries.Single();
        Assert.Multiple(() =>
        {
            // The captured frontier is a defensive clone of the
            // mutation's VectorClock - never the same reference.
            Assert.That(entry.VectorClock, Is.Not.SameAs(vc));
            Assert.That(entry.VectorClock, Is.Not.Null);
            Assert.That(entry.VectorClock!.GetClock("site-a"), Is.EqualTo(ticked));
            Assert.That(entry.VectorClock.Entries, Has.Count.EqualTo(1));
            // Inter-slot aliasing is preserved per spec: both slots
            // share the single clone instance.
            Assert.That(entry.DependencySummary, Is.SameAs(entry.VectorClock));
        });
    }

    [Test]
    public async Task Null_vector_clock_on_mutation_flows_back_to_local_cache_snapshot()
    {
        // Under the producer-side local vector clock cache, a mutation
        // that does not carry an explicit VectorClock falls back to
        // the cache's tree-global snapshot rather than emitting a
        // null frontier. With a fresh cache (no prior local writes,
        // no prior foreign applies, an empty cold-start snapshot
        // from the substituted grain factory) the snapshot is a
        // non-null empty VersionVector - exactly what a remote
        // receiver's dep-check expects when no causal predecessors
        // have been declared.
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), AllowAll(), new LocalVectorClockCache(Substitute.For<IGrainFactory>()));

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = "t",
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1 },
            VectorClock = null,
        }, CancellationToken.None);

        var entry = sink.Entries.Single();
        Assert.Multiple(() =>
        {
            Assert.That(entry.VectorClock, Is.Not.Null,
                "Cache fallback must stamp a non-null empty VersionVector when ambient VC is unset.");
            Assert.That(entry.VectorClock!.Entries, Is.Empty,
                "Fresh cache snapshot before any local advance has no entries.");
            Assert.That(entry.DependencySummary, Is.SameAs(entry.VectorClock),
                "Inter-slot aliasing is preserved on the cache-fallback path.");
        });
    }

    [Test]
    public async Task DeleteRange_carries_ambient_vector_clock_when_present()
    {
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), AllowAll(), new LocalVectorClockCache(Substitute.For<IGrainFactory>()));
        var vc = new VersionVector();
        var ticked = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        vc.Entries["site-a"] = ticked;

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = "t",
            Kind = MutationKind.DeleteRange,
            Key = "a",
            EndExclusiveKey = "z",
            IsTombstone = true,
            VectorClock = vc,
        }, CancellationToken.None);

        var entry = sink.Entries.Single();
        Assert.Multiple(() =>
        {
            Assert.That(entry.VectorClock, Is.Not.SameAs(vc));
            Assert.That(entry.VectorClock, Is.Not.Null);
            Assert.That(entry.VectorClock!.GetClock("site-a"), Is.EqualTo(ticked));
        });
    }

    // -- Gap (i): defensive-clone contract -----------------------------

    [Test]
    public async Task Vector_clock_is_defensively_cloned_so_post_emit_mutation_does_not_leak()
    {
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), AllowAll(), new LocalVectorClockCache(Substitute.For<IGrainFactory>()));
        var vc = new VersionVector();
        var atEmit = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        vc.Entries["site-a"] = atEmit;

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = "t",
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1 },
            VectorClock = vc,
        }, CancellationToken.None);

        // Producer-side advances applied AFTER the observer returned
        // must not leak into the captured entry. This pins the
        // defensive-clone contract: a downstream consumer reading the
        // entry's frontier sees the value at emit time, not the
        // current value of the originating site's local clock.
        vc.Tick("site-b");
        vc.Entries["site-a"] = HybridLogicalClock.Tick(atEmit);

        var entry = sink.Entries.Single();
        Assert.Multiple(() =>
        {
            Assert.That(entry.VectorClock, Is.Not.SameAs(vc));
            Assert.That(entry.VectorClock!.Entries.ContainsKey("site-b"), Is.False,
                "post-emit advance on a new origin must not leak into the captured entry");
            Assert.That(entry.VectorClock.GetClock("site-a"), Is.EqualTo(atEmit),
                "post-emit advance on an existing origin must not leak into the captured entry");
            Assert.That(entry.DependencySummary, Is.SameAs(entry.VectorClock),
                "both slots remain aliased to the single clone (per spec)");
        });
    }

    // -- Gap (vi): filter-rejected mutation does not emit a VC entry ---

    [Test]
    public async Task Filter_rejected_mutation_does_not_emit_even_when_vector_clock_is_set()
    {
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor(new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            KeyPrefixes = new[] { "ok/" },
        }), AllowAll(), new LocalVectorClockCache(Substitute.For<IGrainFactory>()));
        var vc = new VersionVector();
        vc.Tick("site-a");

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = "t",
            Kind = MutationKind.Set,
            Key = "skip/1",
            Value = new byte[] { 1 },
            VectorClock = vc,
        }, CancellationToken.None);

        // The per-key filter runs before the entry is constructed, so
        // a rejected mutation never reaches the sink even when it
        // carries a non-null causal-plus frontier.
        Assert.That(sink.Entries, Is.Empty);
    }

    // -- Gap (viii): options-change cache invalidation does not break VC stamping --

    [Test]
    public async Task Options_change_invalidation_does_not_break_vector_clock_stamping()
    {
        // The observer's per-tree filter cache is invalidated on
        // IOptionsMonitor.OnChange. VC stamping reads from
        // mutation.VectorClock directly (not from cached options) so
        // an options change must have no effect on the stamping path.
        // Capture the registered callback, fire it between emits,
        // and assert that the VC slot is correctly stamped on both
        // the pre-change and post-change emits.
        var initialOptions = new LatticeReplicationOptions { ClusterId = "site-a" };
        var changedOptions = new LatticeReplicationOptions { ClusterId = "site-a" };
        Action<LatticeReplicationOptions, string?>? captured = null;
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.CurrentValue.Returns(initialOptions);
        monitor.Get(Arg.Any<string>()).Returns(initialOptions);
        monitor.OnChange(Arg.Do<Action<LatticeReplicationOptions, string?>>(h => captured = h))
            .Returns((IDisposable?)null);

        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, monitor, AllowAll(), new LocalVectorClockCache(Substitute.For<IGrainFactory>()));
        var vc1 = new VersionVector();
        var t1 = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        vc1.Entries["site-a"] = t1;

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = "t",
            Kind = MutationKind.Set,
            Key = "k1",
            Value = new byte[] { 1 },
            VectorClock = vc1,
        }, CancellationToken.None);

        // Simulate a runtime options change: the observer clears its
        // compiled-filter cache, but VC stamping must still work on
        // the next emit because it does not consult the cache.
        Assert.That(captured, Is.Not.Null, "observer must subscribe to OnChange");
        captured!.Invoke(changedOptions, null);

        var vc2 = new VersionVector();
        var t2 = HybridLogicalClock.Tick(t1);
        vc2.Entries["site-a"] = t2;

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = "t",
            Kind = MutationKind.Set,
            Key = "k2",
            Value = new byte[] { 2 },
            VectorClock = vc2,
        }, CancellationToken.None);

        Assert.That(sink.Entries, Has.Count.EqualTo(2));
        Assert.Multiple(() =>
        {
            Assert.That(sink.Entries[0].VectorClock!.GetClock("site-a"), Is.EqualTo(t1));
            Assert.That(sink.Entries[1].VectorClock!.GetClock("site-a"), Is.EqualTo(t2));
            Assert.That(sink.Entries[0].VectorClock, Is.Not.SameAs(sink.Entries[1].VectorClock),
                "each emit gets its own defensive clone");
        });
    }

    // ------------------------------------------------------------------
    // Pre-merge typed delta passthrough
    // ------------------------------------------------------------------

    [Test]
    public async Task Delta_kind_and_payload_are_forwarded_verbatim_on_set()
    {
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), AllowAll(), new LocalVectorClockCache(Substitute.For<IGrainFactory>()));
        var payload = new byte[] { 4, 5, 6 };

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = "t",
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1 },
            DeltaKind = "ol.crdt.ors.add",
            DeltaPayload = payload,
        }, CancellationToken.None);

        var entry = sink.Entries.Single();
        Assert.Multiple(() =>
        {
            Assert.That(entry.DeltaKind, Is.EqualTo("ol.crdt.ors.add"));
            Assert.That(entry.DeltaPayload, Is.SameAs(payload));
        });
    }

    [Test]
    public async Task Delta_slots_are_null_on_emit_when_mutation_did_not_author_a_delta()
    {
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), AllowAll(), new LocalVectorClockCache(Substitute.For<IGrainFactory>()));

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = "t",
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1 },
        }, CancellationToken.None);

        var entry = sink.Entries.Single();
        Assert.Multiple(() =>
        {
            Assert.That(entry.DeltaKind, Is.Null);
            Assert.That(entry.DeltaPayload, Is.Null);
        });
    }

    [Test]
    public async Task Delete_mutation_forwards_delta_kind_and_payload()
    {
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), AllowAll(), new LocalVectorClockCache(Substitute.For<IGrainFactory>()));
        var payload = new byte[] { 1 };

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = "t",
            Kind = MutationKind.Delete,
            Key = "k",
            IsTombstone = true,
            DeltaKind = "ol.crdt.ors.rm",
            DeltaPayload = payload,
        }, CancellationToken.None);

        var entry = sink.Entries.Single();
        Assert.Multiple(() =>
        {
            Assert.That(entry.DeltaKind, Is.EqualTo("ol.crdt.ors.rm"));
            Assert.That(entry.DeltaPayload, Is.SameAs(payload));
        });
    }

    [Test]
    public async Task Delta_payload_is_forwarded_by_reference_not_cloned()
    {
        // Bytes are treated as opaque (matching the observer's
        // existing handling of mutation.Value): the producer authored
        // the payload once at the call site and the observer forwards
        // the reference verbatim. Pinned to keep the hot path
        // allocation-free for typed CRDT emits.
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), AllowAll(), new LocalVectorClockCache(Substitute.For<IGrainFactory>()));
        var payload = new byte[] { 7 };

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = "t",
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1 },
            DeltaKind = "ol.crdt.pnc.inc",
            DeltaPayload = payload,
        }, CancellationToken.None);

        Assert.That(sink.Entries.Single().DeltaPayload, Is.SameAs(payload));
    }

    // ----------------------------------------------------------------__
    // R-090 - MutationCategory classification + maintenance skip
    // ------------------------------------------------------------------

    [Test]
    public async Task Maintenance_category_set_is_skipped()
    {
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), AllowAll(), new LocalVectorClockCache(Substitute.For<IGrainFactory>()));

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = DefaultTree,
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1, 2, 3 },
            Category = MutationCategory.Maintenance,
        }, CancellationToken.None);

        Assert.That(sink.Entries, Is.Empty);
    }

    [Test]
    public async Task Maintenance_category_delete_is_skipped()
    {
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), AllowAll(), new LocalVectorClockCache(Substitute.For<IGrainFactory>()));

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = DefaultTree,
            Kind = MutationKind.Delete,
            Key = "gone",
            IsTombstone = true,
            Category = MutationCategory.Maintenance,
        }, CancellationToken.None);

        Assert.That(sink.Entries, Is.Empty);
    }

    [Test]
    public async Task Maintenance_category_delete_range_is_skipped()
    {
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), AllowAll(), new LocalVectorClockCache(Substitute.For<IGrainFactory>()));

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = DefaultTree,
            Kind = MutationKind.DeleteRange,
            Key = "a",
            EndExclusiveKey = "z",
            IsTombstone = true,
            Category = MutationCategory.Maintenance,
        }, CancellationToken.None);

        Assert.That(sink.Entries, Is.Empty);
    }

    [Test]
    public async Task User_category_default_is_emitted_to_sink()
    {
        // Wire-compat regression: a freshly-constructed LatticeMutation
        // with no explicit Category should default to MutationCategory.User
        // (the [Id(11)] default) and emit through the observer unchanged.
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), AllowAll(), new LocalVectorClockCache(Substitute.For<IGrainFactory>()));

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = DefaultTree,
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1 },
        }, CancellationToken.None);

        Assert.That(sink.Entries, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task Explicit_user_category_is_emitted_to_sink()
    {
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), AllowAll(), new LocalVectorClockCache(Substitute.For<IGrainFactory>()));

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = DefaultTree,
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1 },
            Category = MutationCategory.User,
        }, CancellationToken.None);

        Assert.That(sink.Entries, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task Maintenance_category_skip_is_independent_of_origin_cluster_id()
    {
        // R-090 spec: the classification is independent of OriginClusterId.
        // A remote-origin maintenance emit is still Maintenance and still
        // skips the WAL.
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), AllowAll(), new LocalVectorClockCache(Substitute.For<IGrainFactory>()));

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = DefaultTree,
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1 },
            OriginClusterId = "site-b",
            Category = MutationCategory.Maintenance,
        }, CancellationToken.None);

        Assert.That(sink.Entries, Is.Empty);
    }

    [Test]
    public async Task Maintenance_category_skip_does_not_consult_mode_resolver()
    {
        // The Maintenance gate runs before mode resolution so the resolver
        // is never consulted for a structural-maintenance emit.
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        resolver.Resolve(Arg.Any<string>()).Returns(LatticeMergeMode.LwwRegister);

        var observer = new ReplicationMutationObserver(new CapturingSink(), Monitor("site-a"), resolver, new LocalVectorClockCache(Substitute.For<IGrainFactory>()));

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = DefaultTree,
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1 },
            Category = MutationCategory.Maintenance,
        }, CancellationToken.None);

        resolver.DidNotReceive().Resolve(Arg.Any<string>());
    }

    [Test]
    public async Task Maintenance_category_skip_does_not_invoke_key_filter()
    {
        // The Maintenance gate runs before per-key filter compilation /
        // evaluation so the predicate is never invoked for a maintenance
        // emit, even when one is configured.
        var calls = 0;
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor(new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            KeyFilter = _ => { calls++; return true; },
        }), AllowAll(), new LocalVectorClockCache(Substitute.For<IGrainFactory>()));

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = DefaultTree,
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1 },
            Category = MutationCategory.Maintenance,
        }, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(sink.Entries, Is.Empty);
            Assert.That(calls, Is.EqualTo(0));
        });
    }

    [Test]
    public async Task Maintenance_category_skip_does_not_invoke_sink()
    {
        // Pin the no-WriteAsync contract: a maintenance emit must not
        // touch the sink at all (no allocation, no awaitable).
        var sink = Substitute.For<IReplogSink>();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), AllowAll(), new LocalVectorClockCache(Substitute.For<IGrainFactory>()));

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = DefaultTree,
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1 },
            Category = MutationCategory.Maintenance,
        }, CancellationToken.None);

        await sink.DidNotReceive().WriteAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task User_and_maintenance_emits_interleave_correctly()
    {
        // Pin that the gate is per-mutation (not sticky): a maintenance
        // emit followed by a user emit on the same observer instance
        // produces exactly one captured entry, and vice versa.
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), AllowAll(), new LocalVectorClockCache(Substitute.For<IGrainFactory>()));

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = DefaultTree,
            Kind = MutationKind.Set,
            Key = "k1",
            Value = new byte[] { 1 },
            Category = MutationCategory.User,
        }, CancellationToken.None);

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = DefaultTree,
            Kind = MutationKind.Set,
            Key = "k2",
            Value = new byte[] { 2 },
            Category = MutationCategory.Maintenance,
        }, CancellationToken.None);

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = DefaultTree,
            Kind = MutationKind.Set,
            Key = "k3",
            Value = new byte[] { 3 },
            Category = MutationCategory.User,
        }, CancellationToken.None);

        Assert.That(
            sink.Entries.Select(e => e.Key).ToArray(),
            Is.EqualTo(new[] { "k1", "k3" }));
    }
}

