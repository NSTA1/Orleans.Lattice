using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class ReplicationMutationObserverTests
{
    private static IOptionsMonitor<LatticeReplicationOptions> Monitor(string clusterId) =>
        Monitor(new LatticeReplicationOptions { ClusterId = clusterId });

    private static IOptionsMonitor<LatticeReplicationOptions> Monitor(LatticeReplicationOptions options)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

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
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"));
        var ts = HybridLogicalClock.Tick(HybridLogicalClock.Zero);

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = "tree",
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
            Assert.That(e.TreeId, Is.EqualTo("tree"));
            Assert.That(e.Op, Is.EqualTo(ReplogOp.Set));
            Assert.That(e.Key, Is.EqualTo("k"));
            Assert.That(e.EndExclusiveKey, Is.Null);
            Assert.That(e.Value, Is.EqualTo(new byte[] { 1, 2, 3 }));
            Assert.That(e.Timestamp, Is.EqualTo(ts));
            Assert.That(e.IsTombstone, Is.False);
            Assert.That(e.ExpiresAtTicks, Is.EqualTo(100L));
            Assert.That(e.OriginClusterId, Is.EqualTo("site-a"));
        });
    }

    [Test]
    public async Task Delete_mutation_emits_entry_with_tombstone_flag()
    {
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"));

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = "tree",
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
        });
    }

    [Test]
    public async Task DeleteRange_mutation_emits_entry_with_end_exclusive_key()
    {
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"));

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = "tree",
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
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"));

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = "tree",
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
        // Real registrations are guarded by LatticeReplicationOptionsValidator,
        // which rejects an empty ClusterId before any observer call. This unit
        // test bypasses validation by injecting options directly, so the
        // observer's documented behaviour is to forward the configured value
        // as-is rather than substitute null.
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-x"));

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = "tree",
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 7 },
        }, CancellationToken.None);

        Assert.That(sink.Entries.Single().OriginClusterId, Is.EqualTo("site-x"));
    }

    [Test]
    public void Unknown_mutation_kind_throws_invalid_operation()
    {
        var observer = new ReplicationMutationObserver(new CapturingSink(), Monitor("site-a"));

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
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"));
        using var cts = new CancellationTokenSource();

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = "tree",
            Kind = MutationKind.Set,
            Key = "k",
            Value = Array.Empty<byte>(),
        }, cts.Token);

        await sink.Received(1).WriteAsync(Arg.Any<ReplogEntry>(), cts.Token);
    }

    // ------------------------------------------------------------------
    // R-012 — producer-side filters
    // ------------------------------------------------------------------

    [Test]
    public async Task Tree_allowlist_null_replicates_every_tree()
    {
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor(new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ReplicatedTrees = null,
        }));

        await observer.OnMutationAsync(SetMutation("any-tree", "k"), CancellationToken.None);

        Assert.That(sink.Entries, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task Tree_allowlist_empty_skips_every_tree()
    {
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor(new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ReplicatedTrees = Array.Empty<string>(),
        }));

        await observer.OnMutationAsync(SetMutation("any-tree", "k"), CancellationToken.None);

        Assert.That(sink.Entries, Is.Empty);
    }

    [Test]
    public async Task Tree_allowlist_includes_only_listed_trees()
    {
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor(new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ReplicatedTrees = new[] { "yes" },
        }));

        await observer.OnMutationAsync(SetMutation("yes", "k1"), CancellationToken.None);
        await observer.OnMutationAsync(SetMutation("no", "k2"), CancellationToken.None);

        Assert.That(sink.Entries.Select(e => e.TreeId).ToArray(), Is.EqualTo(new[] { "yes" }));
    }

    [Test]
    public async Task Tree_allowlist_membership_is_case_sensitive()
    {
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor(new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ReplicatedTrees = new[] { "Tree" },
        }));

        await observer.OnMutationAsync(SetMutation("tree", "k"), CancellationToken.None);

        Assert.That(sink.Entries, Is.Empty);
    }

    [Test]
    public async Task Key_filter_rejecting_predicate_skips_mutation()
    {
        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor(new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            KeyFilter = static k => k.StartsWith("ok/"),
        }));

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
        }));

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
        }));

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
        }));

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
        }));

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
            ReplicatedTrees = new[] { "t" },
            KeyFilter = static k => k.EndsWith("-keep"),
            KeyPrefixes = new[] { "x/" },
        }));

        // Only the entry that satisfies tree, prefix, and predicate passes.
        await observer.OnMutationAsync(SetMutation("t", "x/a-keep"), CancellationToken.None);
        await observer.OnMutationAsync(SetMutation("t", "x/a-drop"), CancellationToken.None);
        await observer.OnMutationAsync(SetMutation("t", "y/a-keep"), CancellationToken.None);
        await observer.OnMutationAsync(SetMutation("other", "x/a-keep"), CancellationToken.None);

        Assert.That(
            sink.Entries.Select(e => e.Key).ToArray(),
            Is.EqualTo(new[] { "x/a-keep" }));
    }

    [Test]
    public async Task Filters_resolve_per_tree_named_options_instance()
    {
        // Configure the default options to deny everything (empty allowlist),
        // but configure the named "vip" tree to opt back in. The observer
        // resolves options via Get(treeId) so the per-tree override wins.
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        var defaults = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ReplicatedTrees = Array.Empty<string>(),
        };
        var vip = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ReplicatedTrees = new[] { "vip" },
        };
        monitor.CurrentValue.Returns(defaults);
        monitor.Get(Arg.Any<string>()).Returns(defaults);
        monitor.Get("vip").Returns(vip);

        var sink = new CapturingSink();
        var observer = new ReplicationMutationObserver(sink, monitor);

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
            ReplicatedTrees = Array.Empty<string>(),
        }));

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
        }));

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

        using var observer = new ReplicationMutationObserver(new CapturingSink(), monitor);

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
        }));

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
            ReplicatedTrees = Array.Empty<string>(),
        };
        var allow = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ReplicatedTrees = null,
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
        using var observer = new ReplicationMutationObserver(sink, monitor);

        await observer.OnMutationAsync(SetMutation("t", "k"), CancellationToken.None);
        Assert.That(sink.Entries, Is.Empty, "Initial deny-all options should suppress the write.");

        // Mutate the underlying options and signal the change. The next
        // mutation must rebuild the compiled filter from the new snapshot.
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
        using var observer = new ReplicationMutationObserver(sink, monitor);

        await observer.OnMutationAsync(SetMutation("t", "k1"), CancellationToken.None);

        // Mutating the options instance reference without firing OnChange
        // must not affect the cached cluster id - the snapshot is the
        // source of truth until invalidation.
        current = v2;
        await observer.OnMutationAsync(SetMutation("t", "k2"), CancellationToken.None);

        // After OnChange the next mutation rebuilds and observes v2.
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

        var observer = new ReplicationMutationObserver(new CapturingSink(), monitor);
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

        var observer = new ReplicationMutationObserver(new CapturingSink(), monitor);

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
        }));

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
        }));

        await observer.OnMutationAsync(SetMutation("t", "anything"), CancellationToken.None);

        // After filtering null entries the collection is empty, which is
        // documented as "no prefix restriction" - every key passes.
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

