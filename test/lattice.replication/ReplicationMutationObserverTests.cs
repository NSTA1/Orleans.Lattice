using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit tests for <see cref="ReplicationMutationObserver"/>. The observer
/// no longer builds a WAL record or captures a vector clock - the durable
/// change-feed record is written by the foreground leaf commit-log writer.
/// The observer's remaining job is to nudge the registered
/// <see cref="IReplogSink"/> with the committed tree id when (and only
/// when) the mutation is eligible for replication, so these tests assert
/// the gating behaviour through a tree-id-recording fake sink.
/// </summary>
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
    /// per-key filters. Opts every tree id in to
    /// <see cref="LatticeMergeMode.LwwRegister"/>; tests that exercise mode
    /// resolution itself construct an explicit resolver instead.
    /// </summary>
    private sealed class AllowAllResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) => LatticeMergeMode.LwwRegister;
    }

    private static ILatticeMergeModeResolver AllowAll() => new AllowAllResolver();

    /// <summary>
    /// Records the tree id of every nudge the observer fires.
    /// </summary>
    private sealed class RecordingSink : IReplogSink
    {
        public List<string> Nudges { get; } = new();

        public Task WriteAsync(string treeId, CancellationToken cancellationToken)
        {
            Nudges.Add(treeId);
            return Task.CompletedTask;
        }
    }

    private static LatticeMutation SetMutation(string treeId, string key) => new()
    {
        TreeId = treeId,
        Kind = MutationKind.Set,
        Key = key,
        Value = new byte[] { 1 },
    };

    // ------------------------------------------------------------------
    // Eligible commits nudge the sink with the committed tree id
    // ------------------------------------------------------------------

    [Test]
    public async Task Set_mutation_nudges_sink_with_tree_id()
    {
        var sink = new RecordingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), AllowAll());

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = DefaultTree,
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1, 2, 3 },
        }, CancellationToken.None);

        Assert.That(sink.Nudges, Is.EqualTo(new[] { DefaultTree }));
    }

    [Test]
    public async Task Delete_mutation_nudges_sink_with_tree_id()
    {
        var sink = new RecordingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), AllowAll());

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = DefaultTree,
            Kind = MutationKind.Delete,
            Key = "gone",
            IsTombstone = true,
        }, CancellationToken.None);

        Assert.That(sink.Nudges, Is.EqualTo(new[] { DefaultTree }));
    }

    [Test]
    public async Task DeleteRange_mutation_nudges_sink_with_tree_id()
    {
        var sink = new RecordingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), AllowAll());

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = DefaultTree,
            Kind = MutationKind.DeleteRange,
            Key = "a",
            EndExclusiveKey = "z",
            IsTombstone = true,
        }, CancellationToken.None);

        Assert.That(sink.Nudges, Is.EqualTo(new[] { DefaultTree }));
    }

    [Test]
    public async Task Tombstone_reap_kind_is_skipped()
    {
        // Tombstone-reap envelopes are local structural cleanup and have
        // no observer dispatch path; the defence-in-depth short-circuit
        // must suppress any nudge before the Set/Delete/DeleteRange switch.
        var sink = new RecordingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), AllowAll());

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = DefaultTree,
            Kind = MutationKind.Tombstone,
            Key = "k",
        }, CancellationToken.None);

        Assert.That(sink.Nudges, Is.Empty);
    }

    [Test]
    public void Unknown_mutation_kind_throws_invalid_operation()
    {
        var observer = new ReplicationMutationObserver(new RecordingSink(), Monitor("site-a"), AllowAll());

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

        await sink.Received(1).WriteAsync(DefaultTree, cts.Token);
    }

    // ------------------------------------------------------------------
    // Constructor guards
    // ------------------------------------------------------------------

    [Test]
    public void Constructor_throws_on_null_sink()
    {
        Assert.That(
            () => new ReplicationMutationObserver(null!, Monitor("site-a"), AllowAll()),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_throws_on_null_options()
    {
        Assert.That(
            () => new ReplicationMutationObserver(new RecordingSink(), null!, AllowAll()),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_throws_on_null_mode_resolver()
    {
        Assert.That(
            () => new ReplicationMutationObserver(new RecordingSink(), Monitor("site-a"), null!),
            Throws.ArgumentNullException);
    }

    // ------------------------------------------------------------------
    // Declared replication mode is the gate
    // ------------------------------------------------------------------

    [Test]
    public async Task Mode_resolver_null_skips_mutation()
    {
        var sink = new RecordingSink();
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        resolver.Resolve(Arg.Any<string>()).Returns((LatticeMergeMode?)null);

        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), resolver);

        await observer.OnMutationAsync(SetMutation("undeclared", "k"), CancellationToken.None);

        Assert.That(sink.Nudges, Is.Empty);
    }

    [Test]
    public async Task Declared_tree_nudges_and_undeclared_tree_is_skipped()
    {
        var sink = new RecordingSink();
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        resolver.Resolve("declared").Returns(LatticeMergeMode.LwwRegister);
        resolver.Resolve("undeclared").Returns((LatticeMergeMode?)null);

        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), resolver);

        await observer.OnMutationAsync(SetMutation("declared", "k"), CancellationToken.None);
        await observer.OnMutationAsync(SetMutation("undeclared", "k"), CancellationToken.None);

        Assert.That(sink.Nudges, Is.EqualTo(new[] { "declared" }));
    }

    [Test]
    public async Task Mode_resolver_is_consulted_per_mutation()
    {
        var sink = new RecordingSink();
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        resolver.Resolve(Arg.Any<string>()).Returns(LatticeMergeMode.LwwRegister);

        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), resolver);

        await observer.OnMutationAsync(SetMutation("t", "k1"), CancellationToken.None);
        await observer.OnMutationAsync(SetMutation("t", "k2"), CancellationToken.None);

        resolver.Received(2).Resolve("t");
    }

    // ------------------------------------------------------------------
    // Producer-side per-key filters (mode resolver permissive)
    // ------------------------------------------------------------------

    [Test]
    public async Task Key_filter_rejecting_predicate_skips_mutation()
    {
        var sink = new RecordingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor(new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            KeyFilter = static k => k.StartsWith("ok/"),
        }), AllowAll());

        await observer.OnMutationAsync(SetMutation("t", "ok/1"), CancellationToken.None);
        await observer.OnMutationAsync(SetMutation("t", "skip/1"), CancellationToken.None);

        // Only the accepted key produces a nudge.
        Assert.That(sink.Nudges, Is.EqualTo(new[] { "t" }));
    }

    [Test]
    public async Task Key_filter_null_replicates_every_key()
    {
        var sink = new RecordingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor(new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            KeyFilter = null,
        }), AllowAll());

        await observer.OnMutationAsync(SetMutation("t", "anything"), CancellationToken.None);

        Assert.That(sink.Nudges, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task Key_prefixes_empty_imposes_no_restriction()
    {
        var sink = new RecordingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor(new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            KeyPrefixes = Array.Empty<string>(),
        }), AllowAll());

        await observer.OnMutationAsync(SetMutation("t", "any"), CancellationToken.None);

        Assert.That(sink.Nudges, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task Key_prefixes_match_first_prefix_passes()
    {
        var sink = new RecordingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor(new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            KeyPrefixes = new[] { "a/", "b/" },
        }), AllowAll());

        await observer.OnMutationAsync(SetMutation("t", "a/1"), CancellationToken.None);
        await observer.OnMutationAsync(SetMutation("t", "b/2"), CancellationToken.None);
        await observer.OnMutationAsync(SetMutation("t", "c/3"), CancellationToken.None);

        // Two of the three keys match a configured prefix.
        Assert.That(sink.Nudges, Is.EqualTo(new[] { "t", "t" }));
    }

    [Test]
    public async Task Key_prefix_match_is_ordinal_case_sensitive()
    {
        var sink = new RecordingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor(new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            KeyPrefixes = new[] { "Repl/" },
        }), AllowAll());

        await observer.OnMutationAsync(SetMutation("t", "repl/1"), CancellationToken.None);

        Assert.That(sink.Nudges, Is.Empty);
    }

    [Test]
    public async Task Filters_combine_with_logical_and()
    {
        var sink = new RecordingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor(new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            KeyFilter = static k => k.EndsWith("-keep"),
            KeyPrefixes = new[] { "x/" },
        }), AllowAll());

        await observer.OnMutationAsync(SetMutation("t", "x/a-keep"), CancellationToken.None);
        await observer.OnMutationAsync(SetMutation("t", "x/a-drop"), CancellationToken.None);
        await observer.OnMutationAsync(SetMutation("t", "y/a-keep"), CancellationToken.None);

        // Only the key matching both the prefix allowlist and the
        // predicate produces a nudge.
        Assert.That(sink.Nudges, Is.EqualTo(new[] { "t" }));
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

        var sink = new RecordingSink();
        var observer = new ReplicationMutationObserver(sink, monitor, AllowAll());

        await observer.OnMutationAsync(SetMutation("vip", "k"), CancellationToken.None);
        await observer.OnMutationAsync(SetMutation("other", "k"), CancellationToken.None);

        Assert.That(sink.Nudges, Is.EqualTo(new[] { "vip" }));
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

        await sink.DidNotReceive().WriteAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task DeleteRange_filter_evaluates_against_start_key()
    {
        var sink = new RecordingSink();
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

        // Only the range whose start key matches the prefix nudges.
        Assert.That(sink.Nudges, Is.EqualTo(new[] { "t" }));
    }

    // ------------------------------------------------------------------
    // Compiled-filter cache
    // ------------------------------------------------------------------

    [Test]
    public async Task Options_resolution_is_cached_per_tree_id()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        var opts = new LatticeReplicationOptions { ClusterId = "site-a" };
        monitor.CurrentValue.Returns(opts);
        monitor.Get(Arg.Any<string>()).Returns(opts);

        using var observer = new ReplicationMutationObserver(new RecordingSink(), monitor, AllowAll());

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
        var sink = new RecordingSink();
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
            Assert.That(sink.Nudges, Has.Count.EqualTo(3));
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

        var sink = new RecordingSink();
        using var observer = new ReplicationMutationObserver(sink, monitor, AllowAll());

        await observer.OnMutationAsync(SetMutation("t", "k"), CancellationToken.None);
        Assert.That(sink.Nudges, Is.Empty, "Initial deny-all filter should suppress the nudge.");

        current = allow;
        Assert.That(changeCallback, Is.Not.Null);
        changeCallback!.Invoke(allow, null);

        await observer.OnMutationAsync(SetMutation("t", "k"), CancellationToken.None);
        Assert.That(sink.Nudges, Has.Count.EqualTo(1));
    }

    [Test]
    public void Dispose_releases_options_change_subscription()
    {
        var subscription = Substitute.For<IDisposable>();
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.CurrentValue.Returns(new LatticeReplicationOptions { ClusterId = "x" });
        monitor.Get(Arg.Any<string>()).Returns(new LatticeReplicationOptions { ClusterId = "x" });
        monitor.OnChange(Arg.Any<Action<LatticeReplicationOptions, string?>>()).Returns(subscription);

        var observer = new ReplicationMutationObserver(new RecordingSink(), monitor, AllowAll());
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

        var observer = new ReplicationMutationObserver(new RecordingSink(), monitor, AllowAll());

        Assert.DoesNotThrow(() => observer.Dispose());
        Assert.DoesNotThrow(() => observer.Dispose());
    }

    [Test]
    public async Task Null_prefix_entries_are_ignored_when_compiling_filter()
    {
        var sink = new RecordingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor(new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            KeyPrefixes = new[] { null!, "ok/" },
        }), AllowAll());

        await observer.OnMutationAsync(SetMutation("t", "ok/1"), CancellationToken.None);
        await observer.OnMutationAsync(SetMutation("t", "skip/1"), CancellationToken.None);

        Assert.That(sink.Nudges, Is.EqualTo(new[] { "t" }));
    }

    [Test]
    public async Task All_null_prefix_collection_imposes_no_restriction()
    {
        var sink = new RecordingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor(new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            KeyPrefixes = new[] { (string)null!, null! },
        }), AllowAll());

        await observer.OnMutationAsync(SetMutation("t", "anything"), CancellationToken.None);

        Assert.That(sink.Nudges, Has.Count.EqualTo(1));
    }

    // ------------------------------------------------------------------
    // Maintenance-category mutations are suppressed
    // ------------------------------------------------------------------

    [Test]
    public async Task Maintenance_category_set_is_skipped()
    {
        var sink = new RecordingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), AllowAll());

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = DefaultTree,
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1, 2, 3 },
            Category = MutationCategory.Maintenance,
        }, CancellationToken.None);

        Assert.That(sink.Nudges, Is.Empty);
    }

    [Test]
    public async Task Maintenance_category_delete_is_skipped()
    {
        var sink = new RecordingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), AllowAll());

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = DefaultTree,
            Kind = MutationKind.Delete,
            Key = "gone",
            IsTombstone = true,
            Category = MutationCategory.Maintenance,
        }, CancellationToken.None);

        Assert.That(sink.Nudges, Is.Empty);
    }

    [Test]
    public async Task Maintenance_category_delete_range_is_skipped()
    {
        var sink = new RecordingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), AllowAll());

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = DefaultTree,
            Kind = MutationKind.DeleteRange,
            Key = "a",
            EndExclusiveKey = "z",
            IsTombstone = true,
            Category = MutationCategory.Maintenance,
        }, CancellationToken.None);

        Assert.That(sink.Nudges, Is.Empty);
    }

    [Test]
    public async Task User_category_default_is_emitted_to_sink()
    {
        // Wire-compat regression: a freshly-constructed LatticeMutation
        // with no explicit Category should default to MutationCategory.User
        // and nudge the sink unchanged.
        var sink = new RecordingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), AllowAll());

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = DefaultTree,
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1 },
        }, CancellationToken.None);

        Assert.That(sink.Nudges, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task Explicit_user_category_is_emitted_to_sink()
    {
        var sink = new RecordingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), AllowAll());

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = DefaultTree,
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1 },
            Category = MutationCategory.User,
        }, CancellationToken.None);

        Assert.That(sink.Nudges, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task Maintenance_category_skip_is_independent_of_origin_cluster_id()
    {
        // The classification is independent of OriginClusterId: a
        // remote-origin maintenance emit is still Maintenance and still
        // skips the nudge.
        var sink = new RecordingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), AllowAll());

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = DefaultTree,
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1 },
            OriginClusterId = "site-b",
            Category = MutationCategory.Maintenance,
        }, CancellationToken.None);

        Assert.That(sink.Nudges, Is.Empty);
    }

    [Test]
    public async Task Maintenance_category_skip_does_not_consult_mode_resolver()
    {
        // The Maintenance gate runs before mode resolution so the resolver
        // is never consulted for a structural-maintenance emit.
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        resolver.Resolve(Arg.Any<string>()).Returns(LatticeMergeMode.LwwRegister);

        var observer = new ReplicationMutationObserver(new RecordingSink(), Monitor("site-a"), resolver);

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
        var sink = new RecordingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor(new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            KeyFilter = _ => { calls++; return true; },
        }), AllowAll());

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
            Assert.That(sink.Nudges, Is.Empty);
            Assert.That(calls, Is.EqualTo(0));
        });
    }

    [Test]
    public async Task Maintenance_category_skip_does_not_invoke_sink()
    {
        // Pin the no-nudge contract: a maintenance emit must not touch the
        // sink at all.
        var sink = Substitute.For<IReplogSink>();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), AllowAll());

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = DefaultTree,
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1 },
            Category = MutationCategory.Maintenance,
        }, CancellationToken.None);

        await sink.DidNotReceive().WriteAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task User_and_maintenance_emits_interleave_correctly()
    {
        // Pin that the gate is per-mutation (not sticky): a maintenance
        // emit between two user emits suppresses exactly one nudge.
        var sink = new RecordingSink();
        var observer = new ReplicationMutationObserver(sink, Monitor("site-a"), AllowAll());

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

        // The two user emits nudge; the maintenance emit is suppressed.
        Assert.That(sink.Nudges, Is.EqualTo(new[] { DefaultTree, DefaultTree }));
    }
}
