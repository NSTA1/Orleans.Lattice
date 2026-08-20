using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Deterministic unit tests for <see cref="RepoContextVectorPlaneReDeriver"/>, the
/// repository-context self-healer that re-derives a rebuildable vector-plane tree
/// which fell terminally off its write-ahead log (surfaced on leaf activation as
/// <see cref="LeafProjectionStaleException"/>). The tests substitute the faulting
/// tree so the terminal state the silo harness cannot easily provoke is exercised
/// directly, and cover the whole contract: detect -> log/meter -> re-derive ->
/// converge, the fail-closed allow-list, single-flight/idempotency, and the guard's
/// no-masking re-throw. No timing, Chaos, or Integration dependence.
/// </summary>
[TestFixture]
public sealed class RepoContextVectorPlaneReDeriverTests
{
    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private static LeafProjectionStaleException Stale(string treeId)
        => new($"leaf projection for tree '{treeId}' has fallen off the write-ahead log");

    private static (RepoContextVectorPlaneReDeriver ReDeriver, IGrainFactory Factory, CapturingLoggerProvider Log) Build()
    {
        var factory = Substitute.For<IGrainFactory>();
        var log = new CapturingLoggerProvider();
        using var loggerFactory = LoggerFactory.Create(b => b.AddProvider(log));
        var logger = loggerFactory.CreateLogger<RepoContextVectorPlaneReDeriver>();
        return (new RepoContextVectorPlaneReDeriver(factory, logger), factory, log);
    }

    private sealed record Measurement(long Value, string? Tree, string? Outcome);

    // Captures the re-derive counter's measurements with their tree/outcome tags.
    // Started after the re-deriver is built so the already-published instrument is
    // enumerated, mirroring RepoContextUsageRecorderTests.
    private static (List<Measurement> Measurements, MeterListener Listener) StartCapture()
    {
        var measurements = new List<Measurement>();
        var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == RepoContextUsageRecorder.MeterName
                && instrument.Name == RepoContextVectorPlaneReDeriver.ReDeriveInstrumentName)
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((_, measurement, tags, _) =>
        {
            string? tree = null;
            string? outcome = null;
            foreach (var tag in tags)
            {
                if (tag.Key == RepoContextVectorPlaneReDeriver.TreeTagKey)
                {
                    tree = tag.Value as string;
                }
                else if (tag.Key == RepoContextVectorPlaneReDeriver.OutcomeTagKey)
                {
                    outcome = tag.Value as string;
                }
            }

            lock (measurements)
            {
                measurements.Add(new Measurement(measurement, tree, outcome));
            }
        });
        listener.Start();
        return (measurements, listener);
    }

    private static long Total(IEnumerable<Measurement> measurements, string tree, string outcome)
    {
        lock (measurements)
        {
            return measurements
                .Where(m => m.Tree == tree && m.Outcome == outcome)
                .Sum(m => m.Value);
        }
    }

    // ── Constructor guards ─────────────────────────────────────────────────

    [Test]
    public void Constructor_null_grain_factory_throws()
    {
        var log = new CapturingLoggerProvider();
        using var loggerFactory = LoggerFactory.Create(b => b.AddProvider(log));
        var logger = loggerFactory.CreateLogger<RepoContextVectorPlaneReDeriver>();

        Assert.Throws<ArgumentNullException>(() => new RepoContextVectorPlaneReDeriver(null!, logger));
    }

    [Test]
    public void Constructor_null_logger_throws()
    {
        var factory = Substitute.For<IGrainFactory>();
        Assert.Throws<ArgumentNullException>(() => new RepoContextVectorPlaneReDeriver(factory, null!));
    }

    // ── GuardAsync (void) argument guards ──────────────────────────────────

    [Test]
    public void GuardAsync_null_tree_name_throws()
    {
        var (reDeriver, _, _) = Build();
        using (reDeriver)
        {
            Assert.ThrowsAsync<ArgumentNullException>(async () =>
                await reDeriver.GuardAsync(null!, () => Task.CompletedTask, Ct));
        }
    }

    [Test]
    public void GuardAsync_null_operation_throws()
    {
        var (reDeriver, _, _) = Build();
        using (reDeriver)
        {
            Assert.ThrowsAsync<ArgumentNullException>(async () =>
                await reDeriver.GuardAsync(RepoContextTrees.VectorMetadata, null!, Ct));
        }
    }

    [Test]
    public void GuardAsync_generic_null_tree_name_throws()
    {
        var (reDeriver, _, _) = Build();
        using (reDeriver)
        {
            Assert.ThrowsAsync<ArgumentNullException>(async () =>
                await reDeriver.GuardAsync(null!, () => Task.FromResult(0), Ct));
        }
    }

    [Test]
    public void GuardAsync_generic_null_operation_throws()
    {
        var (reDeriver, _, _) = Build();
        using (reDeriver)
        {
            Assert.ThrowsAsync<ArgumentNullException>(async () =>
                await reDeriver.GuardAsync<int>(RepoContextTrees.VectorMetadata, null!, Ct));
        }
    }

    // ── GuardAsync happy path ──────────────────────────────────────────────

    [Test]
    public async Task GuardAsync_runs_the_operation_when_no_falloff_occurs()
    {
        var (reDeriver, _, _) = Build();
        using (reDeriver)
        {
            var ran = false;
            await reDeriver.GuardAsync(RepoContextTrees.VectorMetadata, () =>
            {
                ran = true;
                return Task.CompletedTask;
            }, Ct);

            Assert.That(ran, Is.True, "A non-faulting operation runs unmodified through the guard.");
        }
    }

    [Test]
    public async Task GuardAsync_generic_returns_the_operation_result_when_no_falloff_occurs()
    {
        var (reDeriver, _, _) = Build();
        using (reDeriver)
        {
            var result = await reDeriver.GuardAsync(
                RepoContextTrees.VectorMembership, () => Task.FromResult(42), Ct);

            Assert.That(result, Is.EqualTo(42));
        }
    }

    // ── GuardAsync: detect -> log/meter -> re-derive -> converge ────────────

    [Test]
    public async Task GuardAsync_detects_a_falloff_resets_the_tree_then_rethrows_and_a_later_pass_converges()
    {
        var (reDeriver, factory, log) = Build();
        using (reDeriver)
        {
            var tree = Substitute.For<ILattice>();
            factory.GetGrain<ILattice>(RepoContextTrees.VectorMetadata).Returns(tree);

            // The reset heals the tree: once DeleteTreeAsync runs the tree activates
            // clean, so a subsequent pass no longer falls off.
            var fallenOff = true;
            tree.DeleteTreeAsync(Arg.Any<CancellationToken>()).Returns(_ =>
            {
                fallenOff = false;
                return Task.CompletedTask;
            });

            var (measurements, listener) = StartCapture();
            using var _ = listener;

            // First pass: the operation falls off the log. The guard re-throws the
            // originating fault (no masking) only after awaiting the single-flight
            // reset, so by the time it re-throws the tree has been healed.
            Assert.ThrowsAsync<LeafProjectionStaleException>(async () =>
                await reDeriver.GuardAsync(
                    RepoContextTrees.VectorMetadata,
                    () => fallenOff
                        ? throw Stale(RepoContextTrees.VectorMetadata)
                        : Task.CompletedTask,
                    Ct));

            Assert.That(fallenOff, Is.False,
                "The guard awaits the reset before re-throwing, so the tree is reset by the time the fault surfaces.");

            // Second pass now converges - the tree activates clean and the operation runs.
            var converged = false;
            await reDeriver.GuardAsync(
                RepoContextTrees.VectorMetadata,
                () =>
                {
                    if (fallenOff)
                    {
                        throw Stale(RepoContextTrees.VectorMetadata);
                    }

                    converged = true;
                    return Task.CompletedTask;
                },
                Ct);

            Assert.Multiple(() =>
            {
                Assert.That(converged, Is.True, "After the reset, the ingest/gap pass converges.");
                Assert.That(
                    Total(measurements, RepoContextTrees.VectorMetadata, RepoContextVectorPlaneReDeriver.OutcomeObserved),
                    Is.EqualTo(1), "The fall-off is metered once as observed.");
                Assert.That(
                    Total(measurements, RepoContextTrees.VectorMetadata, RepoContextVectorPlaneReDeriver.OutcomeCompleted),
                    Is.EqualTo(1), "The reset is metered once as completed.");
                Assert.That(
                    log.Entries.Any(e => e.Level == LogLevel.Warning && e.Exception is LeafProjectionStaleException),
                    Is.True, "The originating fault is logged with its stack trace before remediation.");
            });

            await tree.Received(1).DeleteTreeAsync(Arg.Any<CancellationToken>());
            await tree.Received(1).PurgeTreeAsync(Arg.Any<CancellationToken>());
        }
    }

    [Test]
    public void GuardAsync_a_non_stale_fault_propagates_without_resetting()
    {
        var (reDeriver, factory, _) = Build();
        using (reDeriver)
        {
            var tree = Substitute.For<ILattice>();
            factory.GetGrain<ILattice>(RepoContextTrees.VectorMetadata).Returns(tree);

            Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await reDeriver.GuardAsync(
                    RepoContextTrees.VectorMetadata,
                    () => throw new InvalidOperationException("unrelated write fault"),
                    Ct));

            tree.DidNotReceive().DeleteTreeAsync(Arg.Any<CancellationToken>());
        }
    }

    // ── ObserveAndReDeriveAsync argument guards ────────────────────────────

    [Test]
    public void ObserveAndReDeriveAsync_null_tree_name_throws()
    {
        var (reDeriver, _, _) = Build();
        using (reDeriver)
        {
            Assert.Throws<ArgumentNullException>(() =>
                reDeriver.ObserveAndReDeriveAsync(null!, Stale("x"), Ct));
        }
    }

    [Test]
    public void ObserveAndReDeriveAsync_null_exception_throws()
    {
        var (reDeriver, _, _) = Build();
        using (reDeriver)
        {
            Assert.Throws<ArgumentNullException>(() =>
                reDeriver.ObserveAndReDeriveAsync(RepoContextTrees.VectorMetadata, null!, Ct));
        }
    }

    // ── ObserveAndReDeriveAsync: allow-listed reset ────────────────────────

    [Test]
    [TestCase("repo-context-vector-metadata")]
    [TestCase("repo-context-vector-membership")]
    public async Task ObserveAndReDeriveAsync_a_rebuildable_tree_logs_meters_and_resets(string treeName)
    {
        var (reDeriver, factory, log) = Build();
        using (reDeriver)
        {
            var tree = Substitute.For<ILattice>();
            factory.GetGrain<ILattice>(treeName).Returns(tree);

            var (measurements, listener) = StartCapture();
            using var _ = listener;

            await reDeriver.ObserveAndReDeriveAsync(treeName, Stale(treeName), Ct);

            Assert.Multiple(() =>
            {
                Assert.That(
                    Total(measurements, treeName, RepoContextVectorPlaneReDeriver.OutcomeObserved),
                    Is.EqualTo(1));
                Assert.That(
                    Total(measurements, treeName, RepoContextVectorPlaneReDeriver.OutcomeCompleted),
                    Is.EqualTo(1));
                Assert.That(
                    log.Entries.Any(e => e.Level == LogLevel.Warning && e.Exception is LeafProjectionStaleException),
                    Is.True, "The fall-off is logged with its exception before remediation.");
            });

            await tree.Received(1).DeleteTreeAsync(Arg.Any<CancellationToken>());
            await tree.Received(1).PurgeTreeAsync(Arg.Any<CancellationToken>());
        }
    }

    // ── ObserveAndReDeriveAsync: fail-closed refusal ───────────────────────

    [Test]
    [TestCase("repo-context-structural")]
    [TestCase("repo-context-symbol")]
    [TestCase("repo-context-memory")]
    [TestCase("repo-context-content")]
    [TestCase("repo-context-xref")]
    [TestCase("repo-context-session")]
    [TestCase("repo-context-vector-payload")]
    [TestCase("some-unknown-tree")]
    public async Task ObserveAndReDeriveAsync_a_non_rebuildable_tree_surfaces_the_fault_but_never_resets(string treeName)
    {
        var (reDeriver, factory, log) = Build();
        using (reDeriver)
        {
            var tree = Substitute.For<ILattice>();
            factory.GetGrain<ILattice>(treeName).Returns(tree);

            var (measurements, listener) = StartCapture();
            using var _ = listener;

            await reDeriver.ObserveAndReDeriveAsync(treeName, Stale(treeName), Ct);

            Assert.Multiple(() =>
            {
                Assert.That(
                    Total(measurements, treeName, RepoContextVectorPlaneReDeriver.OutcomeRefused),
                    Is.EqualTo(1), "The refused fault is surfaced through the meter.");
                Assert.That(
                    Total(measurements, treeName, RepoContextVectorPlaneReDeriver.OutcomeObserved),
                    Is.EqualTo(0), "A refused tree is never treated as an observed re-derivation target.");
                Assert.That(
                    log.Entries.Any(e => e.Level == LogLevel.Warning
                        && e.Exception is LeafProjectionStaleException
                        && e.Message.Contains("fail-closed", StringComparison.Ordinal)),
                    Is.True, "The refusal is logged with its exception and the fail-closed reason.");
            });

            // The primary/store-of-record and write-once trees are never reset - real data loss guard.
            await tree.DidNotReceive().DeleteTreeAsync(Arg.Any<CancellationToken>());
            await tree.DidNotReceive().PurgeTreeAsync(Arg.Any<CancellationToken>());
        }
    }

    // ── ObserveAndReDeriveAsync: purge trips the terminal leaf ─────────────

    [Test]
    public async Task ObserveAndReDeriveAsync_tolerates_a_purge_that_trips_the_terminal_leaf()
    {
        var (reDeriver, factory, log) = Build();
        using (reDeriver)
        {
            var tree = Substitute.For<ILattice>();
            factory.GetGrain<ILattice>(RepoContextTrees.VectorMetadata).Returns(tree);

            // DeleteTreeAsync succeeds (shard-root state only), but the immediate purge
            // walks the chain and re-trips the terminal leaf. The reset must swallow
            // that, relying on the delete's reminder-driven purge, and still complete.
            tree.PurgeTreeAsync(Arg.Any<CancellationToken>())
                .ThrowsAsync(Stale(RepoContextTrees.VectorMetadata));

            var (measurements, listener) = StartCapture();
            using var _ = listener;

            await reDeriver.ObserveAndReDeriveAsync(
                RepoContextTrees.VectorMetadata, Stale(RepoContextTrees.VectorMetadata), Ct);

            Assert.Multiple(() =>
            {
                Assert.That(
                    Total(measurements, RepoContextTrees.VectorMetadata, RepoContextVectorPlaneReDeriver.OutcomeCompleted),
                    Is.EqualTo(1), "The delete unblocked the terminal state, so the reset still completes.");
                Assert.That(
                    Total(measurements, RepoContextTrees.VectorMetadata, RepoContextVectorPlaneReDeriver.OutcomeFailed),
                    Is.EqualTo(0), "A purge that trips the terminal leaf is not a reset failure.");
            });

            await tree.Received(1).DeleteTreeAsync(Arg.Any<CancellationToken>());
        }
    }

    // ── ObserveAndReDeriveAsync: delete failure ────────────────────────────

    [Test]
    public async Task ObserveAndReDeriveAsync_a_failed_delete_meters_failed_and_clears_the_in_flight_signal()
    {
        var (reDeriver, factory, log) = Build();
        using (reDeriver)
        {
            var tree = Substitute.For<ILattice>();
            factory.GetGrain<ILattice>(RepoContextTrees.VectorMetadata).Returns(tree);

            var attempts = 0;
            tree.DeleteTreeAsync(Arg.Any<CancellationToken>()).Returns(_ =>
            {
                attempts++;
                throw new InvalidOperationException("delete failed");
            });

            var (measurements, listener) = StartCapture();
            using var _ = listener;

            await reDeriver.ObserveAndReDeriveAsync(
                RepoContextTrees.VectorMetadata, Stale(RepoContextTrees.VectorMetadata), Ct);

            Assert.Multiple(() =>
            {
                Assert.That(
                    Total(measurements, RepoContextTrees.VectorMetadata, RepoContextVectorPlaneReDeriver.OutcomeFailed),
                    Is.EqualTo(1), "A failed reset is metered as failed and never throws out of remediation.");
                Assert.That(
                    log.Entries.Any(e => e.Level == LogLevel.Error && e.Exception is InvalidOperationException),
                    Is.True, "A failed reset is logged at Error with the underlying cause.");
            });

            // The in-flight signal is cleared on completion, so a fresh fall-off starts
            // a new reset rather than being permanently suppressed.
            await reDeriver.ObserveAndReDeriveAsync(
                RepoContextTrees.VectorMetadata, Stale(RepoContextTrees.VectorMetadata), Ct);

            Assert.That(attempts, Is.EqualTo(2),
                "The single-flight entry is removed after settling, so a later fall-off retries the reset.");
        }
    }

    // ── ObserveAndReDeriveAsync: single-flight ─────────────────────────────

    [Test]
    public async Task ObserveAndReDeriveAsync_is_single_flight_while_a_reset_is_in_flight()
    {
        var (reDeriver, factory, _) = Build();
        using (reDeriver)
        {
            var tree = Substitute.For<ILattice>();
            factory.GetGrain<ILattice>(RepoContextTrees.VectorMetadata).Returns(tree);

            // Gate the reset so it stays in flight while a second observation arrives.
            var gate = new TaskCompletionSource();
            tree.DeleteTreeAsync(Arg.Any<CancellationToken>()).Returns(_ => gate.Task);

            var first = reDeriver.ObserveAndReDeriveAsync(
                RepoContextTrees.VectorMetadata, Stale(RepoContextTrees.VectorMetadata), Ct);
            var second = reDeriver.ObserveAndReDeriveAsync(
                RepoContextTrees.VectorMetadata, Stale(RepoContextTrees.VectorMetadata), Ct);

            Assert.That(ReferenceEquals(first, second), Is.True,
                "A re-derivation already in flight for the tree is joined, not started a second time.");

            gate.SetResult();
            await Task.WhenAll(first, second);

            // Single-flight: exactly one reset ran despite two observations.
            await tree.Received(1).DeleteTreeAsync(Arg.Any<CancellationToken>());
        }
    }

    // ── Dispose ────────────────────────────────────────────────────────────

    [Test]
    public void Dispose_is_safe_to_call()
    {
        var (reDeriver, _, _) = Build();
        Assert.DoesNotThrow(() => reDeriver.Dispose());
    }
}
