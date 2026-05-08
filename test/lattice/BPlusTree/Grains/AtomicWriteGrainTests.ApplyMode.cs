using Microsoft.Extensions.Logging;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the apply-mode saga path on
/// <see cref="AtomicWriteGrain.ExecuteApplyAsync"/>. Exercises the cross-
/// cluster atomic-batch apply seam: validation, idempotent re-entry on
/// committed and compensated sagas, fingerprint mismatch on retry, and
/// mid-saga failure pivoting through compensation to a structured
/// <see cref="AtomicApplyResult"/> (rather than a thrown exception).
/// </summary>
public partial class AtomicWriteGrainTests
{
    /// <summary>
    /// Builds an <see cref="AtomicApplyEntry"/> array from a compact
    /// (key, value, hlc-ticks) tuple list. Tombstones use a
    /// <see langword="null"/> value with <see cref="AtomicApplyEntry.IsTombstone"/>
    /// set to <see langword="true"/>.
    /// </summary>
    private static List<AtomicApplyEntry> MakeApplyEntries(
        params (string Key, byte[]? Value, long Ticks)[] entries)
    {
        var list = new List<AtomicApplyEntry>(entries.Length);
        foreach (var (key, value, ticks) in entries)
        {
            list.Add(new AtomicApplyEntry
            {
                Key = key,
                Value = value,
                Timestamp = new HybridLogicalClock { WallClockTicks = ticks, Counter = 0 },
                ExpiresAtTicks = 0,
                VectorClock = null,
                IsTombstone = value is null,
            });
        }
        return list;
    }

    // --- Input validation ---

    [Test]
    public void ExecuteApplyAsync_throws_on_null_treeId()
    {
        var (grain, _, _, _, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentNullException>(
            () => grain.ExecuteApplyAsync(null!, MakeApplyEntries(("k", [1], 1)), "site-x"));
    }

    [Test]
    public void ExecuteApplyAsync_throws_on_null_entries()
    {
        var (grain, _, _, _, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentNullException>(
            () => grain.ExecuteApplyAsync(TreeId, null!, "site-x"));
    }

    [Test]
    public void ExecuteApplyAsync_throws_on_null_origin_cluster_id()
    {
        var (grain, _, _, _, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentNullException>(
            () => grain.ExecuteApplyAsync(TreeId, MakeApplyEntries(("k", [1], 1)), null!));
    }

    [Test]
    public void ExecuteApplyAsync_throws_on_empty_origin_cluster_id()
    {
        var (grain, _, _, _, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentException>(
            () => grain.ExecuteApplyAsync(TreeId, MakeApplyEntries(("k", [1], 1)), ""));
    }

    [Test]
    public void ExecuteApplyAsync_throws_on_duplicate_keys()
    {
        var (grain, _, _, _, _) = CreateGrain();
        var entries = MakeApplyEntries(("k", [1], 1), ("k", [2], 2));
        Assert.ThrowsAsync<ArgumentException>(
            () => grain.ExecuteApplyAsync(TreeId, entries, "site-x"));
    }

    [Test]
    public void ExecuteApplyAsync_throws_on_null_key()
    {
        var (grain, _, _, _, _) = CreateGrain();
        var entries = new List<AtomicApplyEntry>
        {
            new()
            {
                Key = null!,
                Value = [1],
                Timestamp = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 },
                ExpiresAtTicks = 0,
                VectorClock = null,
                IsTombstone = false,
            },
        };
        Assert.ThrowsAsync<ArgumentException>(
            () => grain.ExecuteApplyAsync(TreeId, entries, "site-x"));
    }

    [Test]
    public void ExecuteApplyAsync_throws_on_null_value_for_non_tombstone()
    {
        var (grain, _, _, _, _) = CreateGrain();
        var entries = new List<AtomicApplyEntry>
        {
            new()
            {
                Key = "k",
                Value = null,
                Timestamp = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 },
                ExpiresAtTicks = 0,
                VectorClock = null,
                IsTombstone = false,
            },
        };
        Assert.ThrowsAsync<ArgumentException>(
            () => grain.ExecuteApplyAsync(TreeId, entries, "site-x"));
    }

    [Test]
    public void ExecuteApplyAsync_throws_on_tombstone_with_non_zero_expiry()
    {
        var (grain, _, _, _, _) = CreateGrain();
        var entries = new List<AtomicApplyEntry>
        {
            new()
            {
                Key = "k",
                Value = null,
                Timestamp = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 },
                ExpiresAtTicks = 12345,
                VectorClock = null,
                IsTombstone = true,
            },
        };
        Assert.ThrowsAsync<ArgumentException>(
            () => grain.ExecuteApplyAsync(TreeId, entries, "site-x"));
    }

    [Test]
    public async Task ExecuteApplyAsync_empty_batch_returns_committed_with_zero_count()
    {
        var (grain, _, reminder, _, _) = CreateGrain();

        var result = await grain.ExecuteApplyAsync(TreeId, [], "site-x");

        Assert.Multiple(() =>
        {
            Assert.That(result.Outcome, Is.EqualTo(AtomicApplyOutcome.Committed));
            Assert.That(result.AppliedCount, Is.EqualTo(0));
            Assert.That(result.FailureReason, Is.Null);
        });
        // Empty-batch fast-path must not register a keepalive reminder.
        await reminder.DidNotReceive().RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
    }

    // --- Idempotent re-entry ---

    [Test]
    public async Task ExecuteApplyAsync_idempotent_retry_after_committed_returns_persisted_outcome()
    {
        var seeded = new FakePersistentState<AtomicWriteState>
        {
            State =
            {
                TreeId = TreeId,
                Phase = AtomicWritePhase.Completed,
                IsApplyMode = true,
                ApplyEntries = MakeApplyEntries(("a", [1], 1), ("b", [2], 2)),
                OriginClusterId = "site-x",
                FailureMessage = null,
            },
        };
        var (grain, _, _, _, _) = CreateGrain(seeded);

        var result = await grain.ExecuteApplyAsync(
            TreeId,
            MakeApplyEntries(("a", [1], 1), ("b", [2], 2)),
            "site-x");

        Assert.Multiple(() =>
        {
            Assert.That(result.Outcome, Is.EqualTo(AtomicApplyOutcome.Committed));
            Assert.That(result.AppliedCount, Is.EqualTo(2));
            Assert.That(result.FailureReason, Is.Null);
        });
    }

    [Test]
    public async Task ExecuteApplyAsync_idempotent_retry_after_compensated_returns_failure_reason()
    {
        var seeded = new FakePersistentState<AtomicWriteState>
        {
            State =
            {
                TreeId = TreeId,
                Phase = AtomicWritePhase.Completed,
                IsApplyMode = true,
                ApplyEntries = MakeApplyEntries(("a", [1], 1), ("b", [2], 2)),
                OriginClusterId = "site-x",
                FailureMessage = "shard down on b",
            },
        };
        var (grain, _, _, _, _) = CreateGrain(seeded);

        var result = await grain.ExecuteApplyAsync(
            TreeId,
            MakeApplyEntries(("a", [1], 1), ("b", [2], 2)),
            "site-x");

        Assert.Multiple(() =>
        {
            Assert.That(result.Outcome, Is.EqualTo(AtomicApplyOutcome.Compensated));
            Assert.That(result.AppliedCount, Is.EqualTo(0));
            Assert.That(result.FailureReason, Is.EqualTo("shard down on b"));
        });
    }

    [Test]
    public void ExecuteApplyAsync_idempotent_retry_validates_payload()
    {
        // Even on the Completed re-entry path, a malformed retry payload
        // must surface as a clean ArgumentException rather than a
        // downstream NullReferenceException.
        var seeded = new FakePersistentState<AtomicWriteState>
        {
            State =
            {
                TreeId = TreeId,
                Phase = AtomicWritePhase.Completed,
                IsApplyMode = true,
                ApplyEntries = MakeApplyEntries(("a", [1], 1)),
                OriginClusterId = "site-x",
            },
        };
        var (grain, _, _, _, _) = CreateGrain(seeded);

        var malformed = MakeApplyEntries(("dup", [1], 1), ("dup", [2], 2));
        Assert.ThrowsAsync<ArgumentException>(
            () => grain.ExecuteApplyAsync(TreeId, malformed, "site-x"));
    }

    // --- Fingerprint mismatch ---

    [Test]
    public async Task ExecuteApplyAsync_rejects_resubmit_with_different_key_set()
    {
        var (grain, _, _, _, _) = CreateGrain();
        var first = MakeApplyEntries(("a", [1], 1), ("b", [2], 2));
        await grain.ExecuteApplyAsync(TreeId, first, "site-x");

        var differentKeys = MakeApplyEntries(("a", [1], 1), ("c", [3], 3));

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.ExecuteApplyAsync(TreeId, differentKeys, "site-x"));
        Assert.That(ex!.Message, Does.Contain("different key set"));
    }

    [Test]
    public async Task ExecuteApplyAsync_accepts_resubmit_with_reordered_keys()
    {
        var (grain, _, _, _, _) = CreateGrain();
        var first = MakeApplyEntries(("a", [1], 1), ("b", [2], 2));
        await grain.ExecuteApplyAsync(TreeId, first, "site-x");

        var reordered = MakeApplyEntries(("b", [2], 2), ("a", [1], 1));

        var result = await grain.ExecuteApplyAsync(TreeId, reordered, "site-x");

        Assert.That(result.Outcome, Is.EqualTo(AtomicApplyOutcome.Committed));
    }

    [Test]
    public async Task ExecuteApplyAsync_mid_saga_failure_then_idempotent_retry_returns_same_compensated_outcome()
    {
        var (grain, _, _, lattice, shard) = CreateGrain();
        StubPreValue(shard, "a", null);
        StubPreValue(shard, "b", null);
        lattice.SetAsync("b", Arg.Any<byte[]>()).Throws(new InvalidOperationException("shard down"));

        var entries = MakeApplyEntries(("a", [1], 1), ("b", [2], 2));

        var first = await grain.ExecuteApplyAsync(TreeId, entries, "site-x");
        var second = await grain.ExecuteApplyAsync(TreeId, entries, "site-x");

        Assert.Multiple(() =>
        {
            Assert.That(first.Outcome, Is.EqualTo(AtomicApplyOutcome.Compensated));
            Assert.That(second.Outcome, Is.EqualTo(first.Outcome));
            Assert.That(second.AppliedCount, Is.EqualTo(first.AppliedCount));
            Assert.That(second.FailureReason, Is.EqualTo(first.FailureReason));
        });
    }

    // --- Grain-key collision guards ---

    [Test]
    public void ExecuteApplyAsync_rejects_collision_with_local_saga_grain()
    {
        var seeded = new FakePersistentState<AtomicWriteState>
        {
            State =
            {
                TreeId = TreeId,
                Phase = AtomicWritePhase.Completed,
                IsApplyMode = false,
                Entries = MakeEntries(("a", [1])),
            },
        };
        var (grain, _, _, _, _) = CreateGrain(seeded);

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.ExecuteApplyAsync(TreeId, MakeApplyEntries(("a", [1], 1)), "site-x"));
        Assert.That(ex!.Message, Does.Contain("collides with a previously-started local saga"));
    }

    [Test]
    public void ExecuteAsync_rejects_collision_with_apply_mode_saga_grain()
    {
        var seeded = new FakePersistentState<AtomicWriteState>
        {
            State =
            {
                TreeId = TreeId,
                Phase = AtomicWritePhase.Completed,
                IsApplyMode = true,
                ApplyEntries = MakeApplyEntries(("a", [1], 1)),
                OriginClusterId = "site-x",
            },
        };
        var (grain, _, _, _, _) = CreateGrain(seeded);

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.ExecuteAsync(TreeId, MakeEntries(("a", [1]))));
        Assert.That(ex!.Message, Does.Contain("collides with a previously-started apply-mode saga"));
    }
}
