using System.Collections.Concurrent;
using System.Text;
using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Chaos proof that single-tree atomic writes stay <b>all-or-nothing</b> and every
/// stored value stays <b>self-describing and decodable</b> while a versioned tree's
/// target schema version is advanced concurrently - and that the one-call eager
/// background migration (#1204) preserves that atomic snapshot when it re-stamps the
/// tree.
/// </summary>
/// <remarks>
/// <para>
/// The test runs in two phases against the same tree, matching the two distinct
/// version-change operations and their concurrency contracts:
/// </para>
/// <para>
/// <b>Phase A - concurrent lazy advance.</b> A committer drives a chain of single-tree
/// <see cref="ILattice.SetManyAtomicAsync(System.Collections.Generic.List{System.Collections.Generic.KeyValuePair{string, byte[]}}, System.Threading.CancellationToken)"/>
/// batches while a churner advances the target version v1 -&gt; v2 -&gt; v3 and a
/// reader polls every key. A target-version advance is a config-only change (it moves
/// no data), so it is safe to run concurrently with live writes: new writes stamp at
/// the new target and existing values are upcast on read. The target is monotonic and
/// an upcaster hop is registered for every pair, so a read never fails. Invariants:
/// (a) every atomic batch lands as a unit (all keys at the same generation), (b) a
/// read never returns raw enveloped bytes (the leading
/// <see cref="LatticeSchemaEnvelope.Magic"/> byte is always stripped by the read
/// decoder) and never throws for a reachable version, and (c) the concurrent advance
/// never tears a batch.
/// </para>
/// <para>
/// <b>Phase B - quiesced eager migration.</b> After the write loop has drained (the
/// tree is quiescent), a single
/// <c>MigrateToTargetVersionAsync</c> re-stamps every value from its own stamped
/// version to the target through the registry. Eager migration is a shadow-build with
/// alias cutover whose v1 contract requires the tree be write-quiescent, so it is
/// validated here in a quiesced window - not hammered under live writes. Invariant:
/// the re-stamped tree still reads back the exact all-or-nothing snapshot the last
/// committed generation installed, every value still decodes, and none leaks raw
/// enveloped bytes.
/// </para>
/// </remarks>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public sealed class AtomicWriteUnderVersionAdvanceChaosTests
{
    private const int KeyCount = 12;
    private const int GenerationCount = 60;
    private static readonly TimeSpan PollCadence = TimeSpan.FromMilliseconds(5);

    private SchemaAtomicChaosClusterFixture _fixture = null!;

    private IGrainFactory Grains => _fixture.Grains;
    private ILatticeSchemaVersionAdmin VersionAdmin =>
        _fixture.SiloServices.GetRequiredService<ILatticeSchemaVersionAdmin>();

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new SchemaAtomicChaosClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private static string KeyOf(int i) => $"vk-{i:D2}";
    private static byte[] Value(int gen, int i) => Encoding.UTF8.GetBytes($"{{\"g\":{gen},\"i\":{i}}}");

    private static int GenerationOf(byte[]? value)
    {
        if (value is null || value.Length == 0)
        {
            return -1;
        }

        if (value[0] == LatticeSchemaEnvelope.Magic)
        {
            return -2; // raw envelope leaked to caller: the read decoder did not run
        }

        var s = Encoding.UTF8.GetString(value);
        var i = s.IndexOf(':');
        var j = s.IndexOf(',');
        return i > 0 && j > i && int.TryParse(s.AsSpan(i + 1, j - i - 1), out var g) ? g : -1;
    }

    [Test]
    public async Task Single_tree_atomic_write_is_all_or_nothing_and_decodable_across_version_advance_and_eager_migration()
    {
        var tree = $"ver-advance-{Guid.NewGuid():N}";
        var keys = Enumerable.Range(0, KeyCount).Select(KeyOf).ToList();
        var lattice = Grains.GetGrain<ILattice>(tree);

        // Opt the tree in to versioning at v1; the churner advances it under load.
        await VersionAdmin.SetVersionConfigAsync(
            tree, new LatticeSchemaVersionConfig(SchemaAtomicChaosClusterFixture.SchemaId, 1));

        var failures = new ConcurrentBag<string>();
        long advances = 0;

        // Phase A: advance the target version v1 -> v2 -> v3 concurrently with the
        // atomic write / read loop. A target advance is a config-only change, safe to
        // run under live writes; the eager data migration is deferred to Phase B.
        using var churnCts = new CancellationTokenSource();
        var churner = Task.Run(async () =>
        {
            uint target = 1;
            while (!churnCts.IsCancellationRequested)
            {
                try
                {
                    if (target < 3)
                    {
                        target++;
                        await VersionAdmin.AdvanceTargetVersionAsync(tree, target);
                        Interlocked.Increment(ref advances);
                    }
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (Exception ex)
                {
                    failures.Add($"churn: {ex.GetType().Name}: {ex.Message}");
                }

                try
                {
                    // Space the two advances out so they interleave with the write loop.
                    await Task.Delay(PollCadence * 4, churnCts.Token);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        });

        for (int gen = 1; gen <= GenerationCount; gen++)
        {
            var batch = keys.Select((k, i) => new KeyValuePair<string, byte[]>(k, Value(gen, i))).ToList();

            var reader = Task.Run(async () =>
            {
                var snap = await lattice.GetManyAsync(keys);
                var observed = keys.Select(k => GenerationOf(snap.GetValueOrDefault(k))).ToList();
                if (observed.Contains(-2))
                {
                    failures.Add($"gen={gen}: reader observed raw enveloped bytes (decoder did not run)");
                }

                if (observed.Where(g => g >= 0).Distinct().Count() > 1)
                {
                    failures.Add($"gen={gen}: reader observed a torn batch across generations");
                }
            });

            await lattice.SetManyAtomicAsync(batch);
            await reader;

            var back = await lattice.GetManyAsync(keys);
            foreach (var k in keys)
            {
                var g = GenerationOf(back.GetValueOrDefault(k));
                if (g == -2)
                {
                    failures.Add($"gen={gen}: read-back of {k} returned raw enveloped bytes");
                }
                else if (g != gen)
                {
                    failures.Add($"gen={gen}: read-back of {k} == {g} (not all-or-nothing)");
                }
            }
        }

        churnCts.Cancel();
        try
        {
            await churner;
        }
        catch (OperationCanceledException)
        {
        }

        // Phase B: the tree is now write-quiescent. Run the one-call eager migration,
        // which re-stamps every value (written across v1..v3 during the loop) to the
        // current target. It must preserve the last committed generation on every key,
        // keep every value decodable, and leak no raw enveloped bytes.
        var report = await VersionAdmin.MigrateToTargetVersionAsync(tree);

        var migrated = await lattice.GetManyAsync(keys);
        foreach (var k in keys)
        {
            var g = GenerationOf(migrated.GetValueOrDefault(k));
            if (g == -2)
            {
                failures.Add($"post-migration: read-back of {k} returned raw enveloped bytes");
            }
            else if (g != GenerationCount)
            {
                failures.Add($"post-migration: read-back of {k} == {g} (migration did not preserve the atomic snapshot)");
            }
        }

        // A second migration to the same target is an idempotent no-op success.
        var again = await VersionAdmin.MigrateToTargetVersionAsync(tree);

        Assert.Multiple(() =>
        {
            Assert.That(failures, Is.Empty,
                "Atomic / decodability invariant violated under version advance + eager migration."
                + Environment.NewLine + string.Join(Environment.NewLine, failures));
            Assert.That(advances, Is.EqualTo(2), "expected the target to advance v1->v2->v3 exactly once each");
            Assert.That(report.Succeeded, Is.True, "eager migration did not succeed");
            Assert.That(again.Succeeded, Is.True, "idempotent re-migration did not succeed");
        });
        TestContext.Out.WriteLine($"advances={advances}, migrated scanned={report.ScannedCount}");
    }
}
