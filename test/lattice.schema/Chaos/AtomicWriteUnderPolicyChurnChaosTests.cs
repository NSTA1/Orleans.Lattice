using System.Collections.Concurrent;
using System.Text;
using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Chaos proof that cross-tree atomic writes stay <b>all-or-nothing</b> while a
/// tree's enforcement policy is churned (set / cleared) concurrently. A committer
/// drives a chain of <see cref="LatticeCrossTreeAtomicWriteExtensions.SetManyAtomicAsync"/>
/// sagas - alternating a compliant and a deliberately non-compliant leg - while a
/// policy churner flips each participating tree's policy between "require JSON" and
/// "no policy", and a concurrent reader polls every key on every generation.
/// </summary>
/// <remarks>
/// The interceptor validates the whole batch once, up front at the coordinator,
/// against whatever policy is current at admission, so every saga is decided
/// atomically: it either commits every leg or rejects the whole batch (a
/// <see cref="LatticeSchemaViolationException"/> or a non-committed outcome) and
/// mutates no tree. The concurrent policy churn changes <i>which</i> outcome a given
/// generation gets, but never splits a batch - which is exactly the guarantee under
/// test. The reader never observes a torn (cross-generation) per-tree snapshot.
/// </remarks>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public sealed class AtomicWriteUnderPolicyChurnChaosTests
{
    private const int KeysPerTree = 8;
    private const int GenerationCount = 60;
    private static readonly TimeSpan PollCadence = TimeSpan.FromMilliseconds(5);

    private SchemaAtomicChaosClusterFixture _fixture = null!;

    private IGrainFactory Grains => _fixture.Grains;
    private ILatticeSchemaAdmin Admin => _fixture.SiloServices.GetRequiredService<ILatticeSchemaAdmin>();

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new SchemaAtomicChaosClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private static string KeyOf(int i) => $"k-{i:D2}";
    private static byte[] Compliant(int gen) => Encoding.UTF8.GetBytes($"{{\"g\":{gen}}}");

    // A deliberately non-JSON value (so a "require JSON" policy rejects it) that still
    // embeds the generation, so a committed non-compliant leg is identifiable by
    // GenerationOf just like a compliant one - the atomicity assertion is about which
    // generation landed, not about JSON validity.
    private static byte[] NonCompliant(int gen) => Encoding.UTF8.GetBytes($"nope-{gen}");

    // Extracts the generation embedded as the first run of digits in the value, so it
    // reads both a compliant `{"g":N}` and a non-compliant `nope-N` value uniformly.
    private static int GenerationOf(byte[]? value)
    {
        if (value is null || value.Length == 0)
        {
            return -1;
        }

        var s = Encoding.UTF8.GetString(value);
        var i = 0;
        while (i < s.Length && !char.IsDigit(s[i]))
        {
            i++;
        }

        var j = i;
        while (j < s.Length && char.IsDigit(s[j]))
        {
            j++;
        }

        return i < j && int.TryParse(s.AsSpan(i, j - i), out var g) ? g : -1;
    }

    [Test]
    public async Task Cross_tree_atomic_write_is_all_or_nothing_while_policy_is_churned()
    {
        var treeA = $"pol-churn-a-{Guid.NewGuid():N}";
        var treeB = $"pol-churn-b-{Guid.NewGuid():N}";
        var keys = Enumerable.Range(0, KeysPerTree).Select(KeyOf).ToList();

        var failures = new ConcurrentBag<string>();
        long committed = 0, rejected = 0;

        using var churnCts = new CancellationTokenSource();
        var churner = Task.Run(async () =>
        {
            var rng = new Random(20260712);
            while (!churnCts.IsCancellationRequested)
            {
                foreach (var tree in new[] { treeA, treeB })
                {
                    try
                    {
                        if (rng.Next(2) == 0)
                        {
                            await Admin.SetPolicyAsync(tree, new LatticeSchemaPolicy(new[] { LatticeSchemaRule.Json() }));
                        }
                        else
                        {
                            await Admin.ClearPolicyAsync(tree);
                        }
                    }
                    catch (Exception ex)
                    {
                        failures.Add($"churn: {ex.GetType().Name}: {ex.Message}");
                    }
                }

                try
                {
                    await Task.Delay(PollCadence, churnCts.Token);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        });

        for (int gen = 1; gen <= GenerationCount; gen++)
        {
            // Even generations carry a non-compliant leg so the reject branch is
            // exercised whenever treeB currently has a JSON policy; odd generations
            // are fully compliant and always commit.
            var legValue = gen % 2 == 0 ? NonCompliant(gen) : Compliant(gen);
            var batches = new List<LatticeTreeBatch>
            {
                new(treeA, keys.Select(k => new KeyValuePair<string, byte[]>(k, Compliant(gen))).ToList()),
                new(treeB, keys.Select(k => new KeyValuePair<string, byte[]>(k, legValue)).ToList()),
            };

            var reader = PollOnceForTornBatch(treeA, treeB, keys, failures);

            bool didCommit;
            try
            {
                var outcome = await Grains.SetManyAtomicAsync(batches, Guid.NewGuid().ToString("N"));
                didCommit = outcome == CrossTreeAtomicWriteOutcome.Committed;
            }
            catch (LatticeSchemaViolationException)
            {
                didCommit = false;
            }

            await reader;

            // All-or-nothing: read back every key in BOTH trees and assert the batch
            // landed as a unit for whichever outcome the race produced.
            var a = await Grains.GetGrain<ILattice>(treeA).GetManyAsync(keys);
            var b = await Grains.GetGrain<ILattice>(treeB).GetManyAsync(keys);
            if (didCommit)
            {
                committed++;
                foreach (var k in keys)
                {
                    if (GenerationOf(a.GetValueOrDefault(k)) != gen)
                    {
                        failures.Add($"gen={gen} committed but treeA[{k}] != gen");
                    }

                    if (GenerationOf(b.GetValueOrDefault(k)) != gen)
                    {
                        failures.Add($"gen={gen} committed but treeB[{k}] != gen");
                    }
                }
            }
            else
            {
                rejected++;
                // Rejected: neither tree advanced to this generation on any key.
                foreach (var k in keys)
                {
                    if (GenerationOf(a.GetValueOrDefault(k)) == gen)
                    {
                        failures.Add($"gen={gen} rejected but treeA[{k}] == gen (partial apply)");
                    }

                    if (GenerationOf(b.GetValueOrDefault(k)) == gen)
                    {
                        failures.Add($"gen={gen} rejected but treeB[{k}] == gen (partial apply)");
                    }
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

        Assert.Multiple(() =>
        {
            Assert.That(failures, Is.Empty,
                "Atomic all-or-nothing violated under policy churn." + Environment.NewLine
                + string.Join(Environment.NewLine, failures));
            Assert.That(committed, Is.GreaterThan(0), "expected some generations to commit");
            Assert.That(rejected, Is.GreaterThan(0), "expected some generations to be rejected by the churned policy");
        });
        TestContext.Out.WriteLine($"committed={committed}, rejected={rejected}");
    }

    // A single concurrent read of both trees while the saga is in flight: within one
    // tree a committed saga installs all keys at the same generation, so a settled
    // snapshot has at most one non-negative generation - never a torn mix that would
    // reveal a partial saga.
    private Task PollOnceForTornBatch(string treeA, string treeB, List<string> keys, ConcurrentBag<string> failures) =>
        Task.Run(async () =>
        {
            var a = await Grains.GetGrain<ILattice>(treeA).GetManyAsync(keys);
            var b = await Grains.GetGrain<ILattice>(treeB).GetManyAsync(keys);
            AssertUniformOrEmpty(a, keys, "treeA", failures);
            AssertUniformOrEmpty(b, keys, "treeB", failures);
        });

    private static void AssertUniformOrEmpty(
        Dictionary<string, byte[]> snapshot, List<string> keys, string label, ConcurrentBag<string> failures)
    {
        var distinctPositive = keys
            .Select(k => GenerationOf(snapshot.GetValueOrDefault(k)))
            .Where(g => g >= 0)
            .Distinct()
            .Count();
        if (distinctPositive > 1)
        {
            failures.Add($"{label}: torn snapshot across generations");
        }
    }
}
