namespace Orleans.Lattice.Replication.Tests.PublicApiContract;

/// <summary>
/// Cross-cluster atomic-saga visibility coverage. Pins the contract
/// that <see cref="ILattice.SetManyAtomicAsync(System.Collections.Generic.List{System.Collections.Generic.KeyValuePair{string, byte[]}}, System.Threading.CancellationToken)"/>
/// is atomic <em>across replicated clusters</em> in addition to being
/// atomic within the authoring tree: a continuous reader on the
/// receiving site observes either zero or all of a saga's keys at
/// every poll - never a partial subset - and after convergence the
/// receiver projection is bit-identical to the source.
/// <para>
/// These are deterministic in-process integration tests: the
/// loopback transport ships every batch through the production
/// <see cref="IChangeFeed"/> -&gt; <see cref="IReplicationBatchEncoder"/>
/// -&gt; <see cref="IReplicationTransport"/> -&gt; <see cref="IReplicationApplier"/>
/// pipeline, so the prepared / terminal split on the wire and the
/// receiver-side pending-bucket gating are both under test without
/// the partition-cycling chaos pump.
/// </para>
/// </summary>
public partial class PublicReplicationApiContractTests
{
    private const int AtomicBatchSize = 8;

    [Test]
    public async Task SetManyAtomicAsync_replicates_atomically_across_clusters()
    {
        // Author a single saga on Site A. After convergence on Site B
        // every key must be visible AND carry the authored bytes.
        // The contract claim is the receiver applies the prepared
        // writes only after the terminal arrives, so the public
        // reader on Site B sees the full batch as one transition.
        var treeId = NextTreeId("atomic-saga-converges");
        var treeOnA = await CreateReplicatedTreeAsync(treeId);
        var treeOnB = _fixture.TreeOnB(treeId);

        var batch = new List<KeyValuePair<string, byte[]>>(AtomicBatchSize);
        for (var i = 0; i < AtomicBatchSize; i++)
        {
            batch.Add(Kvp($"atom-{i:D2}", $"v-{i:D2}"));
        }

        await treeOnA.SetManyAtomicAsync(batch);

        await PublicReplicationApiClusterFixture.WaitForConvergenceAsync(
            async () =>
            {
                for (var i = 0; i < AtomicBatchSize; i++)
                {
                    if (Str(await treeOnB.GetAsync($"atom-{i:D2}")) != $"v-{i:D2}")
                    {
                        return false;
                    }
                }
                return true;
            },
            $"Site B should see every key from the atomic saga authored on Site A for tree '{treeId}'.");

        for (var i = 0; i < AtomicBatchSize; i++)
        {
            var observed = await treeOnB.GetAsync($"atom-{i:D2}");
            Assert.That(Str(observed), Is.EqualTo($"v-{i:D2}"),
                $"Key 'atom-{i:D2}' must converge to the authored value on Site B.");
        }
    }

    [Test]
    public async Task SetManyAtomicAsync_visibility_on_remote_site_is_zero_or_all_throughout_convergence()
    {
        // While a saga is authored on Site A and is in-flight to
        // Site B, a continuous reader on Site B must NEVER observe a
        // partial subset of the saga's keys. Permitted observations
        // are: every key present, every key absent, or - if the
        // reader straddles two sagas - every key with the new value
        // and every key with the old value. A partial-count
        // observation in between is a cross-cluster atomicity
        // violation.
        var treeId = NextTreeId("atomic-saga-zero-or-all");
        var treeOnA = await CreateReplicatedTreeAsync(treeId);
        var treeOnB = _fixture.TreeOnB(treeId);

        // Seed every key with round 0 so the universe is pinned
        // before the test window opens. Wait for the seed to land
        // on Site B so the continuous reader has a stable starting
        // point.
        var seed = new List<KeyValuePair<string, byte[]>>(AtomicBatchSize);
        for (var i = 0; i < AtomicBatchSize; i++)
        {
            seed.Add(Kvp($"zorall-{i:D2}", "v-000"));
        }
        await treeOnA.SetManyAtomicAsync(seed);
        await PublicReplicationApiClusterFixture.WaitForConvergenceAsync(
            async () =>
            {
                for (var i = 0; i < AtomicBatchSize; i++)
                {
                    if (Str(await treeOnB.GetAsync($"zorall-{i:D2}")) != "v-000")
                    {
                        return false;
                    }
                }
                return true;
            },
            "seed convergence");

        // Stop guard for the continuous reader; flipped after the
        // workload completes and we have given Site B a final
        // convergence window.
        var stop = false;
        var partialObservation = (Round: -1, PresentCount: -1, Detail: (string?)null);
        var partialObservationLock = new object();

        var reader = Task.Run(async () =>
        {
            // Pre-build the key list once - it never changes during
            // the test window, and GetManyAsync takes a List<string>.
            var keysToRead = new List<string>(AtomicBatchSize);
            for (var i = 0; i < AtomicBatchSize; i++)
            {
                keysToRead.Add($"zorall-{i:D2}");
            }

            while (!Volatile.Read(ref stop))
            {
                // GetManyAsync is the atomic-snapshot read primitive:
                // it pre-fetches the per-tree TxRegistry snapshot,
                // fans the per-shard reads out under that ambient
                // view, and post-validates the snapshot for any
                // saga transition during fan-out (retrying on a
                // detected race). A multi-RPC parallel-GetAsync
                // sweep is NOT a snapshot - it observes per-key
                // wall-clock times that span the inter-saga
                // visibility flip - and would surface false
                // partial-observation alarms here.
                var values = await treeOnB.GetManyAsync(keysToRead);

                // Atomic-visibility invariant: every key must carry
                // the SAME round. The set of distinct rounds across
                // the batch must have cardinality 1. A snapshot that
                // observes more than one round is a partial-saga
                // visibility violation.
                var rounds = new HashSet<int>();
                for (var i = 0; i < AtomicBatchSize; i++)
                {
                    values.TryGetValue($"zorall-{i:D2}", out var v);
                    rounds.Add(RoundOf(v));
                }

                if (rounds.Count != 1)
                {
                    lock (partialObservationLock)
                    {
                        if (partialObservation.Round < 0)
                        {
                            // Capture the first violation for the
                            // failure message.
                            var presentCount = values.Count;
                            var detail = string.Join(",",
                                Enumerable.Range(0, AtomicBatchSize).Select(i =>
                                {
                                    values.TryGetValue($"zorall-{i:D2}", out var vv);
                                    return $"{i:D2}=r{RoundOf(vv)}";
                                }));
                            partialObservation = (rounds.First(), presentCount, detail);
                        }
                    }
                    return;
                }

                await Task.Delay(10);
            }
        });

        // Author rounds 1..N on Site A, each as a fresh saga.
        // Concurrent sagas would overlap on the receiver - but the
        // claim here is single-saga atomicity, so we serialise
        // authoring and rely on the continuous reader to catch any
        // partial visibility window introduced by the inter-cluster
        // ship pipeline.
        const int rounds = 6;
        for (var round = 1; round <= rounds; round++)
        {
            var roundBatch = new List<KeyValuePair<string, byte[]>>(AtomicBatchSize);
            for (var i = 0; i < AtomicBatchSize; i++)
            {
                roundBatch.Add(Kvp($"zorall-{i:D2}", $"v-{round:D3}"));
            }
            await treeOnA.SetManyAtomicAsync(roundBatch);
        }

        // Wait for the final round to land on Site B so the reader
        // has the chance to observe every round flip.
        await PublicReplicationApiClusterFixture.WaitForConvergenceAsync(
            async () =>
            {
                for (var i = 0; i < AtomicBatchSize; i++)
                {
                    if (Str(await treeOnB.GetAsync($"zorall-{i:D2}")) != $"v-{rounds:D3}")
                    {
                        return false;
                    }
                }
                return true;
            },
            $"final round '{rounds}' should converge on Site B for tree '{treeId}'.");

        Volatile.Write(ref stop, true);
        await reader;

        Assert.That(
            partialObservation.Round,
            Is.LessThan(0),
            $"Site B observed a PARTIAL saga visibility on tree '{treeId}': round={partialObservation.Round}, present={partialObservation.PresentCount}/{AtomicBatchSize}, detail=[{partialObservation.Detail}]. Cross-cluster atomic visibility was violated.");
    }

    /// <summary>
    /// Decodes the round number from a value byte buffer authored as
    /// <c>v-NNN</c> by the zero-or-all test. Returns <c>-1</c> for
    /// <see langword="null"/> / malformed / pre-seed values; the
    /// invariant check rejects mixed-round snapshots so a stray
    /// <c>-1</c> mixed with any real round still trips the assertion.
    /// </summary>
    private static int RoundOf(byte[]? value)
    {
        if (value is null)
        {
            return -1;
        }
        var s = Str(value);
        if (!s.StartsWith("v-", System.StringComparison.Ordinal))
        {
            return -1;
        }
        return int.TryParse(s.AsSpan(2), out var round) ? round : -1;
    }
}
