namespace Orleans.Lattice.Vector.Tests;

/// <summary>
/// Deterministic synthetic corpora and the brute-force exact search the recall
/// harness measures against.
/// <para>
/// The generator uses its own xorshift stream rather than <see cref="Random"/> so
/// a corpus is reproducible across runtimes and releases, which is what makes a
/// committed recall figure meaningful rather than a snapshot of one machine.
/// </para>
/// </summary>
internal static class VectorCorpus
{
    /// <summary>
    /// Builds a clustered corpus: <paramref name="clusters"/> randomly placed
    /// centres, each surrounded by Gaussian noise. Real embedding corpora are
    /// strongly clustered, so this is the geometry an inverted file is actually
    /// deployed against.
    /// </summary>
    internal static float[][] Clustered(int count, int dimensions, int clusters, ulong seed, float spread = 0.35f)
    {
        var random = new TestRandom(seed);
        var centres = new float[clusters][];
        for (var c = 0; c < clusters; c++)
        {
            centres[c] = new float[dimensions];
            for (var d = 0; d < dimensions; d++)
            {
                centres[c][d] = random.NextGaussian();
            }
        }

        var corpus = new float[count][];
        for (var i = 0; i < count; i++)
        {
            var centre = centres[random.NextInt32(clusters)];
            var vector = new float[dimensions];
            for (var d = 0; d < dimensions; d++)
            {
                vector[d] = centre[d] + (spread * random.NextGaussian());
            }

            corpus[i] = vector;
        }

        return corpus;
    }

    /// <summary>
    /// Builds an unclustered corpus of independent Gaussian vectors. This is the
    /// adversarial case for any partitioning scheme - there is no cluster
    /// structure to exploit - and is measured alongside the clustered case so the
    /// published recall figure is not flattered by a friendly corpus.
    /// </summary>
    internal static float[][] Uniform(int count, int dimensions, ulong seed)
    {
        var random = new TestRandom(seed);
        var corpus = new float[count][];
        for (var i = 0; i < count; i++)
        {
            var vector = new float[dimensions];
            for (var d = 0; d < dimensions; d++)
            {
                vector[d] = random.NextGaussian();
            }

            corpus[i] = vector;
        }

        return corpus;
    }

    /// <summary>
    /// Computes the exact top-k by brute force, using the identical total order
    /// the index uses (descending score, ascending key), so a recall comparison
    /// is not confounded by a different tie-break.
    /// </summary>
    internal static long[] ExactTopK(
        IReadOnlyList<float[]> corpus,
        IReadOnlyList<long> keys,
        ReadOnlySpan<float> query,
        int k,
        VectorDistanceMetric metric)
    {
        var results = new List<VectorSearchResult>(corpus.Count);
        for (var i = 0; i < corpus.Count; i++)
        {
            var score = metric == VectorDistanceMetric.Cosine
                ? VectorSimilarity.Cosine(query, corpus[i])
                : VectorSimilarity.Dot(query, corpus[i]);
            results.Add(new VectorSearchResult(keys[i], score));
        }

        results.Sort(static (left, right) =>
        {
            var byScore = right.Score.CompareTo(left.Score);
            return byScore != 0 ? byScore : left.Key.CompareTo(right.Key);
        });

        var take = Math.Min(k, results.Count);
        var top = new long[take];
        for (var i = 0; i < take; i++)
        {
            top[i] = results[i].Key;
        }

        return top;
    }

    /// <summary>
    /// The fraction of the exact top-k that the approximate result set also
    /// found, in <c>[0, 1]</c>.
    /// </summary>
    internal static double Recall(ReadOnlySpan<VectorSearchResult> approximate, long[] exact)
    {
        if (exact.Length == 0)
        {
            return 1d;
        }

        var expected = new HashSet<long>(exact);
        var hits = 0;
        foreach (var result in approximate)
        {
            if (expected.Contains(result.Key))
            {
                hits++;
            }
        }

        return (double)hits / exact.Length;
    }

    /// <summary>
    /// A small reproducible generator: xorshift128+ bits, with Box-Muller on top
    /// for the Gaussian draws the corpora need.
    /// </summary>
    internal struct TestRandom(ulong seed)
    {
        private ulong _state0 = Mix(ref seed);
        private ulong _state1 = Mix(ref seed);

        internal ulong NextUInt64()
        {
            var s1 = _state0;
            var s0 = _state1;
            _state0 = s0;
            s1 ^= s1 << 23;
            _state1 = s1 ^ s0 ^ (s1 >> 18) ^ (s0 >> 5);
            return _state1 + s0;
        }

        internal int NextInt32(int exclusiveUpperBound) =>
            (int)(ulong)(((UInt128)NextUInt64() * (ulong)exclusiveUpperBound) >> 64);

        internal double NextDouble() => (NextUInt64() >> 11) * (1.0 / (1UL << 53));

        internal float NextGaussian()
        {
            var u1 = Math.Max(NextDouble(), double.Epsilon);
            var u2 = NextDouble();
            return (float)(Math.Sqrt(-2.0 * Math.Log(u1)) * Math.Cos(2.0 * Math.PI * u2));
        }

        private static ulong Mix(ref ulong state)
        {
            state += 0x9E3779B97F4A7C15UL;
            var z = state;
            z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL;
            z = (z ^ (z >> 27)) * 0x94D049BB133111EBUL;
            return z ^ (z >> 31);
        }
    }
}
