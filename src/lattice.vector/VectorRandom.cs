namespace Orleans.Lattice.Vector;

/// <summary>
/// A seeded, allocation-free xorshift128+ generator. It is a mutable struct held
/// by value on the training stack so that drawing a number costs no allocation
/// and no virtual call, and it is deliberately not <see cref="Random"/>: the
/// framework generator's algorithm is an implementation detail that may change
/// between runtimes, whereas an index's training must reproduce bit-for-bit from
/// its seed on any machine and any release.
/// </summary>
internal struct VectorRandom
{
    private ulong _state0;
    private ulong _state1;

    /// <summary>Creates a generator from an explicit seed.</summary>
    /// <param name="seed">The seed. Every seed, including zero, produces a usable stream.</param>
    internal VectorRandom(ulong seed)
    {
        // SplitMix64 expansion, so a small or zero seed still fills both words
        // with well-mixed bits rather than leaving the generator near-degenerate.
        _state0 = SplitMix64(ref seed);
        _state1 = SplitMix64(ref seed);
        if (_state0 == 0 && _state1 == 0)
        {
            _state1 = 0x9E3779B97F4A7C15UL;
        }
    }

    /// <summary>Returns the next 64 raw pseudo-random bits.</summary>
    internal ulong NextUInt64()
    {
        var s1 = _state0;
        var s0 = _state1;
        _state0 = s0;
        s1 ^= s1 << 23;
        _state1 = s1 ^ s0 ^ (s1 >> 18) ^ (s0 >> 5);
        return _state1 + s0;
    }

    /// <summary>
    /// Returns a uniformly distributed integer in <c>[0, exclusiveUpperBound)</c>
    /// using Lemire's multiply-shift reduction, which is unbiased enough for
    /// sampling and needs no division.
    /// </summary>
    /// <param name="exclusiveUpperBound">The exclusive upper bound. Must be positive.</param>
    internal int NextInt32(int exclusiveUpperBound)
    {
        var product = (UInt128)NextUInt64() * (ulong)exclusiveUpperBound;
        return (int)(ulong)(product >> 64);
    }

    private static ulong SplitMix64(ref ulong state)
    {
        state += 0x9E3779B97F4A7C15UL;
        var z = state;
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBUL;
        return z ^ (z >> 31);
    }
}
