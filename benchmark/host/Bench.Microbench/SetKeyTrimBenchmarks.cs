using System;
using System.Buffers;
using System.Collections.Generic;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates a transient base64 <see cref="string"/> that the OR-set and RW-set
/// delta accessors materialise on every staged remove purely to index a
/// dictionary.
/// <para>
/// Both <c>RemoveDelta</c> bodies open with
/// <c>var key = Convert.ToBase64String(element);</c> and then use <c>key</c> for
/// exactly one thing - a <c>TryGetValue</c> against the set's
/// <c>Dictionary&lt;string, List&lt;OrSetDot&gt;&gt;</c>. The string is never
/// stored, returned, logged, or compared; it is dead the moment the lookup
/// returns.
/// </para>
/// <para>
/// <c>OrSet</c> itself already solved this five times over: encode into a
/// <c>Span&lt;char&gt;</c> - stack-allocated below a character threshold and
/// rented from <see cref="ArrayPool{T}"/> above it - and index through
/// <c>Dictionary.GetAlternateLookup&lt;ReadOnlySpan&lt;char&gt;&gt;()</c>, which
/// hashes and compares the span directly against the stored keys without
/// building one. The two accessors were never converted.
/// </para>
/// <para>
/// Read this suite for <b>bytes</b>. The expected value is closed-form: a
/// <see cref="string"/> of <c>n</c> characters costs <c>22 + 2n</c> bytes
/// rounded to the allocation granularity, and base64 of <c>b</c> bytes is
/// <c>(b + 2) / 3 * 4</c> characters - so a 16-byte element costs 24 characters
/// (72 bytes) and a 512-byte element 684 characters (1392 bytes). The optimized
/// lane allocates nothing at the small width and nothing on the steady state at
/// the large one, because the pool rental is returned.
/// </para>
/// <para>
/// <see cref="ElementBytes"/> straddles the 256-character stack threshold
/// deliberately. 16 bytes is the common element width, where the optimized lane
/// must take the <c>stackalloc</c> branch; 512 bytes encodes to 684 characters
/// and so forces the <see cref="ArrayPool{T}"/> branch, which is the lane that
/// could plausibly lose. A trim whose two parameter siblings disagree in sign
/// is the lane's fault, not the change's.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=setkeytrims</c> (or
/// <c>--suite setkeytrims</c>); see <c>Program.cs</c>. No Orleans silo is
/// involved, so it runs cheaply at <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class SetKeyTrimBenchmarks
{
    /// <summary>
    /// Base64 characters that still fit the stack buffer, copied verbatim from
    /// <c>OrSet.MaxStackBase64Chars</c>. Above it the optimized shape rents.
    /// </summary>
    private const int MaxStackBase64Chars = 256;

    /// <summary>
    /// How many staged removes one operation performs. A single dictionary
    /// probe sits under timer resolution, so both lanes repeat equally; the
    /// per-call ratio is unchanged.
    /// </summary>
    private const int RemoveRepeats = 64;

    /// <summary>
    /// Element width in bytes. 16 exercises the <c>stackalloc</c> branch (24
    /// base64 characters); 512 encodes to 684 characters and so exercises the
    /// <see cref="ArrayPool{T}"/> branch.
    /// </summary>
    [Params(16, 512)]
    public int ElementBytes { get; set; }

    private OrSet _set = null!;
    private byte[][] _elements = null!;

    [GlobalSetup]
    public void Setup()
    {
        _set = new OrSet();
        _elements = new byte[RemoveRepeats][];
        for (var i = 0; i < RemoveRepeats; i++)
        {
            var element = new byte[ElementBytes];
            for (var b = 0; b < ElementBytes; b++)
            {
                element[b] = (byte)((i * 31 + b) & 0xFF);
            }

            _elements[i] = element;

            // Half the corpus is present and half absent, so both the hit and
            // the miss path through the lookup are exercised; a lane that only
            // ever missed would skip the stored-key comparison the alternate
            // lookup has to perform.
            if ((i & 1) == 0)
            {
                _set.Add(element, $"replica-{i % 4}", i);
            }
        }

        AssertEquivalence();
    }

    /// <summary>
    /// Proves the span lookup resolves to the identical dot list - by reference
    /// - as the string lookup, for both present and absent elements and at both
    /// widths, before any measurement is attributed to it.
    /// </summary>
    private void AssertEquivalence()
    {
        for (var i = 0; i < RemoveRepeats; i++)
        {
            var viaStringFound = LookupViaString(_elements[i], out var viaStringDots);
            var viaSpanFound = LookupViaSpan(_elements[i], out var viaSpanDots);

            if (viaStringFound != viaSpanFound)
            {
                throw new InvalidOperationException(
                    $"Set key lookup presence diverged at {i}: string={viaStringFound}, span={viaSpanFound}.");
            }

            if (!ReferenceEquals(viaStringDots, viaSpanDots))
            {
                throw new InvalidOperationException($"Set key lookup resolved a different dot list at {i}.");
            }
        }
    }

    private bool LookupViaString(byte[] element, out List<OrSetDot>? dots)
    {
        var key = Convert.ToBase64String(element);
        return _set.Adds.TryGetValue(key, out dots);
    }

    private bool LookupViaSpan(byte[] element, out List<OrSetDot>? dots)
    {
        var charCount = checked((element.Length + 2) / 3 * 4);
        char[]? rented = charCount > MaxStackBase64Chars ? ArrayPool<char>.Shared.Rent(charCount) : null;
        try
        {
            Span<char> buffer = rented ?? stackalloc char[MaxStackBase64Chars];
            if (!Convert.TryToBase64Chars(element, buffer, out var written))
            {
                throw new InvalidOperationException("Base64 encoding overflowed its buffer.");
            }

            var adds = _set.Adds.GetAlternateLookup<ReadOnlySpan<char>>();
            return adds.TryGetValue(buffer[..written], out dots);
        }
        finally
        {
            if (rented is not null)
            {
                ArrayPool<char>.Shared.Return(rented);
            }
        }
    }

    /// <summary>
    /// Baseline: the shipped <c>RemoveDelta</c> opening, verbatim - one base64
    /// string per staged remove, used only to index the dictionary.
    /// </summary>
    [Benchmark(Baseline = true)]
    public int RemoveDelta_Base64String()
    {
        var sink = 0;
        for (var i = 0; i < RemoveRepeats; i++)
        {
            if (LookupViaString(_elements[i], out var dots) && dots!.Count > 0)
            {
                sink += dots.Count;
            }
        }

        return sink;
    }

    /// <summary>
    /// The accessor after the change: encode into a stack or pooled
    /// <see cref="Span{T}"/> and probe through the alternate lookup, so no
    /// string is built.
    /// </summary>
    [Benchmark]
    public int RemoveDelta_Base64Span()
    {
        var sink = 0;
        for (var i = 0; i < RemoveRepeats; i++)
        {
            if (LookupViaSpan(_elements[i], out var dots) && dots!.Count > 0)
            {
                sink += dots.Count;
            }
        }

        return sink;
    }
}
