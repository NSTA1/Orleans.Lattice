using System.Globalization;

namespace VehicleFleetSimulator.AzureThroughput.Engine;

/// <summary>
/// Cheap data-correctness check for the read modes. Every key the producer
/// reads was written by the preseed as <see cref="BenchPreseed.PayloadFor"/>
/// of the index encoded in the key, so a read result can be checked against
/// its expected value with no extra storage round trip. A sampled subset of
/// reads is compared; a wrong value counts as a mismatch and an absent value
/// as missing. Either is a correctness failure, not a performance one.
/// </summary>
public sealed class BenchReadVerifier
{
    private readonly int sampleEvery;
    private long seen;
    private long verified;
    private long mismatch;
    private long missing;
    private long unverifiable;

    /// <summary>Creates a verifier that checks one read in every <paramref name="sampleEvery"/>.</summary>
    /// <param name="sampleEvery">Sampling stride; 1 checks every read.</param>
    public BenchReadVerifier(int sampleEvery = 1)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(sampleEvery, 1);
        this.sampleEvery = sampleEvery;
    }

    /// <summary>The sampling stride.</summary>
    public int SampleEvery => sampleEvery;

    /// <summary>Reads checked and found correct.</summary>
    public long Verified => Interlocked.Read(ref verified);

    /// <summary>Reads that returned a value different from the one seeded.</summary>
    public long Mismatch => Interlocked.Read(ref mismatch);

    /// <summary>Reads that returned no value for a seeded key.</summary>
    public long Missing => Interlocked.Read(ref missing);

    /// <summary>Sampled keys whose expected value could not be derived.</summary>
    public long Unverifiable => Interlocked.Read(ref unverifiable);

    /// <summary>
    /// Builds a verifier from <c>BENCH_READ_VERIFY_SAMPLE</c>: unset means 1
    /// (check every read), <c>0</c> disables verification (returns null).
    /// </summary>
    /// <param name="value">The raw environment value.</param>
    public static BenchReadVerifier? FromEnvironmentValue(string? value)
    {
        if (string.IsNullOrWhiteSpace(value)) return new BenchReadVerifier();
        if (!int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var stride) || stride < 0)
        {
            return new BenchReadVerifier();
        }

        return stride == 0 ? null : new BenchReadVerifier(stride);
    }

    /// <summary>Checks one read result if it falls in the sample.</summary>
    /// <param name="key">The key that was read.</param>
    /// <param name="value">The value returned, or null when absent.</param>
    public void Observe(string key, byte[]? value)
    {
        ArgumentNullException.ThrowIfNull(key);
        if (sampleEvery > 1 && Interlocked.Increment(ref seen) % sampleEvery != 0) return;
        if (!TryIndexFor(key, out var index))
        {
            Interlocked.Increment(ref unverifiable);
            return;
        }

        if (value is null)
        {
            Interlocked.Increment(ref missing);
        }
        else if (Matches(index, value))
        {
            Interlocked.Increment(ref verified);
        }
        else
        {
            Interlocked.Increment(ref mismatch);
        }
    }

    /// <summary>The <c>read-verify-final</c> fields, for the FINAL report.</summary>
    public string Format() => FormattableString.Invariant(
        $"verified={Verified} mismatch={Mismatch} missing={Missing} unverifiable={Unverifiable} sample={sampleEvery}");

    private static bool TryIndexFor(string key, out int index)
    {
        index = 0;
        if (!Guid.TryParseExact(key, "N", out var guid)) return false;
        Span<byte> bytes = stackalloc byte[16];
        guid.TryWriteBytes(bytes);
        index = BitConverter.ToInt32(bytes[..4]);
        return string.Equals(BenchPreseed.KeyFor(index), key, StringComparison.Ordinal);
    }

    private static bool Matches(int index, byte[] value)
    {
        if (value.Length != BenchPreseed.PayloadBytes) return false;
        for (var b = 0; b < value.Length; b++)
        {
            if (value[b] != (byte)((index + b) & 0xFF)) return false;
        }

        return true;
    }
}
