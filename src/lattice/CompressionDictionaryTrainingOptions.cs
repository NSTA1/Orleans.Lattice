namespace Orleans.Lattice;

/// <summary>
/// Configuration for the auto-trained shared compression-dictionary provider
/// (<see cref="AutoTrainingCompressionDictionaryProvider"/>). The provider is
/// opt-in and off by default: leave <see cref="Enabled"/> at its default
/// <see langword="false"/> and the provider trains nothing, resolves no
/// dictionary id, and emits no telemetry - an allocation-free no-op.
/// <para>
/// Every knob bounds a resource. The sampled reservoir is capped by both
/// sample count (<see cref="MaxSampleCount"/>) and total bytes
/// (<see cref="MaxReservoirBytes"/>), with a per-sample ceiling
/// (<see cref="MaxSampleBytes"/>) and a sampling probability
/// (<see cref="SamplingRate"/>). The trained dictionary size is capped by
/// <see cref="DictionaryCapacityBytes"/>. Training cadence is rate-limited by
/// <see cref="MinTrainingInterval"/> and floored by <see cref="MinSamplesToTrain"/>.
/// Only <see cref="RetainedVersionCount"/> recent dictionary versions are kept
/// resolvable, and published ids count up from <see cref="FirstDictionaryId"/>.
/// </para>
/// </summary>
public sealed class CompressionDictionaryTrainingOptions
{
    /// <summary>
    /// Whether auto-training is active. Defaults to <see langword="false"/>:
    /// while disabled the provider ignores observed payloads, trains nothing,
    /// resolves no dictionary id, and emits no telemetry.
    /// </summary>
    public bool Enabled { get; set; }

    /// <summary>
    /// Maximum number of payload samples retained in the reservoir. When the
    /// cap is reached the oldest sample is evicted to admit a new one. Must be
    /// at least <c>1</c>. Defaults to <c>1024</c>.
    /// </summary>
    public int MaxSampleCount { get; set; } = 1024;

    /// <summary>
    /// Maximum total bytes retained across all reservoir samples. When the cap
    /// is reached the oldest samples are evicted until the reservoir fits.
    /// Must be at least <c>1</c> and at least <see cref="MaxSampleBytes"/>.
    /// Defaults to 8 MiB.
    /// </summary>
    public long MaxReservoirBytes { get; set; } = 8L * 1024 * 1024;

    /// <summary>
    /// Maximum size of a single observed payload that may enter the reservoir.
    /// Larger payloads are ignored (never partially copied). Must be at least
    /// <c>1</c>. Defaults to 64 KiB.
    /// </summary>
    public int MaxSampleBytes { get; set; } = 64 * 1024;

    /// <summary>
    /// Probability in <c>[0, 1]</c> that an observed payload is admitted to the
    /// reservoir. <c>1.0</c> (the default) samples every payload; lower values
    /// thin a high-volume stream so the reservoir holds a representative spread
    /// without pinning the newest traffic. Must not be <see cref="double.NaN"/>.
    /// </summary>
    public double SamplingRate { get; set; } = 1.0;

    /// <summary>
    /// Maximum size in bytes of a trained dictionary. Caps both the training
    /// output and the in-memory footprint of every retained version. Must be at
    /// least <c>1</c>. Defaults to 112 KiB (the Zstandard reference default).
    /// </summary>
    public int DictionaryCapacityBytes { get; set; } = 112 * 1024;

    /// <summary>
    /// Minimum number of reservoir samples required before a training pass runs.
    /// A pass requested below this floor is skipped (counted as an
    /// insufficient-samples outcome) and never throws. Must be at least <c>1</c>.
    /// Defaults to <c>100</c>.
    /// </summary>
    public int MinSamplesToTrain { get; set; } = 100;

    /// <summary>
    /// Minimum wall-clock interval between training passes. A pass requested
    /// inside this window since the previous attempt is skipped (counted as a
    /// cadence outcome). Must not be negative; <see cref="TimeSpan.Zero"/>
    /// disables the cadence gate. Defaults to 5 minutes.
    /// </summary>
    public TimeSpan MinTrainingInterval { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Number of recent dictionary versions kept resolvable after a roll-over,
    /// including the current one. A frame compressed against a version still
    /// within this ring decompresses; older versions are evicted. Must be at
    /// least <c>1</c>. Defaults to <c>4</c>.
    /// </summary>
    public int RetainedVersionCount { get; set; } = 4;

    /// <summary>
    /// The dictionary id assigned to the first trained dictionary; subsequent
    /// versions count up from it. Must not be the reserved id <c>0</c>
    /// ("no dictionary"). Defaults to <c>1</c>.
    /// </summary>
    public uint FirstDictionaryId { get; set; } = 1u;

    /// <summary>
    /// Validates the options, throwing <see cref="ArgumentOutOfRangeException"/>
    /// for any value outside its documented range. Called by the DI registration
    /// helper and by the provider constructor.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">
    /// Any option is outside its documented valid range.
    /// </exception>
    public void Validate()
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxSampleCount, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxReservoirBytes, 1L);
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxSampleBytes, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxReservoirBytes, (long)MaxSampleBytes);
        ArgumentOutOfRangeException.ThrowIfLessThan(DictionaryCapacityBytes, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(MinSamplesToTrain, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(RetainedVersionCount, 1);

        if (double.IsNaN(SamplingRate) || SamplingRate < 0.0 || SamplingRate > 1.0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(SamplingRate), SamplingRate,
                "SamplingRate must be a number in [0, 1].");
        }

        if (MinTrainingInterval < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(MinTrainingInterval), MinTrainingInterval,
                "MinTrainingInterval must not be negative.");
        }

        if (FirstDictionaryId == 0u)
        {
            throw new ArgumentOutOfRangeException(
                nameof(FirstDictionaryId), FirstDictionaryId,
                "FirstDictionaryId must not be the reserved id 0 ('no dictionary').");
        }
    }
}
