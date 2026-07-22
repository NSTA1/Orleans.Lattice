using System.Globalization;
using Microsoft.Extensions.Caching.Distributed;

namespace Orleans.Lattice.Caching.AzureBlob;

/// <summary>
/// Pure expiration arithmetic for a blob-backed cache entry: it turns
/// <see cref="DistributedCacheEntryOptions"/> into the absolute cap, sliding
/// window, and current effective-expiry instant stored in blob metadata, and
/// computes the forward slide applied when a sliding entry is read. Kept free of
/// any Azure dependency so the whole expiry contract is unit-testable with a
/// deterministic clock.
/// </summary>
internal static class BlobCacheEntryExpiration
{
    /// <summary>Blob metadata key for the absolute-expiration cap (UTC ticks).</summary>
    public const string AbsoluteExpirationMetadataKey = "absexp";

    /// <summary>Blob metadata key for the sliding-expiration window (ticks).</summary>
    public const string SlidingExpirationMetadataKey = "sldexp";

    /// <summary>Blob metadata key for the current effective-expiration instant (UTC ticks).</summary>
    public const string EffectiveExpirationMetadataKey = "expiry";

    /// <summary>
    /// The absolute cap, sliding window, and current effective-expiry of an
    /// entry. A <see langword="null"/> <see cref="Effective"/> means the entry
    /// never expires on its own (it lives until removed or overwritten).
    /// </summary>
    /// <param name="Absolute">The hard expiry cap, or <see langword="null"/> for none.</param>
    /// <param name="Sliding">The sliding window, or <see langword="null"/> for none.</param>
    /// <param name="Effective">The instant the entry currently expires, or <see langword="null"/> for never.</param>
    public readonly record struct Values(
        DateTimeOffset? Absolute,
        TimeSpan? Sliding,
        DateTimeOffset? Effective);

    /// <summary>
    /// Computes the stored expiration for a new entry written at
    /// <paramref name="now"/> with <paramref name="options"/>.
    /// </summary>
    /// <param name="options">The caller's entry options.</param>
    /// <param name="now">The write instant from the cache's clock.</param>
    /// <returns>The absolute cap, sliding window, and initial effective expiry.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// The absolute expiration is already in the past, or the sliding expiration is not positive.
    /// </exception>
    public static Values Compute(DistributedCacheEntryOptions options, DateTimeOffset now)
    {
        ArgumentNullException.ThrowIfNull(options);

        DateTimeOffset? absolute;
        if (options.AbsoluteExpirationRelativeToNow.HasValue)
        {
            absolute = now + options.AbsoluteExpirationRelativeToNow.Value;
        }
        else if (options.AbsoluteExpiration.HasValue)
        {
            if (options.AbsoluteExpiration.Value <= now)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(options),
                    options.AbsoluteExpiration.Value,
                    "The absolute expiration value must be in the future.");
            }

            absolute = options.AbsoluteExpiration;
        }
        else
        {
            absolute = null;
        }

        var sliding = options.SlidingExpiration;
        if (sliding.HasValue && sliding.Value <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                sliding.Value,
                "The sliding expiration value must be positive.");
        }

        DateTimeOffset? effective;
        if (sliding.HasValue)
        {
            var candidate = now + sliding.Value;
            effective = absolute.HasValue && candidate > absolute.Value ? absolute.Value : candidate;
        }
        else
        {
            effective = absolute;
        }

        return new Values(absolute, sliding, effective);
    }

    /// <summary>
    /// Serializes <paramref name="values"/> into the blob-metadata dictionary
    /// stored alongside the entry content. Only the populated components are
    /// written, so an entry with no expiry carries no expiry metadata.
    /// </summary>
    /// <param name="values">The expiration to serialize.</param>
    /// <returns>The metadata dictionary (never <see langword="null"/>).</returns>
    public static IDictionary<string, string> ToMetadata(Values values)
    {
        var metadata = new Dictionary<string, string>(capacity: 3, StringComparer.Ordinal);
        if (values.Absolute.HasValue)
        {
            metadata[AbsoluteExpirationMetadataKey] = FormatTicks(values.Absolute.Value.UtcTicks);
        }

        if (values.Sliding.HasValue)
        {
            metadata[SlidingExpirationMetadataKey] = FormatTicks(values.Sliding.Value.Ticks);
        }

        if (values.Effective.HasValue)
        {
            metadata[EffectiveExpirationMetadataKey] = FormatTicks(values.Effective.Value.UtcTicks);
        }

        return metadata;
    }

    /// <summary>
    /// Parses the expiration components from an entry's blob metadata. A missing
    /// or unparsable component reads back as <see langword="null"/>, so a
    /// hand-edited or partially written blob degrades to "never expires" rather
    /// than throwing.
    /// </summary>
    /// <param name="metadata">The blob metadata, or <see langword="null"/>.</param>
    /// <returns>The parsed expiration.</returns>
    public static Values FromMetadata(IDictionary<string, string>? metadata)
    {
        if (metadata is null)
        {
            return default;
        }

        var absolute = ReadUtcTicks(metadata, AbsoluteExpirationMetadataKey);
        var effective = ReadUtcTicks(metadata, EffectiveExpirationMetadataKey);
        TimeSpan? sliding = metadata.TryGetValue(SlidingExpirationMetadataKey, out var raw)
            && long.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var ticks)
            && ticks > 0
                ? TimeSpan.FromTicks(ticks)
                : null;

        return new Values(absolute, sliding, effective);
    }

    /// <summary>
    /// Determines whether <paramref name="values"/> has expired as of
    /// <paramref name="now"/>.
    /// </summary>
    /// <param name="values">The entry's expiration.</param>
    /// <param name="now">The current instant.</param>
    /// <returns><see langword="true"/> when the entry is expired.</returns>
    public static bool IsExpired(Values values, DateTimeOffset now)
        => values.Effective.HasValue && now >= values.Effective.Value;

    /// <summary>
    /// Computes the forward-slid effective expiry for a sliding entry read at
    /// <paramref name="now"/>, or <see langword="null"/> when nothing should be
    /// rewritten (the entry is not sliding, or the slide would not advance the
    /// stored instant). The slide is capped at the absolute expiration.
    /// </summary>
    /// <param name="values">The entry's current expiration.</param>
    /// <param name="now">The read instant.</param>
    /// <returns>The new effective expiry to persist, or <see langword="null"/>.</returns>
    public static DateTimeOffset? Slide(Values values, DateTimeOffset now)
    {
        if (!values.Sliding.HasValue)
        {
            return null;
        }

        var candidate = now + values.Sliding.Value;
        if (values.Absolute.HasValue && candidate > values.Absolute.Value)
        {
            candidate = values.Absolute.Value;
        }

        // Only rewrite when the window actually advances; a read that lands on
        // the same tick (or a capped entry already at its absolute cap) must not
        // churn blob metadata.
        return values.Effective.HasValue && candidate <= values.Effective.Value ? null : candidate;
    }

    private static string FormatTicks(long ticks) => ticks.ToString(CultureInfo.InvariantCulture);

    private static DateTimeOffset? ReadUtcTicks(IDictionary<string, string> metadata, string key)
        => metadata.TryGetValue(key, out var raw)
            && long.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var ticks)
            && ticks >= 0
            && ticks <= DateTimeOffset.MaxValue.UtcTicks
                ? new DateTimeOffset(ticks, TimeSpan.Zero)
                : null;
}
