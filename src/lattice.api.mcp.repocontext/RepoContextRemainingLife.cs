namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// A read-time projection of a repository-context entry's remaining life, derived
/// from the absolute UTC expiry that Orleans.Lattice core stamps on a TTL'd write
/// (surfaced as <see cref="VersionedValue.ExpiresAtTicks"/>, which mirrors the
/// internal <c>LwwValue&lt;T&gt;.ExpiresAtTicks</c>). It carries both the relative
/// remaining <see cref="Remaining"/> and the absolute <see cref="ExpiresAtUtc"/>
/// so a later read DTO can present "expires in ..." and "expires at ..." without
/// re-deriving them.
/// <para>
/// This is a pure surfacing helper - it introduces no expiry mechanism. Core
/// already converts a TTL to an absolute expiry at write time, hides expired
/// entries on every read, and reaps them through background tombstone
/// compaction; this type only reads that expiry back and projects it.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(RepoContextTypeAliases.RemainingLife)]
[Immutable]
internal readonly record struct RepoContextRemainingLife
{
    /// <summary>
    /// The entry's absolute expiry in UTC <see cref="DateTime.Ticks"/>, or
    /// <c>0</c> when the entry does not expire. Copied verbatim from the entry's
    /// stored <see cref="VersionedValue.ExpiresAtTicks"/>.
    /// </summary>
    [Id(0)]
    public long ExpiresAtTicks { get; init; }

    /// <summary>
    /// The remaining life at the instant this projection was taken, clamped to be
    /// non-negative. <see cref="TimeSpan.Zero"/> both for a non-expiring entry
    /// (see <see cref="Expires"/>) and for one whose expiry has already passed
    /// (see <see cref="HasExpired"/>).
    /// </summary>
    [Id(1)]
    public TimeSpan Remaining { get; init; }

    /// <summary>
    /// <see langword="true"/> when the entry carries an expiry that the
    /// projection instant has reached or passed. Always <see langword="false"/>
    /// for a non-expiring entry.
    /// </summary>
    [Id(2)]
    public bool HasExpired { get; init; }

    /// <summary>
    /// <see langword="true"/> when the entry carries a finite expiry (i.e.
    /// <see cref="ExpiresAtTicks"/> is non-zero), whether or not it has yet
    /// passed; <see langword="false"/> for a durable entry.
    /// </summary>
    public bool Expires => ExpiresAtTicks != 0L;

    /// <summary>
    /// The absolute expiry as a UTC <see cref="DateTime"/>, or
    /// <see langword="null"/> when the entry does not expire.
    /// </summary>
    public DateTime? ExpiresAtUtc =>
        Expires ? new DateTime(ExpiresAtTicks, DateTimeKind.Utc) : null;

    /// <summary>
    /// The projection for an entry that never expires: no expiry tick, zero
    /// remaining, not expired.
    /// </summary>
    public static RepoContextRemainingLife NeverExpires { get; } = new();

    /// <summary>
    /// Projects the remaining life for an entry whose absolute expiry is
    /// <paramref name="expiresAtTicks"/> (UTC ticks; <c>0</c> means it does not
    /// expire), evaluated against <paramref name="nowUtcTicks"/>.
    /// </summary>
    /// <param name="expiresAtTicks">The absolute UTC expiry tick, or <c>0</c> for a non-expiring entry.</param>
    /// <param name="nowUtcTicks">The projection instant, in UTC ticks.</param>
    public static RepoContextRemainingLife FromExpiry(long expiresAtTicks, long nowUtcTicks)
    {
        if (expiresAtTicks == 0L)
        {
            return NeverExpires;
        }

        var remainingTicks = expiresAtTicks - nowUtcTicks;
        if (remainingTicks <= 0L)
        {
            return new RepoContextRemainingLife
            {
                ExpiresAtTicks = expiresAtTicks,
                Remaining = TimeSpan.Zero,
                HasExpired = true,
            };
        }

        return new RepoContextRemainingLife
        {
            ExpiresAtTicks = expiresAtTicks,
            Remaining = TimeSpan.FromTicks(remainingTicks),
            HasExpired = false,
        };
    }

    /// <summary>
    /// Projects the remaining life for an entry whose absolute expiry is
    /// <paramref name="expiresAtTicks"/>, evaluated against
    /// <paramref name="nowUtc"/>. A local-kind instant is converted to UTC; an
    /// unspecified-kind instant is treated as already UTC.
    /// </summary>
    /// <param name="expiresAtTicks">The absolute UTC expiry tick, or <c>0</c> for a non-expiring entry.</param>
    /// <param name="nowUtc">The projection instant, expressed in UTC.</param>
    public static RepoContextRemainingLife FromExpiry(long expiresAtTicks, DateTime nowUtc)
    {
        var nowUtcTicks = nowUtc.Kind == DateTimeKind.Local ? nowUtc.ToUniversalTime().Ticks : nowUtc.Ticks;
        return FromExpiry(expiresAtTicks, nowUtcTicks);
    }

    /// <summary>
    /// Reads the expiry back off a <paramref name="value"/> returned by
    /// <see cref="ILattice.GetWithVersionAsync"/> and projects its remaining life
    /// against <paramref name="nowUtc"/>. A value with no expiry (or an absent /
    /// tombstoned key, whose <see cref="VersionedValue.ExpiresAtTicks"/> is
    /// <c>0</c>) projects to <see cref="NeverExpires"/>.
    /// </summary>
    /// <param name="value">The versioned read result to project. Must not be <see langword="null"/>.</param>
    /// <param name="nowUtc">The projection instant, expressed in UTC.</param>
    /// <exception cref="ArgumentNullException"><paramref name="value"/> is null.</exception>
    public static RepoContextRemainingLife FromVersionedValue(VersionedValue value, DateTime nowUtc)
    {
        ArgumentNullException.ThrowIfNull(value);
        return FromExpiry(value.ExpiresAtTicks, nowUtc);
    }
}
