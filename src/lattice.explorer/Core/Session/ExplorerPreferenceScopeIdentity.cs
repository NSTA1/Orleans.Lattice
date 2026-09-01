namespace Orleans.Lattice.Explorer.Core.Session;

/// <summary>
/// The identity a preference is remembered against: the signed-in user and the
/// connected cluster.
/// </summary>
/// <remarks>
/// Both parts are folded into a fixed-width token
/// (<see cref="ToScopeToken"/>) that prefixes the stored key. Folding rather than
/// storing the raw values keeps the key namespace flat regardless of what an
/// endpoint URL or a username contains, and keeps an operator's identity out of
/// the shared preference document at rest.
/// </remarks>
/// <param name="User">
/// The signed-in user, or <see cref="Anonymous"/> when signed out. Never
/// <see langword="null"/> for a value produced by
/// <see cref="IExplorerPreferenceScopeProvider"/>.
/// </param>
/// <param name="Cluster">
/// The connected cluster, conventionally its configured endpoint, or
/// <see cref="Unconfigured"/> before one is applied.
/// </param>
public readonly record struct ExplorerPreferenceScopeIdentity(string User, string Cluster)
{
    /// <summary>The stand-in user for a signed-out session.</summary>
    public const string Anonymous = "anonymous";

    /// <summary>The stand-in cluster for a session with no endpoint applied yet.</summary>
    public const string Unconfigured = "unconfigured";

    /// <summary>The identity of a signed-out, unconfigured session.</summary>
    public static ExplorerPreferenceScopeIdentity Empty { get; } = new(Anonymous, Unconfigured);

    /// <summary>
    /// The fixed-width token this identity contributes to a stored key at
    /// <paramref name="scope"/>. Deterministic across sessions and processes, so
    /// a preference written today is found again tomorrow.
    /// </summary>
    /// <param name="scope">The scope the key is declared at.</param>
    public string ToScopeToken(ExplorerPreferenceScope scope) =>
        scope == ExplorerPreferenceScope.User
            ? string.Create(17, this, static (destination, identity) =>
            {
                destination[0] = 'u';
                WriteHex(destination[1..], Fold(identity.User));
            })
            : string.Create(34, this, static (destination, identity) =>
            {
                destination[0] = 'u';
                WriteHex(destination[1..17], Fold(identity.User));
                destination[17] = 'c';
                WriteHex(destination[18..], Fold(identity.Cluster));
            });

    // FNV-1a over the UTF-16 code units. Not cryptographic and not meant to be:
    // this is a partition token, so all it has to be is stable, fixed width and
    // collision-free in practice across the handful of identities one browser
    // profile ever sees.
    private static ulong Fold(string? value)
    {
        const ulong Offset = 14695981039346656037;
        const ulong Prime = 1099511628211;

        var hash = Offset;
        if (string.IsNullOrEmpty(value))
        {
            return hash;
        }

        for (var i = 0; i < value.Length; i++)
        {
            var c = value[i];
            hash = (hash ^ (byte)c) * Prime;
            hash = (hash ^ (byte)(c >> 8)) * Prime;
        }

        return hash;
    }

    private static void WriteHex(Span<char> destination, ulong value)
    {
        const string Digits = "0123456789abcdef";
        for (var i = 15; i >= 0; i--)
        {
            destination[i] = Digits[(int)(value & 0xF)];
            value >>= 4;
        }
    }
}
