using System.Text;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Persists the <c>repocontext_context</c> tool's reuse bookkeeping for one named
/// caller session in its own dedicated tree (<see cref="RepoContextTrees.Session"/>),
/// keyed strictly per <c>(repoId, sessionId)</c> so no session can ever observe
/// another's deliveries. It records only what the server actually delivered - never
/// a wire-supplied claim - and folds each call's additions into the grow-only
/// <see cref="RepoContextSessionRecord"/> CRDT, so calls that share a session id and
/// run concurrently converge on merge under any order.
/// <para>
/// Every write carries a finite time-to-live (<see cref="DefaultSessionTtl"/>), so an
/// abandoned session's bookkeeping lapses on its own and the tree stays bounded. The
/// store adds no storage primitive of its own: it is a thin read-merge-write over the
/// core <see cref="ILattice"/> surface, mirroring
/// <c>RepoContextStore.RememberAsync</c>.
/// </para>
/// </summary>
internal sealed class RepoContextSessionStore
{
    /// <summary>
    /// The lifetime a session's bookkeeping survives without a refreshing write.
    /// Every <see cref="RecordAsync"/> re-stamps the record with this window, so an
    /// active session stays live while an abandoned one lapses.
    /// </summary>
    internal static readonly TimeSpan DefaultSessionTtl = TimeSpan.FromHours(6);

    private readonly IGrainFactory _grainFactory;
    private readonly Serializer _serializer;

    /// <summary>Creates the session reuse-bookkeeping store.</summary>
    /// <param name="grainFactory">The grain factory used to reach the dedicated session tree. Must not be <see langword="null"/>.</param>
    /// <param name="serializer">The Orleans serializer used to decode and re-encode the session record. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException">A required argument is null.</exception>
    public RepoContextSessionStore(IGrainFactory grainFactory, Serializer serializer)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(serializer);
        _grainFactory = grainFactory;
        _serializer = serializer;
    }

    private ILattice Tree() => _grainFactory.GetGrain<ILattice>(RepoContextTrees.Session);

    /// <summary>
    /// Loads the reuse bookkeeping for one session, or <see langword="null"/> when the
    /// session has recorded nothing yet (or its record has lapsed). Fails closed on a
    /// blank <paramref name="repoId"/> or <paramref name="sessionId"/> by returning
    /// <see langword="null"/> rather than reaching for a malformed key.
    /// </summary>
    /// <param name="repoId">The repository identifier the session is scoped to.</param>
    /// <param name="sessionId">The opaque caller session identifier.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The stored record, or <see langword="null"/> when absent.</returns>
    public async Task<RepoContextSessionRecord?> LoadAsync(string repoId, string sessionId, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(repoId) || string.IsNullOrEmpty(sessionId))
        {
            return null;
        }

        var key = RepoContextKeys.Session(repoId, sessionId);
        var bytes = await Tree().GetAsync(key, cancellationToken).ConfigureAwait(false);
        return bytes is null ? null : _serializer.Deserialize<RepoContextSessionRecord>(bytes);
    }

    /// <summary>
    /// Folds this call's genuinely delivered units into the session record and writes
    /// it back under a fresh time-to-live. <paramref name="receipts"/> are the opaque
    /// receipts of every unit delivered; <paramref name="possessions"/> are the
    /// <c>path\0hash</c> tokens of the file versions delivered <b>as a complete
    /// body</b> only - the caller must never pass a partial delivery here, which is
    /// what keeps a possession claim honest. A call with nothing to record is a no-op.
    /// </summary>
    /// <param name="repoId">The repository identifier the session is scoped to.</param>
    /// <param name="sessionId">The opaque caller session identifier.</param>
    /// <param name="receipts">The opaque receipts of the units delivered this call.</param>
    /// <param name="possessions">The whole-file possession tokens delivered this call.</param>
    /// <param name="cancellationToken">Cancels the read-merge-write.</param>
    public async Task RecordAsync(
        string repoId,
        string sessionId,
        IReadOnlyList<string> receipts,
        IReadOnlyList<string> possessions,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(receipts);
        ArgumentNullException.ThrowIfNull(possessions);
        if (string.IsNullOrEmpty(repoId) || string.IsNullOrEmpty(sessionId))
        {
            return;
        }

        if (receipts.Count == 0 && possessions.Count == 0)
        {
            return;
        }

        var key = RepoContextKeys.Session(repoId, sessionId);
        var tree = Tree();
        var existing = await tree.GetAsync(key, cancellationToken).ConfigureAwait(false);
        var record = existing is null
            ? new RepoContextSessionRecord { SessionId = sessionId, RepoId = repoId }
            : _serializer.Deserialize<RepoContextSessionRecord>(existing);

        for (var i = 0; i < receipts.Count; i++)
        {
            record.Receipts.Add(Encoding.UTF8.GetBytes(receipts[i]));
        }

        for (var i = 0; i < possessions.Count; i++)
        {
            record.Possession.Add(Encoding.UTF8.GetBytes(possessions[i]));
        }

        var bytes = _serializer.SerializeToArray(record);
        await tree.SetAsync(key, bytes, DefaultSessionTtl, cancellationToken).ConfigureAwait(false);
    }
}
