using System.Collections.Concurrent;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>
/// The circuit's answer to "may this caller use schema administration, and on
/// this tree what may they do?", read from the facade's capability probe and
/// remembered per circuit.
/// </summary>
/// <remarks>
/// <para>
/// The area-wide answer probes a reserved sentinel tree id, which reads and
/// writes nothing: it is the flags the probe reports - not the fact that it
/// completed - that admit the caller. A grant that names trees rather than the
/// whole cluster admits nothing on the sentinel, so a signed-in caller it refuses
/// is asked about their own trees, up to <see cref="MaximumTreeProbes"/> of them,
/// and admitted only by a schema-administration grant there (#5385): schema read is
/// ordinary tree read, so a reader of a tree is not thereby a schema user. Every
/// refusal, an unserved facade, a
/// missing registration and any other fault fail closed; a transient fault reads
/// as not checked, which admits nothing and is asked again. An anonymous caller who
/// is refused is told to sign in; a signed-in one who is refused sees no area.
/// </para>
/// <para>
/// A definite answer (visible, or refused) is remembered only for the caller it
/// was read for (<see cref="SchemaFacades.Caller"/>: the sign-in, the endpoint and
/// the asserted tenant), and dropped as soon as the sign-in or the connection
/// changes, so a new caller or a tenant switch asks again. A fault is not
/// remembered, so the next navigation asks again.
/// </para>
/// </remarks>
internal sealed class SchemaAccess : IDisposable
{
    /// <summary>The reserved tree id the area-wide probe asks about. The probe has no side effects.</summary>
    public const string ProbeTreeId = "__schema_capability_probe__";

    /// <summary>What an anonymous, refused caller is told.</summary>
    public const string SignInReason = "Sign in to manage schema on this cluster.";

    /// <summary>How long a tree's probed grants are reused.</summary>
    public static readonly TimeSpan GrantFreshness = TimeSpan.FromSeconds(30);

    /// <summary>
    /// The most of the caller's own trees the area-wide answer probes when the sentinel
    /// admits nothing (#5385). A caller whose first <see cref="MaximumTreeProbes"/> trees
    /// grant no schema administration does not see the area, however many trees remain
    /// unasked: like every other unproven answer, the bound fails closed.
    /// </summary>
    public const int MaximumTreeProbes = 8;

    private readonly SchemaFacades _facades;
    private readonly SchemaTreeCatalog? _catalog;
    private readonly TimeProvider _time;
    private readonly ConcurrentDictionary<string, (SchemaGrants Grants, DateTimeOffset ReadAt, ShellCallerKey Caller)> _trees = new(StringComparer.Ordinal);
    private Tuple<AreaAvailability, ShellCallerKey, DateTimeOffset>? _availability;
    private long _generation;

    /// <summary>Creates the access answer over the circuit's facades.</summary>
    /// <param name="facades">The area's facades.</param>
    /// <param name="time">The clock grant freshness is measured on.</param>
    /// <param name="catalog">
    /// The caller's trees, probed when the sentinel admits nothing so a caller whose grants
    /// name trees rather than the whole cluster still sees the area; <see langword="null"/>
    /// to answer from the sentinel alone.
    /// </param>
    public SchemaAccess(SchemaFacades facades, TimeProvider time, SchemaTreeCatalog? catalog = null)
    {
        ArgumentNullException.ThrowIfNull(facades);
        ArgumentNullException.ThrowIfNull(time);
        _facades = facades;
        _time = time;
        _catalog = catalog;
        if (_facades.Auth is { } auth)
        {
            auth.AuthenticationChanged += Forget;
        }

        if (_facades.Session is { } session)
        {
            session.ConfigurationChanged += Forget;
        }
    }

    /// <summary>Whether the caller may see the Schema area.</summary>
    /// <param name="cancellationToken">Cancelled when the directory stops waiting.</param>
    /// <returns>The area's availability.</returns>
    public async ValueTask<AreaAvailability> GetAvailabilityAsync(CancellationToken cancellationToken)
    {
        var caller = _facades.Caller;
        var generation = Volatile.Read(ref _generation);
        if (Volatile.Read(ref _availability) is { } remembered
            && remembered.Item2 == caller
            && _time.GetUtcNow() - remembered.Item3 < GrantFreshness)
        {
            return remembered.Item1;
        }

        if (_facades.Schema is not { } schema)
        {
            return AreaAvailability.Hidden;
        }

        SchemaGrants grants;
        try
        {
            grants = SchemaGrants.From(await schema.ProbeCapabilitiesAsync(ProbeTreeId, cancellationToken).ConfigureAwait(false));
        }
        catch (LatticeAuthorizationDeniedException)
        {
            grants = SchemaGrants.None;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception) when (AreaProbeFaults.IsTransient(exception))
        {
            // The cluster did not answer: not checked, and not remembered.
            return AreaAvailability.Unchecked;
        }
        catch (Exception)
        {
            // Unserved, unconfigured or anything else: hidden, and asked again next time.
            return AreaAvailability.Hidden;
        }

        AreaAvailability answer;
        if (grants.HasAny)
        {
            answer = AreaAvailability.Visible;
        }
        else if (_facades.Auth is { IsAuthenticated: false })
        {
            answer = AreaAvailability.Unavailable(SignInReason);
        }
        else
        {
            // The sentinel answers for a grant over every tree. A grant that names trees
            // (a schema owner of a few trees, #5385) admits nothing there, so ask the
            // caller's own trees.
            var trees = await ProbeOwnTreesAsync(cancellationToken).ConfigureAwait(false);
            if (trees is null)
            {
                return AreaAvailability.Unchecked;
            }

            answer = trees.Value ? AreaAvailability.Visible : AreaAvailability.Hidden;
        }

        // Remembered only for the caller it was read for, and only while that is
        // still the caller.
        if (_facades.Caller != caller || Volatile.Read(ref _generation) != generation)
        {
            return AreaAvailability.Hidden;
        }

        Volatile.Write(ref _availability, Tuple.Create(answer, caller, _time.GetUtcNow()));
        return answer;
    }

    /// <summary>What the caller may do with <paramref name="treeId"/>'s schema.</summary>
    /// <param name="treeId">The logical tree id.</param>
    /// <param name="refresh">Probe again even when the remembered answer is fresh.</param>
    /// <param name="cancellationToken">Cancels the probe.</param>
    /// <returns>The grants; <see cref="SchemaGrants.None"/> when the probe failed or was refused.</returns>
    public async Task<SchemaGrants> GetGrantsAsync(string treeId, bool refresh, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        var caller = _facades.Caller;
        var generation = Volatile.Read(ref _generation);
        if (!refresh
            && _trees.TryGetValue(treeId, out var remembered)
            && remembered.Caller == caller
            && _time.GetUtcNow() - remembered.ReadAt < GrantFreshness)
        {
            return remembered.Grants;
        }

        if (_facades.Schema is not { } schema)
        {
            return SchemaGrants.None;
        }

        SchemaGrants grants;
        try
        {
            grants = SchemaGrants.From(await schema.ProbeCapabilitiesAsync(treeId, cancellationToken).ConfigureAwait(false));
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception)
        {
            return SchemaGrants.None;
        }

        if (_facades.Caller != caller || Volatile.Read(ref _generation) != generation)
        {
            return SchemaGrants.None;
        }

        _trees[treeId] = (grants, _time.GetUtcNow(), caller);
        return grants;
    }

    /// <summary>Forgets every remembered answer, so the next read probes again.</summary>
    public void Forget()
    {
        Interlocked.Increment(ref _generation);
        Volatile.Write(ref _availability, null);
        _trees.Clear();
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_facades.Auth is { } auth)
        {
            auth.AuthenticationChanged -= Forget;
        }

        if (_facades.Session is { } session)
        {
            session.ConfigurationChanged -= Forget;
        }
    }

    /// <summary>
    /// Whether any of the caller's own trees grants schema administration: the first
    /// <see cref="MaximumTreeProbes"/> trees the caller lists, in order, stopping at the
    /// first that grants. Schema read is ordinary tree read, so a read grant alone does
    /// not admit the caller; and when the bound is reached with trees still unasked, the
    /// answer is no, failing closed as every other unproven answer does.
    /// </summary>
    /// <param name="cancellationToken">Cancels the probes.</param>
    /// <returns>
    /// <see langword="true"/> to show the area, <see langword="false"/> to hide it, and
    /// <see langword="null"/> when the cluster did not answer.
    /// </returns>
    private async Task<bool?> ProbeOwnTreesAsync(CancellationToken cancellationToken)
    {
        if (_catalog is null)
        {
            return false;
        }

        IReadOnlyList<string> trees;
        try
        {
            trees = await _catalog.GetAsync(refresh: false, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception) when (AreaProbeFaults.IsTransient(exception))
        {
            return null;
        }
        catch (Exception)
        {
            return false;
        }

        var listing = _facades.ListingTenant;
        var probed = 0;
        foreach (var tree in trees)
        {
            if (!ShellAssertedTenant.Lists(listing, tree))
            {
                continue;
            }

            if (probed == MaximumTreeProbes)
            {
                return false;
            }

            probed++;
            if ((await GetGrantsAsync(tree, refresh: false, cancellationToken).ConfigureAwait(false)).CanChangeAnything)
            {
                return true;
            }
        }

        return false;
    }
}
