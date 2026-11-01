using System.Collections.Concurrent;
using Orleans.Lattice.Api.Schema;
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
/// completed - that admit the caller. Every refusal, an unserved facade, a
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

    /// <summary>The most catalogue trees probed for a grant when the area-wide probe grants nothing.</summary>
    public const int MaximumTreeProbes = 64;

    /// <summary>How many catalogue trees are probed at once.</summary>
    public const int TreeProbeConcurrency = 8;

    private readonly SchemaFacades _facades;
    private readonly TimeProvider _time;
    private readonly SchemaTreeCatalog? _catalog;
    private readonly ConcurrentDictionary<string, (SchemaGrants Grants, DateTimeOffset ReadAt, ShellCallerKey Caller)> _trees = new(StringComparer.Ordinal);
    private Tuple<AreaAvailability, ShellCallerKey, DateTimeOffset>? _availability;
    private long _generation;

    /// <summary>Creates the access answer over the circuit's facades.</summary>
    /// <param name="facades">The area's facades.</param>
    /// <param name="time">The clock grant freshness is measured on.</param>
    /// <param name="catalog">
    /// The circuit's tree catalogue, whose trees are probed when the area-wide probe
    /// grants nothing; <see langword="null"/> admits only on a cluster-wide grant.
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

        var answer = grants.HasAny
            ? AreaAvailability.Visible
            : _facades.Auth is { IsAuthenticated: false }
                ? AreaAvailability.Unavailable(SignInReason)
                : AreaAvailability.Hidden;
        if (answer == AreaAvailability.Hidden)
        {
            // The sentinel speaks only for a cluster-wide grant: a caller whose grants
            // name trees (a reader of three trees, say) reads those trees' schema, so a
            // tree the catalogue lists that grants anything admits the area.
            switch (await GrantsAnyTreeAsync(cancellationToken).ConfigureAwait(false))
            {
                case null:
                    return AreaAvailability.Unchecked;
                case true:
                    answer = AreaAvailability.Visible;
                    break;
            }
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

    /// <summary>
    /// Whether a tree the catalogue lists (at most <see cref="MaximumTreeProbes"/>,
    /// <see cref="TreeProbeConcurrency"/> at a time) grants the caller any schema
    /// authority. <see langword="null"/> when the cluster did not answer and nothing
    /// admitted the caller; a refusal or any other fault admits nothing.
    /// </summary>
    private async Task<bool?> GrantsAnyTreeAsync(CancellationToken cancellationToken)
    {
        if (_catalog is null || _facades.Schema is not { } schema)
        {
            return false;
        }

        string[] trees;
        try
        {
            var listing = _facades.ListingTenant;
            trees = [.. (await _catalog.GetAsync(refresh: false, cancellationToken).ConfigureAwait(false))
                .Where(tree => ShellAssertedTenant.Lists(listing, tree))
                .Take(MaximumTreeProbes)];
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

        var unanswered = false;
        for (var start = 0; start < trees.Length; start += TreeProbeConcurrency)
        {
            var wave = new Task<bool?>[Math.Min(TreeProbeConcurrency, trees.Length - start)];
            for (var i = 0; i < wave.Length; i++)
            {
                wave[i] = ProbeTreeAsync(schema, trees[start + i], cancellationToken);
            }

            var answers = await Task.WhenAll(wave).ConfigureAwait(false);
            if (answers.Contains(true))
            {
                return true;
            }

            unanswered |= answers.Contains(null);
        }

        return unanswered ? null : false;
    }

    private static async Task<bool?> ProbeTreeAsync(ILatticeSchemaControl schema, string treeId, CancellationToken cancellationToken)
    {
        try
        {
            return SchemaGrants.From(await schema.ProbeCapabilitiesAsync(treeId, cancellationToken).ConfigureAwait(false)).HasAny;
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
}
