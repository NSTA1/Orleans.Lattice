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
/// completed - that admit the caller. Every refusal, an unserved facade, a
/// missing registration and any other fault fail closed. An anonymous caller who
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

    private readonly SchemaFacades _facades;
    private readonly TimeProvider _time;
    private readonly ConcurrentDictionary<string, (SchemaGrants Grants, DateTimeOffset ReadAt, ShellCallerKey Caller)> _trees = new(StringComparer.Ordinal);
    private Tuple<AreaAvailability, ShellCallerKey>? _availability;

    /// <summary>Creates the access answer over the circuit's facades.</summary>
    /// <param name="facades">The area's facades.</param>
    /// <param name="time">The clock grant freshness is measured on.</param>
    public SchemaAccess(SchemaFacades facades, TimeProvider time)
    {
        ArgumentNullException.ThrowIfNull(facades);
        ArgumentNullException.ThrowIfNull(time);
        _facades = facades;
        _time = time;
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
        if (Volatile.Read(ref _availability) is { } remembered && remembered.Item2 == caller)
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
        catch (Exception)
        {
            // Unserved, unreachable, unconfigured or anything else: hidden, and asked again next time.
            return AreaAvailability.Hidden;
        }

        var answer = grants.HasAny
            ? AreaAvailability.Visible
            : _facades.Auth is { IsAuthenticated: false }
                ? AreaAvailability.Unavailable(SignInReason)
                : AreaAvailability.Hidden;
        // Remembered only for the caller it was read for, and only while that is
        // still the caller.
        if (_facades.Caller == caller)
        {
            Volatile.Write(ref _availability, Tuple.Create(answer, caller));
        }

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

        if (_facades.Caller == caller)
        {
            _trees[treeId] = (grants, _time.GetUtcNow(), caller);
        }

        return grants;
    }

    /// <summary>Forgets every remembered answer, so the next read probes again.</summary>
    public void Forget()
    {
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
