using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Orleans.Lattice.Explorer.Shell.Navigation;

namespace Orleans.Lattice.Explorer.Shell.Areas.Schema;

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
/// A definite answer (visible, or refused) is remembered until the sign-in or the
/// connection changes. A fault is not remembered, so the next navigation asks
/// again.
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
    private readonly ConcurrentDictionary<string, (SchemaGrants Grants, DateTimeOffset ReadAt)> _trees = new(StringComparer.Ordinal);
    private StrongBox<AreaAvailability>? _availability;

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
        if (Volatile.Read(ref _availability) is { } remembered)
        {
            return remembered.Value;
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
        Volatile.Write(ref _availability, new StrongBox<AreaAvailability>(answer));
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
        if (!refresh && _trees.TryGetValue(treeId, out var remembered) && _time.GetUtcNow() - remembered.ReadAt < GrantFreshness)
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

        _trees[treeId] = (grants, _time.GetUtcNow());
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
