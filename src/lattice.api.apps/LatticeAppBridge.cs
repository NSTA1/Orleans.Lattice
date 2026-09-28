using System.Collections.Immutable;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Apps;

/// <summary>
/// The in-process implementation of <see cref="ILatticeAppBridge"/>: the single enforcement seam for everything
/// an app UI does with data. Every transport funnels through it, and the Explorer's frame broker is transport
/// only, so the checks here are the ones that count.
/// </summary>
/// <remarks>
/// <para>
/// <b>Order.</b> After the request is validated syntactically and the caller is resolved and rate-limited, each
/// call is authorized in this order, and every step fails closed:
/// </para>
/// <list type="number">
/// <item><description>
/// The install is resolved from the current registry snapshot for the caller's active tenant. It must be
/// enabled, its ceiling pinned to its version, and its record revision equal to the target's install revision;
/// otherwise the call is <see cref="AppBridgeFailure.Denied"/>, exactly as for an app that does not exist.
/// </description></item>
/// <item><description>
/// The bridge operation must be covered for the logical tree by both the operator-consented bridge grants and
/// the installed manifest's request; otherwise <see cref="AppBridgeFailure.Denied"/>.
/// </description></item>
/// <item><description>
/// The logical tree is resolved server-side: a declared tree to <c>a/{slug}/{tree}</c>, an adopted tree to its
/// adopted id, then composed with the tenant. An undeclared name is <see cref="AppBridgeFailure.NotFound"/>.
/// No path accepts a physical tree id from the caller.
/// </description></item>
/// <item><description>
/// The caller must match an app-owned grant of this install - a role binding's group, holding the concrete
/// operation (<see cref="LatticeOperation.Read"/> for a read, <see cref="LatticeOperation.RangeRead"/> for a scan,
/// <see cref="LatticeOperation.Write"/> for a write, <see cref="LatticeOperation.Delete"/> for a delete - the operation
/// the data path itself enforces) over a scope that covers the concrete key or prefix, with the ceiling re-checked. The caller's
/// other rules are deliberately not consulted: this is what stops a caller's broad operator rights flowing
/// into an app's UI. Otherwise <see cref="AppBridgeFailure.Denied"/>.
/// </description></item>
/// <item><description>
/// The operation executes on the core data path under the caller's own ambient identity, so the ordinary
/// data-plane authorization applies as well, with the caller's resolved tenant as the ambient active tenant.
/// The effective right is steps 1-4 intersected with the caller's own rights.
/// </description></item>
/// </list>
/// <para>
/// <b>Limits and failures.</b> The AppKit frame protocol's key, value, page and response bounds are enforced
/// here too. Every failure is an <see cref="AppBridgeException"/> carrying a fixed, sanitised message; no tree
/// id, subject, rule or exception text escapes.
/// </para>
/// </remarks>
internal sealed class LatticeAppBridge : ILatticeAppBridge
{
    private static readonly ConditionalWeakTable<AppRoleGrantInstall, AppBridgeInstallPlan>.CreateValueCallback BuildPlan =
        static install => AppBridgeInstallPlan.Build(install);

    private readonly AppRoleGrantEvaluator _evaluator;
    private readonly IGrainFactory? _grains;
    private readonly ITenantContextResolver? _tenants;
    private readonly ILatticeMembershipContext? _membership;
    private readonly AppBridgeRateLimiter _limiter;
    private readonly ILogger _logger;
    private readonly ConditionalWeakTable<AppRoleGrantInstall, AppBridgeInstallPlan> _plans = new();

    /// <summary>Initializes a new <see cref="LatticeAppBridge"/>.</summary>
    /// <param name="evaluator">The shared app-role evaluation.</param>
    /// <param name="grains">The grain factory the data path dials, or null (every call then fails closed).</param>
    /// <param name="tenants">The active-tenant resolver, or null (every call then fails closed).</param>
    /// <param name="membership">The membership context resolving the caller, or null (every call then fails closed).</param>
    /// <param name="limiter">The per-caller, per-app rate limiter.</param>
    /// <param name="logger">The logger unexpected faults are reported to, or null.</param>
    /// <exception cref="ArgumentNullException"><paramref name="evaluator"/> or <paramref name="limiter"/> is null.</exception>
    public LatticeAppBridge(
        AppRoleGrantEvaluator evaluator,
        IGrainFactory? grains,
        ITenantContextResolver? tenants,
        ILatticeMembershipContext? membership,
        AppBridgeRateLimiter limiter,
        ILogger<LatticeAppBridge>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(evaluator);
        ArgumentNullException.ThrowIfNull(limiter);
        _evaluator = evaluator;
        _grains = grains;
        _tenants = tenants;
        _membership = membership;
        _limiter = limiter;
        _logger = logger ?? NullLogger<LatticeAppBridge>.Instance;
    }

    /// <inheritdoc />
    public async Task<AppBridgeValue?> GetAsync(AppBridgeTarget target, string key, CancellationToken cancellationToken = default)
    {
        try
        {
            ValidateKey(key);
            var access = await AuthorizeAsync(target, AppUiBridgeOperations.DataRead, LatticeOperation.Read, key, string.Empty, cancellationToken)
                .ConfigureAwait(false);
            byte[]? value;
            using (EnterTenant(access.Tenant))
            {
                value = await access.Tree.GetAsync(key, cancellationToken).ConfigureAwait(false);
            }

            if (value is null)
            {
                return null;
            }

            if (value.Length > AppBridgeLimits.MaxValueBytes)
            {
                throw Fail(AppBridgeFailure.TooLarge);
            }

            return new AppBridgeValue { Key = key, Value = value };
        }
        catch (Exception ex) when (ex is not AppBridgeException && !IsCallerCancellation(ex, cancellationToken))
        {
            throw Translate(ex);
        }
    }

    /// <inheritdoc />
    public async Task<AppBridgePage> ScanAsync(
        AppBridgeTarget target,
        string prefix,
        int pageSize,
        string? continuation = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            if (prefix is null)
            {
                throw Fail(AppBridgeFailure.Invalid);
            }

            if (prefix.Length > AppBridgeLimits.MaxKeyLength)
            {
                throw Fail(AppBridgeFailure.TooLarge);
            }

            if (pageSize < 1)
            {
                throw Fail(AppBridgeFailure.Invalid);
            }

            var size = Math.Min(pageSize, AppBridgeLimits.MaxPageSize);
            var start = prefix;
            if (continuation is not null)
            {
                if (continuation.Length > AppBridgeLimits.MaxContinuationLength)
                {
                    throw Fail(AppBridgeFailure.TooLarge);
                }

                if (!AppBridgeContinuation.TryDecode(continuation, prefix, out start))
                {
                    throw Fail(AppBridgeFailure.Invalid);
                }
            }

            // A scan is a range read on the data path, so the app-owned grant must hold RangeRead, not merely Read.
            var access = await AuthorizeAsync(target, AppUiBridgeOperations.DataRead, LatticeOperation.RangeRead, key: null, prefix, cancellationToken)
                .ConfigureAwait(false);
            using var tenantScope = EnterTenant(access.Tenant);
            var end = LatticeKeyRange.PrefixUpperBound(prefix);
            var entries = ImmutableArray.CreateBuilder<AppBridgeValue>(size);
            var budget = (long)AppBridgeLimits.MaxResponseBytes - AppBridgeLimits.PageOverheadBytes;
            string? next = null;
            await foreach (var entry in access.Tree
                .ScanEntriesAsync(start.Length == 0 ? null : start, end, cancellationToken: cancellationToken)
                .ConfigureAwait(false))
            {
                // The range already bounds the scan to the prefix; a key outside it ends the page rather than
                // being returned.
                if (!entry.Key.StartsWith(prefix, StringComparison.Ordinal))
                {
                    break;
                }

                if (entries.Count == size)
                {
                    next = entry.Key;
                    break;
                }

                var value = entry.Value ?? [];
                if (value.Length > AppBridgeLimits.MaxValueBytes)
                {
                    throw Fail(AppBridgeFailure.TooLarge);
                }

                var cost = AppBridgeLimits.EstimateEntryBytes(entry.Key, value.Length);
                if (cost > budget)
                {
                    next = entry.Key;
                    break;
                }

                budget -= cost;
                entries.Add(new AppBridgeValue { Key = entry.Key, Value = value });
            }

            return new AppBridgePage
            {
                Entries = entries.Count == entries.Capacity ? entries.MoveToImmutable() : entries.ToImmutable(),
                Continuation = next is null ? null : AppBridgeContinuation.Encode(next),
            };
        }
        catch (Exception ex) when (ex is not AppBridgeException && !IsCallerCancellation(ex, cancellationToken))
        {
            throw Translate(ex);
        }
    }

    /// <inheritdoc />
    public async Task SetAsync(
        AppBridgeTarget target,
        string key,
        ReadOnlyMemory<byte> value,
        CancellationToken cancellationToken = default)
    {
        try
        {
            ValidateKey(key);
            if (value.Length > AppBridgeLimits.MaxValueBytes)
            {
                throw Fail(AppBridgeFailure.TooLarge);
            }

            var access = await AuthorizeAsync(target, AppUiBridgeOperations.DataWrite, LatticeOperation.Write, key, string.Empty, cancellationToken)
                .ConfigureAwait(false);

            // The grain surface takes a byte[]; an exactly-sized backing array (what a transport deserializes)
            // is passed as is, anything else is copied once.
            var bytes = MemoryMarshal.TryGetArray(value, out var segment)
                && segment.Array is { } array
                && segment.Offset == 0
                && segment.Count == array.Length
                    ? array
                    : value.ToArray();
            using (EnterTenant(access.Tenant))
            {
                await access.Tree.SetAsync(key, bytes, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (Exception ex) when (ex is not AppBridgeException && !IsCallerCancellation(ex, cancellationToken))
        {
            throw Translate(ex);
        }
    }

    /// <inheritdoc />
    public async Task<bool> DeleteAsync(AppBridgeTarget target, string key, CancellationToken cancellationToken = default)
    {
        try
        {
            ValidateKey(key);
            var access = await AuthorizeAsync(target, AppUiBridgeOperations.DataDelete, LatticeOperation.Delete, key, string.Empty, cancellationToken)
                .ConfigureAwait(false);
            using (EnterTenant(access.Tenant))
            {
                return await access.Tree.DeleteAsync(key, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (Exception ex) when (ex is not AppBridgeException && !IsCallerCancellation(ex, cancellationToken))
        {
            throw Translate(ex);
        }
    }

    /// <summary>
    /// Runs authorization steps 1 to 4 and returns the effective tree, dialled on the data path, with the
    /// caller's resolved tenant. Throws an <see cref="AppBridgeException"/> at the first step that refuses.
    /// </summary>
    private async ValueTask<Access> AuthorizeAsync(
        AppBridgeTarget? target,
        string bridgeOperation,
        LatticeOperation operation,
        string? key,
        string prefix,
        CancellationToken cancellationToken)
    {
        if (target is null
            || !AppSlug.TryParse(target.AppSlug, out var slug)
            || !AppManifestValidator.IsName(target.LogicalTree, AppBridgeLimits.MaxTreeNameLength))
        {
            throw Fail(AppBridgeFailure.Invalid);
        }

        // The caller: every collaborator must be present, the tenant must resolve, and the caller must be a
        // real subject. The anonymous null context and the anonymous subject are refused.
        if (_membership is null or NullLatticeMembershipContext
            || _tenants is null
            || _grains is null
            || !_evaluator.CanServe)
        {
            throw Fail(AppBridgeFailure.Denied);
        }

        TenantId tenant;
        try
        {
            tenant = await AppsFacadeAccess.ResolveTenantAsync(_tenants, cancellationToken).ConfigureAwait(false);
        }
        catch (LatticeTenantAccessDeniedException)
        {
            throw Fail(AppBridgeFailure.Denied);
        }

        var subject = await LatticeAccessGateSubjectResolver.ResolveAsync(_membership, cancellationToken).ConfigureAwait(false);
        if (string.IsNullOrEmpty(subject.SubjectId) || subject.IsAnonymous)
        {
            throw Fail(AppBridgeFailure.Denied);
        }

        if (!_limiter.TryAcquire(subject.SubjectId, tenant, slug))
        {
            throw Fail(AppBridgeFailure.Unavailable);
        }

        // Step 1: the enabled install at exactly the launched revision, in the caller's tenant.
        var snapshot = await _evaluator.GetSnapshotAsync(cancellationToken).ConfigureAwait(false);
        if (!snapshot.TryGet(tenant, slug, out var record)
            || record.Tenant != tenant
            || !AppRoleGrantEvaluator.IsEvaluated(record)
            || record.Revision != target.InstallRevision
            || await _evaluator.GetInstallAsync(record, cancellationToken).ConfigureAwait(false) is not { } install
            || install.Record.Revision != target.InstallRevision
            || install.Record.Tenant != tenant)
        {
            throw Fail(AppBridgeFailure.Denied);
        }

        var plan = _plans.GetValue(install, BuildPlan);

        // Step 2: bridge consent, as consented and as the installed manifest still requests it.
        var grant = new AppUiBridgeGrant(bridgeOperation, target.LogicalTree);
        if (!plan.Consented.Covers(grant) || !plan.Requested.Covers(grant))
        {
            throw Fail(AppBridgeFailure.Denied);
        }

        // Step 3: server-side tree resolution; the caller never names a physical tree.
        if (!plan.TryGetTree(target.LogicalTree, out var tree))
        {
            throw Fail(AppBridgeFailure.NotFound);
        }

        // Step 4: an app-owned grant only; the caller's other rules are not consulted here.
        if (!tree.Allows(subject.GroupIds, operation, key, prefix))
        {
            throw Fail(AppBridgeFailure.Denied);
        }

        // Step 5 is the caller's own call on the data path, gated there under the caller's ambient identity.
        return new Access(_grains.GetGrain<ILattice>(tree.EffectiveTreeId), tenant);
    }

    /// <summary>
    /// Stamps the caller's resolved tenant as the ambient active tenant for the data-path call, unless it is the
    /// default tenant or already stamped. The data plane admits a user-origin call to a tenant-composed
    /// <c>t/{tenant}/...</c> id only when that tenant is the ambient active tenant; the tenant here is the one the
    /// validating resolver returned for this caller, so stamping it widens nothing, and without it a caller whose
    /// tenant was resolved without an explicit assertion would be refused its own app's trees.
    /// </summary>
    private static IDisposable? EnterTenant(TenantId tenant) =>
        tenant.IsDefault || LatticeActiveTenantContext.Current == tenant ? null : LatticeActiveTenantContext.With(tenant);

    private static void ValidateKey(string key)
    {
        if (string.IsNullOrEmpty(key))
        {
            throw Fail(AppBridgeFailure.Invalid);
        }

        if (key.Length > AppBridgeLimits.MaxKeyLength)
        {
            throw Fail(AppBridgeFailure.TooLarge);
        }
    }

    private readonly record struct Access(ILattice Tree, TenantId Tenant);

    private static bool IsCallerCancellation(Exception ex, CancellationToken cancellationToken) =>
        ex is OperationCanceledException && cancellationToken.IsCancellationRequested;

    private static AppBridgeException Fail(AppBridgeFailure failure) => new(failure);

    /// <summary>Maps a fault from the data path to a sanitised failure; the original never escapes.</summary>
    private AppBridgeException Translate(Exception ex)
    {
        switch (ex)
        {
            case LatticeAuthorizationDeniedException or LatticeTenantAccessDeniedException:
                return Fail(AppBridgeFailure.Denied);
            case ArgumentException:
                return Fail(AppBridgeFailure.Invalid);
            default:
                _logger.LogWarning(ex, "Api.Apps: an app bridge request failed on the data path.");
                return Fail(AppBridgeFailure.Unavailable);
        }
    }
}
