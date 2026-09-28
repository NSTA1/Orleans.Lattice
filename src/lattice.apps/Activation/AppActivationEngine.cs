using Microsoft.Extensions.Logging;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Apps;

/// <summary>
/// The activation pipeline's logic: resolve, validate, compile, provision, persist, transition,
/// record. Callers serialize runs per tenant app (see <see cref="AppActivationGrain"/>); every
/// run is total - each failure becomes a failed <see cref="AppActivationOutcome"/> recorded
/// against the app - and runs system-origin because the platform, not the caller, performs
/// the side effects once the caller has been authorized.
/// </summary>
internal sealed class AppActivationEngine
{
    internal const string MembershipChainMessage =
        "App activation requires the App to Auth to Membership chain, and membership is not registered: " +
        "without AddLatticeMembership(...) every caller resolves to the anonymous subject with no groups, " +
        "so every app rule would be unmatchable. Register membership and authorization, then enable the app again.";

    internal const string AuthorizationMissingMessage =
        "App activation requires the App to Auth to Membership chain, and the authorization policy store is not " +
        "registered: without AddLatticeAuth(...) the app's role rules cannot be persisted. Register membership and " +
        "authorization, then enable the app again.";

    private readonly IAppRegistry _registry;
    private readonly IAppSource _source;
    private readonly IAppActivationStatusStore _statusStore;
    private readonly IAppTreeProvisioner _trees;
    private readonly ILogger<AppActivationEngine> _logger;
    private readonly ILatticeAuthorizationPolicyStore? _policyStore;
    private readonly ILatticeMembershipContext? _membership;
    private readonly TimeProvider _time;
    private readonly AppReplicationEnrolment _replication;

    public AppActivationEngine(
        IAppRegistry registry,
        IAppSource source,
        IAppActivationStatusStore statusStore,
        IAppTreeProvisioner trees,
        ILogger<AppActivationEngine> logger,
        ILatticeAuthorizationPolicyStore? policyStore = null,
        ILatticeMembershipContext? membership = null,
        TimeProvider? timeProvider = null,
        ILatticeReplicationConfigAuthority? replication = null)
    {
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(statusStore);
        ArgumentNullException.ThrowIfNull(trees);
        ArgumentNullException.ThrowIfNull(logger);
        _registry = registry;
        _source = source;
        _statusStore = statusStore;
        _trees = trees;
        _logger = logger;
        _policyStore = policyStore;
        _membership = membership;
        _time = timeProvider ?? TimeProvider.System;
        _replication = new AppReplicationEnrolment(replication);
    }

    internal bool IsMembershipRegistered => _membership is not null and not NullLatticeMembershipContext;

    public async Task<AppActivationOutcome> ExecuteAsync(
        AppActivationOperation operation,
        TenantId tenant,
        AppSlug slug,
        CancellationToken cancellationToken)
    {
        AppRegistryTreeNames.RequireTenant(tenant);
        AppRegistryTreeNames.RequireSlug(slug);
        if (!Enum.IsDefined(operation))
        {
            throw new ArgumentOutOfRangeException(nameof(operation), operation, "Unknown app activation operation.");
        }

        using (LatticeSystemOrigin.Enter())
        {
            AppActivationStatus? status = null;
            Run? run = null;
            var statusRead = false;
            Step result;
            try
            {
                status = await _statusStore.GetAsync(tenant, slug, cancellationToken).ConfigureAwait(false);
                statusRead = true;
                run = new Run(this, operation, tenant, slug, status?.AppliedManifest,
                    status?.ReplicationTrees ?? Array.Empty<string>());
                result = await run.ExecuteAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "App activation {Operation} of {Tenant}/{Slug} faulted.", operation, tenant.Value, slug.Value);
                result = Step.Fail(null, AppActivationFailure.Faulted, "faulted", "$", Describe(ex), status?.AppliedManifest);
            }

            var outcome = new AppActivationOutcome
            {
                Tenant = tenant,
                Slug = slug,
                Operation = operation,
                Failure = result.Failure,
                Version = result.Record?.Version,
                State = result.Record?.State,
                Changed = result.Changed,
                Diagnostics = result.Diagnostics,
                CompletedAtUtc = _time.GetUtcNow(),
            };

            if (!outcome.Succeeded)
            {
                _logger.LogWarning(
                    "App activation {Operation} of {Tenant}/{Slug} failed with {Failure}: {Diagnostic}",
                    operation, tenant.Value, slug.Value, outcome.Failure, outcome.Diagnostics.Count > 0 ? outcome.Diagnostics[0].Message : null);
            }

            // An unreadable status is never overwritten: its applied manifest is the only record of
            // which trees were provisioned, and replacing it blind would forget them.
            if (statusRead)
            {
                await RecordAsync(new AppActivationStatus
                {
                    Tenant = tenant,
                    Slug = slug,
                    LastOutcome = outcome,
                    AppliedManifest = result.Applied,
                    ReplicationTrees = run?.ReplicationTrees ?? status?.ReplicationTrees ?? Array.Empty<string>(),
                }, cancellationToken).ConfigureAwait(false);
            }

            return outcome;
        }
    }

    private async Task RecordAsync(AppActivationStatus status, CancellationToken cancellationToken)
    {
        try
        {
            await _statusStore.SetAsync(status, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            _logger.LogWarning(ex, "Failed to record the activation status of {Tenant}/{Slug}.", status.Tenant.Value, status.Slug.Value);
        }
    }

    private static string Describe(Exception ex) => string.Concat(ex.GetType().Name, ": ", ex.Message);

    private static AppActivationFailure MapRegistryError(AppRegistryTransitionError error) => error switch
    {
        AppRegistryTransitionError.NotInstalled => AppActivationFailure.NotInstalled,
        AppRegistryTransitionError.CeilingNotPinned => AppActivationFailure.CeilingNotPinned,
        AppRegistryTransitionError.ConcurrencyConflict => AppActivationFailure.RegistryConflict,
        _ => AppActivationFailure.InvalidTransition,
    };

    private static AppActivationFailure MapSourceStatus(AppSourceStatus status) => status switch
    {
        AppSourceStatus.VersionMismatch => AppActivationFailure.VersionMismatch,
        AppSourceStatus.InvalidManifest => AppActivationFailure.InvalidManifest,
        _ => AppActivationFailure.SourceUnavailable,
    };

    private static IReadOnlyList<AppManifestError> DescribeCompilation(AppRuleCompilation compilation)
    {
        var errors = new List<AppManifestError>(compilation.Excesses.Count + compilation.UnknownRoleBindings.Count);
        foreach (var excess in compilation.Excesses)
        {
            errors.Add(excess.Kind == AppCeilingExcessKind.Operations
                ? new AppManifestError(
                    "ceiling-operations",
                    $"$.roles[{excess.RoleName}].operations",
                    $"Role '{excess.RoleName}' requests operations '{excess.Operations}' beyond the consented capability ceiling.")
                : new AppManifestError(
                    "ceiling-scope",
                    $"$.roles[{excess.RoleName}].scopes",
                    $"Role '{excess.RoleName}' requests scope {excess.Scope?.Kind} '{excess.Scope?.TreeId}' that the consented capability ceiling does not approve."));
        }

        foreach (var binding in compilation.UnknownRoleBindings)
        {
            errors.Add(new AppManifestError(
                "unknown-role-binding",
                "$.roles",
                $"The installed binding of role '{binding.RoleName}' names a role this manifest version does not declare."));
        }

        return errors;
    }

    /// <summary>The terminal result of a run, before it is stamped into an outcome.</summary>
    private readonly record struct Step(
        AppRegistryRecord? Record,
        AppActivationFailure Failure,
        bool Changed,
        IReadOnlyList<AppManifestError> Diagnostics,
        AppManifest? Applied)
    {
        public static Step Ok(AppRegistryRecord? record, bool changed, AppManifest? applied) =>
            new(record, AppActivationFailure.None, changed, Array.Empty<AppManifestError>(), applied);

        public static Step Fail(
            AppRegistryRecord? record,
            AppActivationFailure failure,
            string code,
            string path,
            string message,
            AppManifest? applied) =>
            new(record, failure, false, new[] { new AppManifestError(code, path, message) }, applied);

        public static Step Fail(
            AppRegistryRecord? record,
            AppActivationFailure failure,
            IReadOnlyList<AppManifestError> diagnostics,
            AppManifest? applied) =>
            new(record, failure, false, diagnostics, applied);
    }

    /// <summary>One run's state and steps.</summary>
    private sealed class Run(
        AppActivationEngine engine,
        AppActivationOperation operation,
        TenantId tenant,
        AppSlug slug,
        AppManifest? applied,
        IReadOnlyList<string> replicationTrees)
    {
        public IReadOnlyList<string> ReplicationTrees { get; private set; } = replicationTrees;

        private async Task<(AppActivationFailure Failure, AppManifestError? Diagnostic)> ApplyReplicationAsync(
            AppManifest? manifest, AppManifest? previous, CancellationToken cancellationToken)
        {
            var result = await engine._replication.ApplyAsync(tenant, manifest, previous, ReplicationTrees,
                async pending =>
                {
                    ReplicationTrees = pending;
                    await engine._statusStore.SetAsync(new AppActivationStatus
                    {
                        Tenant = tenant,
                        Slug = slug,
                        AppliedManifest = applied,
                        ReplicationTrees = pending,
                        LastOutcome = new AppActivationOutcome
                        {
                            Tenant = tenant,
                            Slug = slug,
                            Operation = operation,
                            Failure = AppActivationFailure.ReplicationEnrolmentFailed,
                            Diagnostics = new[] { new AppManifestError("replication-pending", "$.replication",
                                "Replication intent was recorded; activation has not completed.") },
                            CompletedAtUtc = engine._time.GetUtcNow(),
                        },
                    }, cancellationToken).ConfigureAwait(false);
                }, cancellationToken).ConfigureAwait(false);
            if (result.Trees is { } trees)
            {
                ReplicationTrees = trees;
            }

            return (result.Failure, result.Diagnostic);
        }

        public async Task<Step> ExecuteAsync(CancellationToken cancellationToken)
        {
            var needsMembership = operation is AppActivationOperation.Enable or AppActivationOperation.Reconcile;
            if (needsMembership && !engine.IsMembershipRegistered)
            {
                return Step.Fail(null, AppActivationFailure.MembershipNotRegistered, "membership-not-registered", "$", MembershipChainMessage, applied);
            }

            if (engine._policyStore is null)
            {
                return Step.Fail(null, AppActivationFailure.AuthorizationNotRegistered, "auth-not-registered", "$", AuthorizationMissingMessage, applied);
            }

            var record = await engine._registry.GetAsync(tenant, slug, cancellationToken).ConfigureAwait(false);
            return operation switch
            {
                AppActivationOperation.Enable => await EnableAsync(record, cancellationToken).ConfigureAwait(false),
                AppActivationOperation.Disable => await DisableAsync(record, cancellationToken).ConfigureAwait(false),
                AppActivationOperation.Uninstall => await UninstallAsync(record, cancellationToken).ConfigureAwait(false),
                _ => await ReconcileAsync(record, cancellationToken).ConfigureAwait(false),
            };
        }

        private async Task<Step> EnableAsync(AppRegistryRecord? record, CancellationToken cancellationToken)
        {
            var decision = AppLifecycle.Evaluate(record, AppLifecycleAction.Enable);
            if (decision.Kind == AppLifecycleDecisionKind.Reject)
            {
                return Step.Fail(record, MapRegistryError(decision.Error), "registry", "$", decision.Message!, applied);
            }

            var activated = await ActivateAsync(record!, cancellationToken).ConfigureAwait(false);
            if (activated.Failure != AppActivationFailure.None)
            {
                return await FailClosedAsync(activated, cancellationToken).ConfigureAwait(false);
            }

            AppRegistryTransitionResult transition;
            try
            {
                transition = await engine._registry.EnableAsync(tenant, slug, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
            {
                // Whether the transition landed is unknown; fail closed by withdrawing the rules.
                // The next reconcile re-activates an app whose record did become enabled.
                await WithdrawRulesAsync(cancellationToken).ConfigureAwait(false);
                throw;
            }

            if (!transition.Succeeded)
            {
                // Do not leave rules live behind a record that is not enabled. The trees stay
                // provisioned, so the activated manifest remains the applied one.
                await WithdrawRulesAsync(cancellationToken).ConfigureAwait(false);
                return Step.Fail(
                    transition.Record ?? record,
                    MapRegistryError(transition.Error),
                    "registry",
                    "$",
                    transition.Message ?? "The registry rejected the transition.",
                    activated.Applied);
            }

            // The rules were compiled from the record this run read. A consent write (a
            // narrowed ceiling, rebound roles, or an upgrade) that landed between that read and
            // the transition leaves them describing superseded consent, and because the record
            // was not yet enabled, the writer did not re-apply it. The transition's record is
            // authoritative from here on, so re-activate against it; a later consent write sees
            // the app enabled and queues its own reconcile behind this run.
            var enabled = transition.Record;
            var expectedRevision = record!.Revision + (transition.Changed ? 1 : 0);
            if (enabled is not null && enabled.Revision != expectedRevision)
            {
                applied = activated.Applied;
                var reactivated = await ActivateAsync(enabled, cancellationToken).ConfigureAwait(false);
                if (reactivated.Failure != AppActivationFailure.None)
                {
                    return await FailClosedAsync(reactivated, cancellationToken).ConfigureAwait(false);
                }

                return Step.Ok(enabled, transition.Changed, reactivated.Applied);
            }

            return Step.Ok(transition.Record, transition.Changed, activated.Applied);
        }

        private async Task<Step> ReconcileAsync(AppRegistryRecord? record, CancellationToken cancellationToken)
        {
            if (record is null)
            {
                return Step.Fail(null, AppActivationFailure.NotInstalled, "registry", "$", "The app is not installed for this tenant.", applied);
            }

            if (record.State == AppRegistryLifecycleState.Enabled)
            {
                if (!record.IsCeilingPinnedToVersion)
                {
                    return Step.Fail(record, AppActivationFailure.CeilingNotPinned, "registry", "$",
                        "The stored capability ceiling was not consented for the stored version; upgrade the app to re-consent.", applied);
                }

                var activated = await ActivateAsync(record, cancellationToken).ConfigureAwait(false);
                return activated.Failure != AppActivationFailure.None
                    ? await FailClosedAsync(activated, cancellationToken).ConfigureAwait(false)
                    : Step.Ok(record, false, activated.Applied);
            }

            var failure = await WithdrawRulesAsync(cancellationToken).ConfigureAwait(false);
            return failure is { } f ? f with { Record = record } : Step.Ok(record, false, applied);
        }

        private async Task<Step> DisableAsync(AppRegistryRecord? record, CancellationToken cancellationToken)
        {
            var decision = AppLifecycle.Evaluate(record, AppLifecycleAction.Disable);
            if (decision.Kind == AppLifecycleDecisionKind.Reject)
            {
                return Step.Fail(record, MapRegistryError(decision.Error), "registry", "$", decision.Message!, applied);
            }

            if (await WithdrawRulesAsync(cancellationToken).ConfigureAwait(false) is { } failure)
            {
                return failure with { Record = record };
            }

            var transition = await engine._registry.DisableAsync(tenant, slug, cancellationToken).ConfigureAwait(false);
            return transition.Succeeded
                ? Step.Ok(transition.Record, transition.Changed, applied)
                : Step.Fail(transition.Record ?? record, MapRegistryError(transition.Error), "registry", "$",
                    transition.Message ?? "The registry rejected the transition.", applied);
        }

        private async Task<Step> UninstallAsync(AppRegistryRecord? record, CancellationToken cancellationToken)
        {
            var decision = AppLifecycle.Evaluate(record, AppLifecycleAction.Uninstall);
            if (decision.Kind == AppLifecycleDecisionKind.Reject)
            {
                return Step.Fail(record, MapRegistryError(decision.Error), "registry", "$", decision.Message!, applied);
            }

            if (await WithdrawRulesAsync(cancellationToken).ConfigureAwait(false) is { } failure)
            {
                return failure with { Record = record };
            }

            // The trees to retire are the ones actually provisioned; when nothing was recorded,
            // fall back to what the installed version declares.
            var manifest = applied;
            if (manifest is null && record!.State != AppRegistryLifecycleState.Uninstalled)
            {
                var resolved = await engine._source.ResolveAsync(slug, record.Version, cancellationToken).ConfigureAwait(false);
                manifest = resolved.Manifest;
            }

            var replication = await ApplyReplicationAsync(null, manifest, cancellationToken).ConfigureAwait(false);
            if (replication.Diagnostic is { } diagnostic)
            {
                return Step.Fail(record, replication.Failure, new[] { diagnostic }, applied);
            }

            if (manifest is not null)
            {
                foreach (var tree in manifest.Trees)
                {
                    if (tree.AdoptedTreeId is not null)
                    {
                        continue;
                    }

                    var treeId = AppActivationTreeNames.StructuralTree(tenant, slug, tree.Name);
                    try
                    {
                        await engine._trees.SoftDeleteAsync(treeId, cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
                    {
                        return Step.Fail(record, AppActivationFailure.TreeProvisioningFailed, "tree-delete",
                            $"$.trees[{tree.Name}]", $"Soft-deleting tree '{treeId}' failed: {Describe(ex)}", applied);
                    }
                }
            }

            if (decision.Kind == AppLifecycleDecisionKind.NoOp)
            {
                return Step.Ok(record, false, null);
            }

            var transition = await engine._registry.UninstallAsync(tenant, slug, cancellationToken).ConfigureAwait(false);
            return transition.Succeeded
                ? Step.Ok(transition.Record, transition.Changed, null)
                : Step.Fail(transition.Record ?? record, MapRegistryError(transition.Error), "registry", "$",
                    transition.Message ?? "The registry rejected the transition.", applied);
        }

        /// <summary>
        /// Resolves, validates, and compiles the installed version, then provisions its trees and
        /// replaces the app's owned rule set. Returns a step whose <see cref="Step.Applied"/> is the
        /// activated manifest on success.
        /// </summary>
        private async Task<Step> ActivateAsync(AppRegistryRecord record, CancellationToken cancellationToken)
        {
            var resolved = await engine._source.ResolveAsync(slug, record.Version, cancellationToken).ConfigureAwait(false);
            if (!resolved.IsResolved || resolved.Manifest is not { } manifest)
            {
                return Step.Fail(record, MapSourceStatus(resolved.Status), resolved.Errors, applied);
            }

            if (manifest.Identity.Slug != slug || manifest.Identity.Version != record.Version)
            {
                return Step.Fail(record, AppActivationFailure.VersionMismatch, "version-mismatch", "$.identity",
                    $"The source supplied '{manifest.Identity.Slug}' version '{manifest.Identity.Version}', not the installed '{slug}' version '{record.Version}'.",
                    applied);
            }

            var previous = applied is not null && applied.Identity.Slug == slug ? applied : null;
            var validation = AppManifestValidator.Validate(manifest, previous);
            if (!validation.IsValid)
            {
                return Step.Fail(record, AppActivationFailure.InvalidManifest, validation.Errors, applied);
            }

            var compilation = AppRoleCompiler.Compile(manifest, tenant, record.RoleBindings, record.Ceiling);
            if (!compilation.Succeeded)
            {
                var failure = compilation.Excesses.Count > 0
                    ? AppActivationFailure.CeilingExceeded
                    : AppActivationFailure.UnknownRoleBinding;
                return Step.Fail(record, failure, DescribeCompilation(compilation), applied);
            }

            var replication = await ApplyReplicationAsync(manifest, previous, cancellationToken).ConfigureAwait(false);
            if (replication.Diagnostic is { } diagnostic)
            {
                return Step.Fail(record, replication.Failure, new[] { diagnostic }, applied);
            }

            foreach (var tree in manifest.Trees)
            {
                if (tree.AdoptedTreeId is not null)
                {
                    continue;
                }

                var treeId = AppActivationTreeNames.StructuralTree(tenant, slug, tree.Name);
                try
                {
                    await engine._trees.EnsureAsync(treeId, tree, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
                {
                    return Step.Fail(record, AppActivationFailure.TreeProvisioningFailed, "tree-create",
                        $"$.trees[{tree.Name}]", $"Provisioning tree '{treeId}' failed: {Describe(ex)}", applied);
                }
            }

            if (await ReplaceRulesAsync(compilation.Rules, cancellationToken).ConfigureAwait(false) is { } ruleFailure)
            {
                return ruleFailure with { Record = record };
            }

            // Trees the new version no longer declares structurally are retired only once the
            // rules granting them are gone.
            if (previous is not null)
            {
                foreach (var dropped in previous.Trees)
                {
                    if (dropped.AdoptedTreeId is not null || DeclaresStructural(manifest, dropped.Name))
                    {
                        continue;
                    }

                    var treeId = AppActivationTreeNames.StructuralTree(tenant, slug, dropped.Name);
                    try
                    {
                        await engine._trees.SoftDeleteAsync(treeId, cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
                    {
                        return Step.Fail(record, AppActivationFailure.TreeProvisioningFailed, "tree-delete",
                            $"$.trees[{dropped.Name}]", $"Soft-deleting dropped tree '{treeId}' failed: {Describe(ex)}", manifest);
                    }
                }
            }

            return Step.Ok(record, false, manifest);
        }

        private static bool DeclaresStructural(AppManifest manifest, string name)
        {
            foreach (var tree in manifest.Trees)
            {
                if (tree.AdoptedTreeId is null && string.Equals(tree.Name, name, StringComparison.Ordinal))
                {
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// Fails an activation closed: when the installed version itself cannot be activated (its
        /// manifest is unavailable, invalid, or exceeds the consented ceiling), any rules left over
        /// from an earlier activation are withdrawn so nothing stays granted under a consent that no
        /// longer describes them. Transient provisioning or persistence failures keep the existing
        /// rules so a retry is not an outage.
        /// </summary>
        private async Task<Step> FailClosedAsync(Step failed, CancellationToken cancellationToken)
        {
            if (failed.Failure is AppActivationFailure.TreeProvisioningFailed or AppActivationFailure.RulePersistenceFailed
                or AppActivationFailure.ReplicationModeChangeRejected or AppActivationFailure.ReplicationPreconditionFailed
                or AppActivationFailure.ReplicationEnrolmentFailed)
            {
                return failed;
            }

            if (await WithdrawRulesAsync(cancellationToken).ConfigureAwait(false) is { } withdrawFailure)
            {
                var diagnostics = new List<AppManifestError>(failed.Diagnostics);
                diagnostics.AddRange(withdrawFailure.Diagnostics);
                return failed with { Diagnostics = diagnostics };
            }

            return failed;
        }

        private Task<Step?> WithdrawRulesAsync(CancellationToken cancellationToken) =>
            ReplaceRulesAsync(Array.Empty<LatticeAuthorizationRule>(), cancellationToken);

        /// <summary>
        /// Replaces the app's stored owned rules for this tenant with <paramref name="compiled"/>
        /// (whole-set replacement). Returns a failed step, or <c>null</c> on success.
        /// </summary>
        private async Task<Step?> ReplaceRulesAsync(
            IReadOnlyList<LatticeAuthorizationRule> compiled,
            CancellationToken cancellationToken)
        {
            var store = engine._policyStore!;
            try
            {
                var prefix = AppRoleCompiler.GetOwnedRuleIdPrefix(slug);
                var stored = new List<LatticeAuthorizationRule>();
                await foreach (var rule in store.ListRulesAsync(cancellationToken).ConfigureAwait(false))
                {
                    // Rule ids share the app prefix across tenants, so only this tenant's rules
                    // are part of the set being replaced.
                    if (rule.RuleId.StartsWith(prefix, StringComparison.Ordinal)
                        && AppActivationTreeNames.BelongsToTenant(rule.Scope.TreeId, tenant))
                    {
                        stored.Add(rule);
                    }
                }

                var diff = AppRoleCompiler.ComputeDiff(slug, compiled, stored);

                // Withdrawals before grants: a store fault part-way through leaves a subset of
                // the old grants plus part of the new ones, never a grant the current consent
                // has withdrawn (a revoked binding, a dropped scope) alongside the new set.
                foreach (var rule in diff.ToDelete)
                {
                    await store.RemoveRuleAsync(rule.Scope.TreeId, rule.RuleId, cancellationToken).ConfigureAwait(false);
                }

                foreach (var rule in diff.ToUpsert)
                {
                    await store.PutRuleAsync(rule, cancellationToken).ConfigureAwait(false);
                }

                return null;
            }
            catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
            {
                return Step.Fail(null, AppActivationFailure.RulePersistenceFailed, "rules", "$.roles",
                    $"Persisting the app's owned rules failed: {Describe(ex)}", applied);
            }
        }
    }
}
