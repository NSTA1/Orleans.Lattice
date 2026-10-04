using System.Collections.Immutable;
using System.Text.RegularExpressions;
using Orleans.Lattice.Api.Apps;

namespace Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;

/// <summary>
/// One staged install, upgrade or re-consent of an app version from a named
/// source: the state machine behind the review page (epic decision E15).
/// </summary>
/// <remarks>
/// <para>
/// The flow lives in the circuit's <see cref="AppInstallFlowStore"/>, so leaving
/// the page and coming back resumes it at the stage it reached - including while a
/// step is still running. Every transition raises <see cref="Changed"/>.
/// </para>
/// <para>
/// A source that advertises <c>RequiresAcquisition</c> passes through
/// <see cref="AppInstallStage.Acquiring"/> (the description is being fetched) and
/// <see cref="AppInstallStage.Verifying"/> (the description and icon are checked
/// against the manifest's digests) before the review; any other source goes
/// straight from <see cref="AppInstallStage.Resolving"/> to the review.
/// </para>
/// <para>
/// Continuations stay on the caller's synchronization context (no
/// <c>ConfigureAwait(false)</c>), so a flow started from a component mutates its
/// state only on that circuit's renderer thread.
/// </para>
/// </remarks>
internal sealed partial class AppInstallFlow
{
    private readonly AppsFacades _facades;
    private readonly Dictionary<string, string> _bindings = new(StringComparer.Ordinal);
    private Task? _loading;

    /// <summary>Creates a flow that has not started.</summary>
    /// <param name="facades">The circuit's facades.</param>
    /// <param name="key">The flow's identity in its store.</param>
    /// <param name="source">The source's summary, or <see langword="null"/> when the source is not listed.</param>
    public AppInstallFlow(AppsFacades facades, AppInstallFlowKey key, AppSourceSummary? source)
    {
        ArgumentNullException.ThrowIfNull(facades);
        ArgumentNullException.ThrowIfNull(key);

        _facades = facades;
        Key = key;
        Source = source;
        Stage = RequiresAcquisition ? AppInstallStage.Acquiring : AppInstallStage.Resolving;
    }

    /// <summary>Raised after every transition.</summary>
    public event Action? Changed;

    /// <summary>The flow's identity: tenant, source, slug and requested version.</summary>
    public AppInstallFlowKey Key { get; }

    /// <summary>The source's summary, or <see langword="null"/> when it is not listed.</summary>
    public AppSourceSummary? Source { get; }

    /// <summary>Whether the source must acquire the app before it can be described.</summary>
    public bool RequiresAcquisition => Source?.Capabilities.HasFlag(AppSourceSummaryCapabilities.RequiresAcquisition) == true;

    /// <summary>The current stage.</summary>
    public AppInstallStage Stage { get; private set; }

    /// <summary>What approval does: install, upgrade or re-consent.</summary>
    public AppInstallMode Mode { get; private set; }

    /// <summary>The reviewed version's description, once resolved.</summary>
    public AppDescriptor? Descriptor { get; private set; }

    /// <summary>The installed version's description when another or the same version is installed.</summary>
    public AppDescriptor? Installed { get; private set; }

    /// <summary>The installed version's recorded consent, or <see langword="null"/> when nothing is installed.</summary>
    public AppConsentReport? Consent { get; private set; }

    /// <summary>The verified icon, or <see langword="null"/> when none is declared or it could not be read.</summary>
    public AppIconAsset? Icon { get; private set; }

    /// <summary>The upgrade's difference, for <see cref="AppInstallMode.Upgrade"/>.</summary>
    public AppUpgradeDiff? Diff { get; private set; }

    /// <summary>Consent drift of the installed version, when it is the reviewed one.</summary>
    public IReadOnlyList<AppActivationIssue> Drift { get; private set; } = [];

    /// <summary>The consent being drafted for approval.</summary>
    public AppConsentDraft Draft { get; private set; } = new(LatticeOperation.None, [], []);

    /// <summary>The role-to-group bindings being drafted, by role name.</summary>
    public IReadOnlyDictionary<string, string> Bindings => _bindings;

    /// <summary>The roles not yet bound to a group.</summary>
    public IReadOnlyList<string> UnboundRoles =>
        Descriptor is null ? [] : [.. Descriptor.Roles.Select(role => role.Name).Where(role => !_bindings.ContainsKey(role))];

    /// <summary>
    /// Whether the reviewed version is the installed one, so the page manages an install rather
    /// than offering one: its trees are already owned by that install, which activation re-verifies.
    /// </summary>
    public bool IsManaging => Mode == AppInstallMode.Reconsent;

    /// <summary>
    /// The activation failures the drafted consent would cause. Managing the installed version
    /// claims no tree, so an ownership conflict is not one of them (as for <see cref="Drift"/>).
    /// </summary>
    public IReadOnlyList<AppActivationIssue> Issues =>
        Descriptor is null ? []
        : IsManaging ? [.. AppConsentAnalysis.Preview(Descriptor, Draft).Where(issue => issue.Kind != AppActivationIssueKind.TreeOwnershipConflict)]
        : AppConsentAnalysis.Preview(Descriptor, Draft);

    /// <summary>Whether the drafted consent is refused outright at install (a tree cannot be owned).</summary>
    public bool IsBlocked => Issues.Any(issue => issue.Kind == AppActivationIssueKind.TreeOwnershipConflict);

    /// <summary>The last failure's sentence, or <see langword="null"/>.</summary>
    public string? Error { get; private set; }

    /// <summary>The stage the flow returns to from <see cref="AppInstallStage.Failed"/>.</summary>
    public AppInstallStage? ResumeStage { get; private set; }

    /// <summary>The last lifecycle result, once installed.</summary>
    public AppLifecycleResult? Result { get; private set; }

    /// <summary>Whether a step is running.</summary>
    public bool IsBusy => Stage is AppInstallStage.Resolving or AppInstallStage.Acquiring or AppInstallStage.Verifying
        or AppInstallStage.Installing or AppInstallStage.Enabling;

    /// <summary>The app's name as shown.</summary>
    public string DisplayName => AppsPresentation.DisplayName(Descriptor?.Presentation, Key.Slug);

    /// <summary>
    /// Resolves the description, acquiring and verifying first when the source
    /// requires it, then reads the installed version and its consent. Idempotent: a
    /// second call awaits the first.
    /// </summary>
    /// <param name="cancellationToken">Stops the caller waiting; the load itself continues.</param>
    public Task LoadAsync(CancellationToken cancellationToken = default)
    {
        _loading ??= RunLoadAsync();
        return _loading.WaitAsync(cancellationToken);
    }

    /// <summary>Approves the review and moves to role binding (or, for a re-consent, to the ceiling).</summary>
    public void Begin()
    {
        Require(AppInstallStage.Review);
        Move(Mode == AppInstallMode.Reconsent ? AppInstallStage.ConfirmCeiling : AppInstallStage.BindRoles);
    }

    /// <summary>Binds <paramref name="role"/> to <paramref name="groupId"/>, or unbinds it for an empty group.</summary>
    /// <param name="role">A declared role.</param>
    /// <param name="groupId">The membership group, or empty to unbind.</param>
    public void Bind(string role, string? groupId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(role);
        if (string.IsNullOrWhiteSpace(groupId))
        {
            _bindings.Remove(role);
        }
        else
        {
            _bindings[role] = groupId.Trim();
        }

        Changed?.Invoke();
    }

    /// <summary>
    /// Moves from role binding to the ceiling once every role is bound or, while
    /// re-binding the installed version, to the comparison of recorded and proposed
    /// bindings, where a role may be left unbound.
    /// </summary>
    /// <exception cref="InvalidOperationException">A role is unbound outside a re-binding.</exception>
    public void ConfirmBindings()
    {
        Require(AppInstallStage.BindRoles);
        if (IsRebinding)
        {
            Move(AppInstallStage.ConfirmBindings);
            return;
        }

        if (UnboundRoles.Count > 0)
        {
            throw new InvalidOperationException("Every role must be bound to a group first.");
        }

        Move(AppInstallStage.ConfirmCeiling);
    }

    /// <summary>Steps back one stage, out of a failure to the stage it failed from, or from a completed re-binding to the review.</summary>
    public void Back()
    {
        switch (Stage)
        {
            case AppInstallStage.BindRoles:
                if (IsRebinding)
                {
                    EndRebinding();
                }

                Move(AppInstallStage.Review);
                break;
            case AppInstallStage.ConfirmBindings:
                Move(AppInstallStage.BindRoles);
                break;
            case AppInstallStage.Rebound:
                Move(AppInstallStage.Review);
                break;
            case AppInstallStage.ConfirmCeiling:
                Move(Mode == AppInstallMode.Reconsent ? AppInstallStage.Review : AppInstallStage.BindRoles);
                break;
            case AppInstallStage.Failed when ResumeStage is { } resume:
                Error = null;
                ResumeStage = null;
                if (resume is AppInstallStage.Resolving or AppInstallStage.Acquiring)
                {
                    _loading = null;
                    Move(RequiresAcquisition ? AppInstallStage.Acquiring : AppInstallStage.Resolving);
                    _ = LoadAsync();
                }
                else
                {
                    Move(resume);
                }

                break;
        }
    }

    /// <summary>Approves or withdraws one operation in the drafted ceiling.</summary>
    /// <param name="operation">A single operation.</param>
    /// <param name="approved">Whether it is approved.</param>
    public void SetOperation(LatticeOperation operation, bool approved)
    {
        Require(AppInstallStage.ConfirmCeiling);
        Draft = Draft with { Operations = approved ? Draft.Operations | operation : Draft.Operations & ~operation };
        Changed?.Invoke();
    }

    /// <summary>Approves or withdraws one exception scope.</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="approved">Whether it is approved.</param>
    public void SetScope(AppExceptionScope scope, bool approved)
    {
        ArgumentNullException.ThrowIfNull(scope);
        Require(AppInstallStage.ConfirmCeiling);
        var scopes = Draft.Scopes.Remove(scope);
        Draft = Draft with { Scopes = approved ? scopes.Add(scope) : scopes };
        Changed?.Invoke();
    }

    /// <summary>Consents to or withholds one bridge grant.</summary>
    /// <param name="grant">The grant.</param>
    /// <param name="consented">Whether it is consented.</param>
    public void SetBridge(AppUiBridgeGrantDescriptor grant, bool consented)
    {
        ArgumentNullException.ThrowIfNull(grant);
        Require(AppInstallStage.ConfirmCeiling);
        var grants = Draft.BridgeGrants.Remove(grant);
        Draft = Draft with { BridgeGrants = consented ? grants.Add(grant) : grants };
        Changed?.Invoke();
    }

    /// <summary>
    /// Runs the approved change: installs or upgrades with the drafted bindings and
    /// ceiling, recording the drafted bridge consent when it differs from what the
    /// install records by itself, or - for a re-consent - replaces the consent.
    /// </summary>
    /// <param name="cancellationToken">Cancels the calls.</param>
    public async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        Require(AppInstallStage.ConfirmCeiling);
        var descriptor = Descriptor!;
        if (_facades.Control is not { } control)
        {
            Fail(AppInstallStage.ConfirmCeiling, "Could not install " + DisplayName + ". This cluster does not serve app management.");
            return;
        }

        Move(AppInstallStage.Installing);
        var verb = Mode switch
        {
            AppInstallMode.Upgrade => "upgrade",
            AppInstallMode.Reconsent => "record the consent of",
            _ => "install",
        };

        try
        {
            if (Mode == AppInstallMode.Reconsent)
            {
                Consent = await control.UpdateConsentAsync(ConsentUpdate(descriptor), cancellationToken);
                Result = new AppLifecycleResult { Slug = descriptor.Slug, Version = descriptor.Version, State = Installed?.State ?? AppLifecycleState.Installed, Changed = true };
            }
            else
            {
                Result = await control.InstallAsync(new AppInstallRequest
                {
                    Slug = descriptor.Slug,
                    Version = descriptor.Version,
                    RoleBindings = [.. _bindings.Select(pair => new AppRoleBindingDescriptor { RoleName = pair.Key, GroupId = pair.Value })],
                    Ceiling = Draft.ToCeiling(),
                    SourceKey = Key.SourceKey,
                    // Pins the install to the manifest reviewed here; a source that changes it before commit
                    // is refused rather than consented unseen. Null against a server that predates the pin.
                    ExpectedManifestDigest = descriptor.ManifestDigest,
                }, cancellationToken);

                // A fresh install records exactly what the manifest requests and an upgrade
                // keeps the consented set, so a different drafted set is recorded explicitly.
                var recorded = Mode == AppInstallMode.Upgrade ? Consent?.BridgeGrants ?? [] : AppConsentAnalysis.RequestedBridge(descriptor);
                if (!SameGrants(recorded, Draft.BridgeGrants))
                {
                    Consent = await control.UpdateConsentAsync(ConsentUpdate(descriptor), cancellationToken);
                }
            }

            Move(Result.State == AppLifecycleState.Enabled ? AppInstallStage.Enabled : AppInstallStage.Installed);
        }
        catch (Exception error) when (error is not OutOfMemoryException)
        {
            Fail(AppInstallStage.ConfirmCeiling, AppsFailureMessages.Describe(error, verb, DisplayName));
        }
    }

    /// <summary>Enables the installed app ("Enable now").</summary>
    /// <param name="cancellationToken">Cancels the call.</param>
    public async Task EnableAsync(CancellationToken cancellationToken = default)
    {
        Require(AppInstallStage.Installed);
        if (_facades.Control is not { } control)
        {
            Fail(AppInstallStage.Installed, "Could not enable " + DisplayName + ". This cluster does not serve app management.");
            return;
        }

        Move(AppInstallStage.Enabling);
        try
        {
            Result = await control.EnableAsync(Key.Slug, cancellationToken);
            Move(AppInstallStage.Enabled);
        }
        catch (Exception error) when (error is not OutOfMemoryException)
        {
            Fail(AppInstallStage.Installed, AppsFailureMessages.Describe(error, "enable", DisplayName));
        }
    }

    private async Task RunLoadAsync()
    {
        var verb = RequiresAcquisition ? "acquire" : "read";
        try
        {
            if (_facades.Catalog is not { } catalog)
            {
                Fail(Stage, $"Could not {verb} {Key.Slug}. This cluster does not serve the app catalogue.");
                return;
            }

            Move(RequiresAcquisition ? AppInstallStage.Acquiring : AppInstallStage.Resolving);
            var descriptor = await catalog.DescribeFromSourceAsync(Key.SourceKey, Key.Slug, Key.Version);
            if (descriptor is null)
            {
                Move(AppInstallStage.NotFound);
                return;
            }

            if (RequiresAcquisition)
            {
                Move(AppInstallStage.Verifying);
                if (Verify(descriptor) is { } problem)
                {
                    Fail(AppInstallStage.Acquiring, $"Could not verify {Key.Slug} from {Key.SourceKey}. {problem}");
                    return;
                }
            }

            if (descriptor.Presentation?.Icon is not null)
            {
                Icon = await TryAsync(() => catalog.GetIconAsync(Key.SourceKey, Key.Slug, descriptor.Version));
                if (Icon is null && RequiresAcquisition)
                {
                    Fail(AppInstallStage.Acquiring, $"Could not verify {Key.Slug} from {Key.SourceKey}. Its icon does not match its manifest digest.");
                    return;
                }
            }

            await ReadInstallationAsync(descriptor);
            Descriptor = descriptor;
            Move(AppInstallStage.Review);
        }
        catch (Exception error) when (error is not OutOfMemoryException)
        {
            Fail(RequiresAcquisition ? AppInstallStage.Acquiring : AppInstallStage.Resolving, AppsFailureMessages.Describe(error, verb, Key.Slug));
        }
    }

    private async Task ReadInstallationAsync(AppDescriptor descriptor)
    {
        if (_facades.Control is { } control)
        {
            Consent = await TryAsync(() => control.GetConsentAsync(descriptor.Slug));
            if (Consent is not null)
            {
                Installed = await TryAsync(() => control.DescribeAsync(descriptor.Slug, Consent.Version));
            }
        }

        foreach (var binding in (Installed ?? descriptor).RoleBindings)
        {
            if (descriptor.Roles.Any(role => string.Equals(role.Name, binding.RoleName, StringComparison.Ordinal)))
            {
                _bindings[binding.RoleName] = binding.GroupId;
            }
        }

        var requested = AppConsentDraft.Requested(descriptor);
        if (Consent is null)
        {
            Mode = AppInstallMode.Install;
            Draft = requested;
            return;
        }

        var recorded = AppConsentDraft.FromReport(Consent);
        Mode = string.Equals(Consent.Version, descriptor.Version, StringComparison.Ordinal) ? AppInstallMode.Reconsent : AppInstallMode.Upgrade;
        Draft = new AppConsentDraft(
            recorded.Operations | requested.Operations,
            [.. recorded.Scopes.Concat(requested.Scopes.Where(scope => !AppConsentAnalysis.Covers(recorded.Scopes, scope)))],
            [.. recorded.BridgeGrants.Concat(requested.BridgeGrants.Where(grant => !AppConsentAnalysis.Covers(recorded.BridgeGrants, grant)))]);

        if (Installed is not null)
        {
            if (Mode == AppInstallMode.Upgrade)
            {
                Diff = AppConsentAnalysis.Diff(Installed, descriptor, Consent);
            }
            else
            {
                Drift = AppConsentAnalysis.Drift(Installed, Consent);
            }
        }
    }

    private string? Verify(AppDescriptor descriptor)
    {
        if (!string.Equals(descriptor.Slug, Key.Slug, StringComparison.Ordinal))
        {
            return "The source returned a different app.";
        }

        if (Key.Version is { } version && !string.Equals(descriptor.Version, version, StringComparison.Ordinal))
        {
            return "The source returned a different version.";
        }

        if (descriptor.SourceKey is { } sourceKey && !string.Equals(sourceKey, Key.SourceKey, StringComparison.Ordinal))
        {
            return "The description came from a different source.";
        }

        if (descriptor.Ui is { } ui
            && (!IsDigest(ui.BundleDigest) || ui.Assets.Any(asset => !IsDigest(asset.Sha256))))
        {
            return "Its UI bundle is not pinned by valid digests.";
        }

        if (descriptor.Presentation?.Icon is { } icon && !IsDigest(icon.Sha256))
        {
            return "Its icon is not pinned by a valid digest.";
        }

        return null;
    }

    private AppConsentUpdate ConsentUpdate(AppDescriptor descriptor) => new()
    {
        Slug = descriptor.Slug,
        Version = descriptor.Version,
        Ceiling = Draft.ToCeiling(),
        BridgeGrants = Draft.BridgeGrants,
    };

    private static bool SameGrants(ImmutableArray<AppUiBridgeGrantDescriptor> a, ImmutableArray<AppUiBridgeGrantDescriptor> b) =>
        a.Length == b.Length && a.All(grant => b.Contains(grant));

    private static bool IsDigest(string? digest) => digest is not null && DigestPattern().IsMatch(digest);

    private static async Task<T?> TryAsync<T>(Func<Task<T?>> read)
        where T : class
    {
        try
        {
            return await read();
        }
        catch (Exception error) when (error is not OutOfMemoryException and not OperationCanceledException)
        {
            return null;
        }
    }

    private void Require(AppInstallStage stage)
    {
        if (Stage != stage)
        {
            throw new InvalidOperationException($"The install flow is at {Stage}, not {stage}.");
        }
    }

    private void Fail(AppInstallStage resume, string sentence)
    {
        Error = sentence;
        ResumeStage = resume;
        Move(AppInstallStage.Failed);
    }

    private void Move(AppInstallStage stage)
    {
        Stage = stage;
        Changed?.Invoke();
    }

    // The tail anchor is \z, not $: in .NET $ also matches immediately before a
    // trailing line feed, so "^[0-9a-f]{64}$" accepted a 64-hex digest with a
    // newline smuggled onto the end and the "pinned by valid digests" badge
    // would have shown for an asset whose digest the cluster then rejects.
    [GeneratedRegex("^[0-9a-f]{64}\\z", RegexOptions.CultureInvariant)]
    private static partial Regex DigestPattern();
}
