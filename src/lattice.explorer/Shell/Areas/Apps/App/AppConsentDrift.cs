using System.Collections.Immutable;
using Orleans.Lattice.Api.Apps;

namespace Orleans.Lattice.Explorer.Shell.Areas.Apps.App;

/// <summary>
/// How an installed app's effective consent differs from what its installed manifest
/// needs: each finding is one plain sentence an <c>AppInstall</c> holder can act on
/// through the catalogue's re-consent flow.
/// </summary>
/// <param name="Findings">The differences, in a stable order; empty when the consent covers the manifest.</param>
internal sealed record AppConsentDrift(ImmutableArray<string> Findings)
{
    /// <summary>Whether the consent fails to cover the installed manifest in any way.</summary>
    public bool HasDrift => !Findings.IsDefaultOrEmpty;

    /// <summary>
    /// Compares <paramref name="consent"/> with the needs of <paramref name="installed"/>: the
    /// consented version, the operations its roles need, cross-app role scopes and adopted
    /// trees outside the approved exception scopes, and the bridge operations its UI asks for.
    /// </summary>
    /// <param name="installed">The installed version's administrative description.</param>
    /// <param name="consent">The effective consent, or <see langword="null"/> when none is recorded.</param>
    /// <returns>The drift; <see cref="HasDrift"/> is <see langword="false"/> when the consent covers everything.</returns>
    public static AppConsentDrift Analyze(AppDescriptor installed, AppConsentReport? consent)
    {
        ArgumentNullException.ThrowIfNull(installed);

        var findings = ImmutableArray.CreateBuilder<string>();
        if (installed.State == AppLifecycleState.Failed)
        {
            findings.Add("Activation failed, so the app is not running. Review its consent and enable it again.");
        }

        if (consent is null)
        {
            findings.Add("No consent is recorded for this install.");
            return new AppConsentDrift(findings.ToImmutable());
        }

        if (!string.Equals(consent.Version, installed.Version, StringComparison.Ordinal))
        {
            findings.Add($"The consent covers version {consent.Version}, but version {installed.Version} is installed.");
        }

        var needed = installed.Roles.Aggregate(LatticeOperation.None, (all, role) => all | role.Operations);
        var missing = needed & ~consent.Ceiling.AllowedOperations;
        if (missing != LatticeOperation.None)
        {
            findings.Add($"Its roles need {AppPageText.Operations(missing)}, which the consented ceiling does not allow.");
        }

        var scopes = consent.Ceiling.ApprovedExceptionScopes;
        foreach (var role in installed.Roles)
        {
            foreach (var scope in role.Scopes)
            {
                if (scope.App is { } app
                    && !string.Equals(app, installed.Slug, StringComparison.Ordinal)
                    && !scopes.Any(approved => string.Equals(approved.App, app, StringComparison.Ordinal)
                        && string.Equals(approved.Tree, scope.Tree, StringComparison.Ordinal)))
                {
                    findings.Add($"The role {role.Name} reaches a/{app}/{scope.Tree}, outside the app's namespace, without an approved exception scope.");
                }
            }
        }

        foreach (var tree in installed.Trees)
        {
            if (tree.AdoptedTreeId is { } adopted
                && !scopes.Any(approved => string.Equals(approved.AdoptedTreeId, adopted, StringComparison.Ordinal)))
            {
                findings.Add($"The adopted tree {tree.Name} is not covered by an approved exception scope.");
            }
        }

        var consented = consent.BridgeGrants ?? [];
        foreach (var requested in installed.Ui?.Bridge ?? [])
        {
            var covered = consented.Any(grant =>
                string.Equals(grant.Operation, requested.Operation, StringComparison.Ordinal)
                && (grant.Tree is null || string.Equals(grant.Tree, requested.Tree, StringComparison.Ordinal)));
            if (!covered)
            {
                var reach = requested.Tree is null ? string.Empty : " on tree " + requested.Tree;
                findings.Add($"Its UI asks to {AppPageText.BridgeOperation(requested.Operation)} ({requested.Operation}{reach}), which was not consented.");
            }
        }

        return new AppConsentDrift(findings.ToImmutable());
    }
}
