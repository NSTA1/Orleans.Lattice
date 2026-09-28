using System.Collections.Frozen;
using System.Collections.Immutable;
using Orleans.Lattice.Api.Apps;

namespace Orleans.Lattice.Explorer.Shell.Framing;

/// <summary>
/// One authorised launch of an app's UI: the proof that the per-launch workspace gate
/// passed for this circuit's user, and the fixed authority of the frame's port.
/// </summary>
/// <remarks>
/// Only <see cref="AppFrameBundleLoader.AuthorizeAsync"/> constructs one, and it records
/// the loader that issued it, so a launch cannot be forged or carried to another
/// circuit. The port's authority is the launch's <c>(slug, install revision)</c>: nothing
/// the frame sends can change it.
/// </remarks>
internal sealed class AppFrameLaunch
{
    internal AppFrameLaunch(
        AppFrameBundleLoader issuer,
        WorkspaceAppDescriptor descriptor,
        AppUiDescriptor ui,
        ImmutableArray<string> roles)
    {
        Issuer = issuer;
        Slug = descriptor.Slug;
        Version = descriptor.Version;
        InstallRevision = descriptor.InstallRevision;
        SourceKey = descriptor.SourceKey ?? string.Empty;
        DisplayName = string.IsNullOrWhiteSpace(descriptor.Presentation?.DisplayName)
            ? descriptor.Slug
            : descriptor.Presentation.DisplayName;
        Ui = ui;
        Trees = descriptor.Trees.IsDefault
            ? FrozenSet<string>.Empty
            : descriptor.Trees.Select(tree => tree.Name).Where(name => name is not null).ToFrozenSet(StringComparer.Ordinal);
        Grants = ui.Bridge.IsDefault ? [] : ui.Bridge;
        Roles = SanitiseRoles(roles);
    }

    /// <summary>The loader, and therefore the circuit, that authorised this launch.</summary>
    internal AppFrameBundleLoader Issuer { get; }

    /// <summary>The app slug.</summary>
    public string Slug { get; }

    /// <summary>The installed version.</summary>
    public string Version { get; }

    /// <summary>The install revision the frame is bound to; a later revision revokes it.</summary>
    public long InstallRevision { get; }

    /// <summary>The source the install came from, or empty when unrecorded; part of the cache key.</summary>
    public string SourceKey { get; }

    /// <summary>The app's display name as plain text, falling back to its slug.</summary>
    public string DisplayName { get; }

    /// <summary>The installed version's UI declaration.</summary>
    public AppUiDescriptor Ui { get; }

    /// <summary>The app's declared logical tree names, compared ordinally.</summary>
    public FrozenSet<string> Trees { get; }

    /// <summary>
    /// The caller's app role names in this app, as the workspace listed them when the launch
    /// was authorised: well-formed names only, de-duplicated, in workspace order, at most
    /// <see cref="AppFrameProtocol.MaxRoles"/>. A snapshot for the launch's lifetime; a
    /// re-launch (including one after revocation) authorises afresh and so refreshes it.
    /// </summary>
    public ImmutableArray<string> Roles { get; }

    /// <summary>The consented bridge grants of the installed version.</summary>
    public ImmutableArray<AppUiBridgeGrantDescriptor> Grants { get; }

    /// <summary>
    /// Returns whether the install's bridge set grants <paramref name="operation"/>, and, for a
    /// data operation, grants it over <paramref name="tree"/> (a grant with no tree covers every
    /// declared tree).
    /// </summary>
    /// <param name="operation">The operation.</param>
    /// <param name="tree">The logical tree for a data operation, or <see langword="null"/>.</param>
    /// <returns><see langword="true"/> only when a grant covers the request.</returns>
    public bool IsGranted(string operation, string? tree)
    {
        foreach (var grant in Grants)
        {
            if (grant is null || !string.Equals(grant.Operation, operation, StringComparison.Ordinal))
            {
                continue;
            }

            if (!AppFrameProtocol.IsDataOperation(operation))
            {
                return true;
            }

            if (tree is not null && (grant.Tree is null || string.Equals(grant.Tree, tree, StringComparison.Ordinal)))
            {
                return true;
            }
        }

        return false;
    }

    private static ImmutableArray<string> SanitiseRoles(ImmutableArray<string> roles)
    {
        if (roles.IsDefaultOrEmpty)
        {
            return [];
        }

        var kept = ImmutableArray.CreateBuilder<string>(Math.Min(roles.Length, AppFrameProtocol.MaxRoles));
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var role in roles)
        {
            if (kept.Count == AppFrameProtocol.MaxRoles)
            {
                break;
            }

            if (role is not null && AppFrameProtocol.IsRoleName(role) && seen.Add(role))
            {
                kept.Add(role);
            }
        }

        return kept.ToImmutable();
    }
}