using Orleans.Lattice.Api.Apps;

namespace Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;

/// <summary>
/// Changing the installed version's role bindings (issue #3884): review, bind roles
/// (the install flow's own step), compare the recorded and proposed group of every
/// role and confirm, then apply through <see cref="ILatticeAppRoleBindings"/>. The
/// app's consent, ceiling and lifecycle state are untouched; an enabled app has its
/// role rules replaced by the cluster, so a removed binding keeps no grant.
/// </summary>
internal sealed partial class AppInstallFlow
{
    /// <summary>Whether the flow is changing the installed version's role bindings rather than installing or re-consenting.</summary>
    public bool IsRebinding { get; private set; }

    /// <summary>
    /// Whether the installed version's role bindings can be changed here: the reviewed
    /// version is the installed one, and it declares at least one role.
    /// </summary>
    public bool CanRebind =>
        Mode == AppInstallMode.Reconsent && Installed is not null && Descriptor is { Roles.IsDefaultOrEmpty: false };

    /// <summary>Every declared role's recorded and drafted group, in declaration order.</summary>
    public IReadOnlyList<AppRoleBindingChange> BindingChanges
    {
        get
        {
            if (Descriptor is not { } descriptor)
            {
                return [];
            }

            var changes = new List<AppRoleBindingChange>(descriptor.Roles.Length);
            foreach (var role in descriptor.Roles)
            {
                changes.Add(new AppRoleBindingChange(role.Name, RecordedGroup(role.Name), _bindings.GetValueOrDefault(role.Name)));
            }

            return changes;
        }
    }

    /// <summary>Whether the drafted bindings differ from the recorded ones.</summary>
    public bool HasBindingChanges => BindingChanges.Any(change => change.IsChanged);

    /// <summary>Starts changing the installed version's role bindings from the ones it records.</summary>
    /// <exception cref="InvalidOperationException">The flow is not at the review, or <see cref="CanRebind"/> is false.</exception>
    public void BeginRebind()
    {
        Require(AppInstallStage.Review);
        if (!CanRebind)
        {
            throw new InvalidOperationException("Only the role bindings of the installed version can be changed.");
        }

        SeedRecordedBindings();
        IsRebinding = true;
        Move(AppInstallStage.BindRoles);
    }

    /// <summary>Applies the confirmed bindings, replacing every recorded one.</summary>
    /// <param name="cancellationToken">Cancels the call.</param>
    public async Task CommitBindingsAsync(CancellationToken cancellationToken = default)
    {
        Require(AppInstallStage.ConfirmBindings);
        var installed = Installed!;
        if (_facades.RoleBindings is not { } rebinder)
        {
            Fail(AppInstallStage.ConfirmBindings, "Could not change the role bindings of " + DisplayName + ". This cluster does not serve role re-binding.");
            return;
        }

        Move(AppInstallStage.Installing);
        try
        {
            var report = await rebinder.UpdateRoleBindingsAsync(new AppRoleBindingsUpdate
            {
                Slug = installed.Slug,
                Version = installed.Version,
                RoleBindings = [.. BindingChanges
                    .Where(change => change.Proposed is not null)
                    .Select(change => new AppRoleBindingDescriptor { RoleName = change.Role, GroupId = change.Proposed! })],
            }, cancellationToken);

            Installed = installed with { RoleBindings = report.RoleBindings, State = report.State };
            IsRebinding = false;
            SeedRecordedBindings();
            Move(AppInstallStage.Rebound);
        }
        catch (Exception error) when (error is not OutOfMemoryException)
        {
            Fail(AppInstallStage.ConfirmBindings, AppsFailureMessages.Describe(error, "change the role bindings of", DisplayName));
        }
    }

    private void EndRebinding()
    {
        IsRebinding = false;
        SeedRecordedBindings();
    }

    private void SeedRecordedBindings()
    {
        _bindings.Clear();
        if (Descriptor is not { } descriptor)
        {
            return;
        }

        foreach (var role in descriptor.Roles)
        {
            if (RecordedGroup(role.Name) is { } group)
            {
                _bindings[role.Name] = group;
            }
        }
    }

    private string? RecordedGroup(string role)
    {
        if (Installed is { } installed)
        {
            foreach (var binding in installed.RoleBindings)
            {
                if (string.Equals(binding.RoleName, role, StringComparison.Ordinal))
                {
                    return binding.GroupId;
                }
            }
        }

        return null;
    }
}
