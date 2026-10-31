using Grpc.Core;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Api.Auth.Grpc;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Api.Backup.Grpc;
using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Api.Data.Grpc;
using Orleans.Lattice.Api.TreeAdmin.Grpc;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts;

/// <summary>The facade calls the contracts share: idempotent writes, counts, and catalogue lookups.</summary>
public static class ContractCalls
{
    private const int PageSize = 500;

    /// <summary>Creates a group (or renames it) and adds every pinned member; idempotent.</summary>
    /// <param name="auth">The Auth API.</param>
    /// <param name="group">The pinned group.</param>
    public static async Task UpsertGroupAsync(LatticeAuthApiGrpcClient auth, PinnedGroup group)
    {
        await auth.UpsertGroupAsync(new AuthGroup { GroupId = group.GroupId, DisplayName = group.DisplayName });
        var members = (await auth.ListGroupMembersAsync(new AuthGroupRef { GroupId = group.GroupId })).Values;
        foreach (var user in group.Users.Where(user => !members.Contains(user)))
        {
            await auth.AddMemberAsync(new AuthMemberEdge { GroupId = group.GroupId, MemberId = user, MemberKind = MembershipMemberKind.User });
        }

        foreach (var nested in group.Groups.Where(nested => !members.Contains(nested)))
        {
            await auth.AddMemberAsync(new AuthMemberEdge { GroupId = group.GroupId, MemberId = nested, MemberKind = MembershipMemberKind.Group });
        }
    }

    /// <summary>Whether the group exists and holds every pinned member.</summary>
    /// <param name="auth">The Auth API.</param>
    /// <param name="group">The pinned group.</param>
    public static async Task<bool> GroupHoldsAsync(LatticeAuthApiGrpcClient auth, PinnedGroup group)
    {
        if ((await auth.GetGroupAsync(new AuthGroupRef { GroupId = group.GroupId })).Group is null)
        {
            return false;
        }

        var members = (await auth.ListGroupMembersAsync(new AuthGroupRef { GroupId = group.GroupId })).Values;
        return group.Users.Concat(group.Groups).All(members.Contains);
    }

    /// <summary>Whether a group exists.</summary>
    /// <param name="auth">The Auth API.</param>
    /// <param name="groupId">The group id.</param>
    public static async Task<bool> GroupExistsAsync(LatticeAuthApiGrpcClient auth, string groupId) =>
        (await auth.GetGroupAsync(new AuthGroupRef { GroupId = groupId })).Group is not null;

    /// <summary>The members of a group, or none when it does not exist.</summary>
    /// <param name="auth">The Auth API.</param>
    /// <param name="groupId">The group id.</param>
    public static async Task<IReadOnlyList<string>> MembersAsync(LatticeAuthApiGrpcClient auth, string groupId) =>
        await GroupExistsAsync(auth, groupId)
            ? (await auth.ListGroupMembersAsync(new AuthGroupRef { GroupId = groupId })).Values
            : [];

    /// <summary>Writes a pinned rule record; idempotent.</summary>
    /// <param name="auth">The Auth API.</param>
    /// <param name="rule">The record.</param>
    public static Task PutRuleAsync(LatticeAuthApiGrpcClient auth, PinnedRule rule) =>
        auth.PutRuleAsync(new AuthPutRule { Rule = rule.ToRule() });

    /// <summary>The stored rule a pinned record names, or <see langword="null"/>.</summary>
    /// <param name="auth">The Auth API.</param>
    /// <param name="rule">The record.</param>
    public static async Task<LatticeAuthorizationRule?> GetRuleAsync(LatticeAuthApiGrpcClient auth, PinnedRule rule) =>
        (await auth.GetRuleAsync(new AuthRuleRef { TreeId = rule.TreeId, RuleId = rule.RuleId })).Rule;

    /// <summary>Whether the stored rule matches the pinned record: subject, scope, operations and effect.</summary>
    /// <param name="auth">The Auth API.</param>
    /// <param name="rule">The record.</param>
    public static async Task<bool> RuleHoldsAsync(LatticeAuthApiGrpcClient auth, PinnedRule rule) =>
        await GetRuleAsync(auth, rule) is { } stored
        && stored.Subject == rule.Subject
        && stored.Scope == LatticeScope.Tree(rule.TreeId)
        && stored.Operations == rule.Operations
        && stored.Effect == LatticeEffect.Allow;

    /// <summary>Every stored rule, across every page.</summary>
    /// <param name="auth">The Auth API.</param>
    public static async Task<IReadOnlyList<LatticeAuthorizationRule>> AllRulesAsync(LatticeAuthApiGrpcClient auth)
    {
        var rules = new List<LatticeAuthorizationRule>();
        string? token = null;
        do
        {
            var page = await auth.ListRulesAsync(new AuthPageRequest { PageSize = AuthPageRequest.MaxPageSize, PageToken = token });
            rules.AddRange(page.Entries);
            token = page.NextPageToken;
        }
        while (token is not null);

        return rules;
    }

    /// <summary>Every stored group, across every page.</summary>
    /// <param name="auth">The Auth API.</param>
    public static async Task<IReadOnlyList<AuthGroup>> AllGroupsAsync(LatticeAuthApiGrpcClient auth)
    {
        var groups = new List<AuthGroup>();
        string? token = null;
        do
        {
            var page = await auth.ListGroupsAsync(new AuthPageRequest { PageSize = AuthPageRequest.MaxPageSize, PageToken = token });
            groups.AddRange(page.Entries);
            token = page.NextPageToken;
        }
        while (token is not null);

        return groups;
    }

    /// <summary>Upserts <paramref name="entries"/> in batches; idempotent.</summary>
    /// <param name="data">The Data API.</param>
    /// <param name="treeId">The tree.</param>
    /// <param name="entries">The entries.</param>
    public static async Task SetManyAsync(LatticeDataApiGrpcClient data, string treeId, IEnumerable<StoryEntry> entries)
    {
        foreach (var batch in entries.Chunk(PageSize))
        {
            await ContractWait.RetryingAsync($"writing {batch.Length} entries to {treeId}", () =>
                data.SetManyAsync(new DataSetManyRequest
                {
                    TreeId = treeId,
                    Upserts = [.. batch.Select(entry => new DataEntry { Key = entry.Key, Value = entry.Value })],
                }));
        }
    }

    /// <summary>Every live entry of <paramref name="treeId"/> in <c>[start, end)</c>.</summary>
    /// <param name="data">The Data API.</param>
    /// <param name="treeId">The tree.</param>
    /// <param name="start">The first key, or <see langword="null"/> for the tree's first.</param>
    /// <param name="end">The key the range stops before, or <see langword="null"/> for the tree's end.</param>
    public static async Task<IReadOnlyList<DataEntry>> RangeAsync(LatticeDataApiGrpcClient data, string treeId, string? start = null, string? end = null)
    {
        var entries = new List<DataEntry>();
        string? token = null;
        do
        {
            var page = await data.ReadRangeAsync(new DataRangeRequest
            {
                TreeId = treeId,
                StartInclusive = start,
                EndExclusive = end,
                PageSize = PageSize,
                ContinuationToken = token,
            });
            entries.AddRange(page.Entries);
            token = page.ContinuationToken;
        }
        while (token is not null);

        return entries;
    }

    /// <summary>How many live keys share <paramref name="prefix"/>.</summary>
    /// <param name="data">The Data API.</param>
    /// <param name="treeId">The tree.</param>
    /// <param name="prefix">The prefix.</param>
    public static async Task<int> CountPrefixAsync(LatticeDataApiGrpcClient data, string treeId, string prefix) =>
        (await RangeAsync(data, treeId, prefix, PrefixEnd(prefix))).Count;

    /// <summary>The value of a key, or <see langword="null"/> when it has none.</summary>
    /// <param name="data">The Data API.</param>
    /// <param name="treeId">The tree.</param>
    /// <param name="key">The key.</param>
    public static async Task<byte[]?> GetAsync(LatticeDataApiGrpcClient data, string treeId, string key)
    {
        var read = await data.GetAsync(new DataGetRequest { TreeId = treeId, Key = key });
        return read.Found ? read.Value : null;
    }

    /// <summary>The key just past every key starting with <paramref name="prefix"/>.</summary>
    /// <param name="prefix">A non-empty prefix.</param>
    public static string PrefixEnd(string prefix) => prefix[..^1] + (char)(prefix[^1] + 1);

    /// <summary>Whether a tree is live: registered and not soft-deleted.</summary>
    /// <param name="admin">The TreeAdmin API.</param>
    /// <param name="treeId">The tree.</param>
    public static async Task<bool> TreeIsLiveAsync(LatticeTreeAdminApiGrpcClient admin, string treeId)
    {
        if (!(await admin.CheckTreeExistsAsync(treeId)).Exists)
        {
            return false;
        }

        return !(await admin.GetTreeDeletionStatusAsync(treeId)).IsDeleted;
    }

    /// <summary>Whether a tree is gone for good: never registered, or purged.</summary>
    /// <param name="admin">The TreeAdmin API.</param>
    /// <param name="treeId">The tree.</param>
    public static async Task<bool> TreeIsPurgedAsync(LatticeTreeAdminApiGrpcClient admin, string treeId)
    {
        if (!(await admin.CheckTreeExistsAsync(treeId)).Exists)
        {
            return true;
        }

        return (await admin.GetTreeDeletionStatusAsync(treeId)).PurgeComplete;
    }

    /// <summary>Soft-deletes a tree, then purges it and waits for the purge; idempotent.</summary>
    /// <param name="admin">The TreeAdmin API.</param>
    /// <param name="treeId">The tree.</param>
    public static async Task DeleteAndPurgeAsync(LatticeTreeAdminApiGrpcClient admin, string treeId)
    {
        if (await TreeIsPurgedAsync(admin, treeId))
        {
            return;
        }

        if (!(await admin.GetTreeDeletionStatusAsync(treeId)).IsDeleted)
        {
            await admin.DeleteTreeAsync(treeId);
        }

        await admin.PurgeTreeAsync(treeId, confirm: true);
        await ContractWait.UntilAsync($"the purge of {treeId}", () => TreeIsPurgedAsync(admin, treeId));
    }

    /// <summary>Every catalogued backup of <paramref name="treeId"/> (or of every tree), newest first.</summary>
    /// <param name="backup">The Backup API.</param>
    /// <param name="treeId">The tree, or <see langword="null"/> for every tree.</param>
    public static async Task<IReadOnlyList<BackupManifest>> BackupsAsync(LatticeBackupApiGrpcClient backup, string? treeId = null)
    {
        var manifests = new List<BackupManifest>();
        string? token = null;
        do
        {
            var page = await backup.ListBackupsAsync(new BackupCatalogRequest
            {
                PageSize = 200,
                PageToken = token,
                TreeId = treeId,
                OrderByCreatedDescending = true,
            });
            manifests.AddRange(page.Entries);
            token = page.NextPageToken;
        }
        while (token is not null);

        return manifests;
    }

    /// <summary>The newest catalogued backup named <paramref name="name"/>, or <see langword="null"/>.</summary>
    /// <param name="backup">The Backup API.</param>
    /// <param name="name">The backup's name.</param>
    /// <param name="treeId">Its tree, or <see langword="null"/> for any.</param>
    public static async Task<BackupManifest?> FindBackupAsync(LatticeBackupApiGrpcClient backup, string name, string? treeId = null) =>
        (await BackupsAsync(backup, treeId)).FirstOrDefault(manifest => manifest.Name == name);

    /// <summary>The backup a finished capture recorded.</summary>
    /// <param name="backup">The Backup API.</param>
    /// <param name="handle">The capture's operation id.</param>
    /// <param name="what">The capture, for messages.</param>
    public static async Task<string> CapturedBackupIdAsync(LatticeBackupApiGrpcClient backup, string handle, string what)
    {
        var status = await ContractWait.SucceededAsync(what, () => backup.GetBackupOperationStatusAsync(handle), TimeSpan.FromMinutes(5));
        return status.Result.TryGetValue(BackupOperationResultKeys.BackupId, out var id) && !string.IsNullOrEmpty(id)
            ? id
            : throw new InvalidOperationException($"{what} succeeded but recorded no backup id.");
    }

    /// <summary>Whether <paramref name="exception"/> says the target does not exist.</summary>
    /// <param name="exception">The failure.</param>
    public static bool IsNotFound(RpcException exception) => exception.StatusCode == StatusCode.NotFound;
}
