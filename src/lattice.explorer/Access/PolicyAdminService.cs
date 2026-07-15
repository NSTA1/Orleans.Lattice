using Grpc.Core;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Explorer.Access;

/// <summary>
/// The default <see cref="IPolicyAdminService"/> over an
/// <see cref="IAuthAdminClient"/>. Every action catches a
/// <see cref="LatticeAuthorizationDeniedException"/> (the translated server
/// denial) and a residual <see cref="RpcException"/> and returns a non-success
/// result, so the Access UI degrades cleanly and never leaks an exception. The
/// Explain and EffectivePermissions views are passed through from the facade
/// unchanged; this service never re-derives a verdict.
/// </summary>
public sealed class PolicyAdminService(IAuthAdminClient client) : IPolicyAdminService
{
    private readonly IAuthAdminClient _client = client ?? throw new ArgumentNullException(nameof(client));

    /// <inheritdoc />
    public async Task<AccessListView<LatticeAuthorizationRule>> ListRulesAsync(int pageSize = 0, string? pageToken = null, CancellationToken cancellationToken = default)
    {
        var request = new AuthPageRequest { PageSize = pageSize, PageToken = string.IsNullOrEmpty(pageToken) ? null : pageToken };
        try
        {
            var page = await _client.ListRulesAsync(request, cancellationToken).ConfigureAwait(false);
            return Succeeded(page.Entries, page.NextPageToken);
        }
        catch (LatticeAuthorizationDeniedException ex)
        {
            return Denied(ex);
        }
        catch (RpcException ex)
        {
            return Failed(ex);
        }
    }

    /// <inheritdoc />
    public async Task<AccessListView<LatticeAuthorizationRule>> ListRulesForTreeAsync(string treeId, int pageSize = 0, string? pageToken = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        var request = new AuthPageRequest { PageSize = pageSize, PageToken = string.IsNullOrEmpty(pageToken) ? null : pageToken };
        try
        {
            var page = await _client.ListRulesForTreeAsync(treeId, request, cancellationToken).ConfigureAwait(false);
            return Succeeded(page.Entries, page.NextPageToken);
        }
        catch (LatticeAuthorizationDeniedException ex)
        {
            return Denied(ex);
        }
        catch (RpcException ex)
        {
            return Failed(ex);
        }
    }

    /// <inheritdoc />
    public async Task<LatticeAuthorizationRule?> GetRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentException.ThrowIfNullOrEmpty(ruleId);
        try
        {
            return await _client.GetRuleAsync(treeId, ruleId, cancellationToken).ConfigureAwait(false);
        }
        catch (LatticeAuthorizationDeniedException)
        {
            return null;
        }
        catch (RpcException)
        {
            return null;
        }
    }

    /// <inheritdoc />
    public Task<AccessOperationResult> PutRuleAsync(LatticeAuthorizationRule rule, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(rule);
        return RunAsync(
            async () =>
            {
                await _client.PutRuleAsync(rule, cancellationToken).ConfigureAwait(false);
                return AccessOperationResult.Success($"Saved rule '{rule.RuleId}' on tree '{rule.Scope.TreeId}'.");
            });
    }

    /// <inheritdoc />
    public Task<AccessOperationResult> DeleteRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentException.ThrowIfNullOrEmpty(ruleId);
        return RunAsync(
            async () =>
            {
                var removed = await _client.RemoveRuleAsync(treeId, ruleId, cancellationToken).ConfigureAwait(false);
                return AccessOperationResult.Success(removed
                    ? $"Deleted rule '{ruleId}' on tree '{treeId}'."
                    : $"Rule '{ruleId}' on tree '{treeId}' was already absent.");
            });
    }

    /// <inheritdoc />
    public async Task<ExplainView> ExplainAsync(string subjectId, LatticeOperation operation, LatticeScope scope, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(subjectId);
        ArgumentNullException.ThrowIfNull(scope);
        try
        {
            var explanation = await _client.ExplainAsync(subjectId, operation, scope, cancellationToken).ConfigureAwait(false);
            return new ExplainView { Status = AccessOperationStatus.Succeeded, Explanation = explanation };
        }
        catch (LatticeAuthorizationDeniedException ex)
        {
            return new ExplainView { Status = AccessOperationStatus.Denied, Message = AccessFailure.DenialMessage(ex) };
        }
        catch (RpcException ex)
        {
            return new ExplainView { Status = AccessOperationStatus.Failed, Message = AccessFailure.FailureMessage(ex) };
        }
    }

    /// <inheritdoc />
    public async Task<EffectivePermissionsView> EffectivePermissionsAsync(string subjectId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(subjectId);
        try
        {
            var permissions = await _client.EffectivePermissionsAsync(subjectId, cancellationToken).ConfigureAwait(false);
            return new EffectivePermissionsView { Status = AccessOperationStatus.Succeeded, Permissions = permissions };
        }
        catch (LatticeAuthorizationDeniedException ex)
        {
            return new EffectivePermissionsView { Status = AccessOperationStatus.Denied, Message = AccessFailure.DenialMessage(ex) };
        }
        catch (RpcException ex)
        {
            return new EffectivePermissionsView { Status = AccessOperationStatus.Failed, Message = AccessFailure.FailureMessage(ex) };
        }
    }

    private static AccessListView<LatticeAuthorizationRule> Succeeded(IReadOnlyList<LatticeAuthorizationRule> entries, string? nextPageToken) =>
        new() { Status = AccessOperationStatus.Succeeded, Entries = entries, NextPageToken = nextPageToken };

    private static AccessListView<LatticeAuthorizationRule> Denied(LatticeAuthorizationDeniedException ex) =>
        new() { Status = AccessOperationStatus.Denied, Message = AccessFailure.DenialMessage(ex) };

    private static AccessListView<LatticeAuthorizationRule> Failed(RpcException ex) =>
        new() { Status = AccessOperationStatus.Failed, Message = AccessFailure.FailureMessage(ex) };

    private static async Task<AccessOperationResult> RunAsync(Func<Task<AccessOperationResult>> action)
    {
        try
        {
            return await action().ConfigureAwait(false);
        }
        catch (LatticeAuthorizationDeniedException ex)
        {
            return AccessOperationResult.Denied(AccessFailure.DenialMessage(ex));
        }
        catch (RpcException ex)
        {
            return AccessOperationResult.Failure(AccessFailure.FailureMessage(ex));
        }
    }
}
