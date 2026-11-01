using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Explorer.UI.Transport;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// The value editor's schema preview: the tree's policy, read once when the editor
/// opens, compiled with the cluster's own validator so a draft is judged exactly as
/// enforcement will judge it. Whether a violating value is then refused is the
/// facade's decision; the preview only says what will be checked.
/// </summary>
internal sealed class DataSchemaCheck
{
    private readonly LatticeSchemaPolicyValidator? _validator;

    private DataSchemaCheck(LatticeSchemaPolicyValidator? validator, string? note)
    {
        _validator = validator;
        Note = note;
    }

    /// <summary>A check for a tree whose policy was not read: nothing is previewed.</summary>
    public static DataSchemaCheck NotRead { get; } = new(null, null);

    /// <summary>Whether the tree has a policy the draft is checked against.</summary>
    public bool HasPolicy => _validator is not null;

    /// <summary>How many rules the policy holds.</summary>
    public int RuleCount => _validator?.RuleCount ?? 0;

    /// <summary>
    /// Why the draft is not checked before save, when it is not - the policy could not
    /// be read, or the cluster would itself refuse it - or <see langword="null"/>.
    /// </summary>
    public string? Note { get; }

    /// <summary>Reads the policy of <paramref name="stateTreeId"/> through the circuit's schema facade.</summary>
    /// <param name="services">The circuit's services.</param>
    /// <param name="stateTreeId">The tree's state id. Never rendered.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    public static async Task<DataSchemaCheck> LoadAsync(IServiceProvider services, string stateTreeId, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrEmpty(stateTreeId);
        ILatticeSchemaControl? schema;
        try
        {
            schema = services.GetShellFacade<ILatticeSchemaControl>();
        }
        catch (InvalidOperationException)
        {
            schema = null;
        }

        if (schema is null)
        {
            return new DataSchemaCheck(null, "This Explorer cannot read schema policies, so the value is not checked before it is saved. The cluster still checks it.");
        }

        LatticeSchemaPolicy? policy;
        try
        {
            policy = await schema.GetPolicyAsync(stateTreeId, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception)
        {
            return new DataSchemaCheck(null, "The tree's schema policy could not be read, so the value is not checked before it is saved. The cluster still checks it.");
        }

        return From(policy);
    }

    /// <summary>A check over <paramref name="policy"/>, or one that previews nothing when there is none.</summary>
    /// <param name="policy">The policy, or <see langword="null"/>.</param>
    public static DataSchemaCheck From(LatticeSchemaPolicy? policy)
    {
        if (policy is null || policy.Rules.Count == 0)
        {
            return new DataSchemaCheck(null, null);
        }

        try
        {
            return new DataSchemaCheck(new LatticeSchemaPolicyValidator(policy), null);
        }
        catch (ArgumentException)
        {
            return new DataSchemaCheck(null, "The tree's schema policy cannot be compiled here, so the value is not checked before it is saved. The cluster still checks it.");
        }
    }

    /// <summary>The reason of every rule <paramref name="value"/> fails, in rule order; empty when it complies.</summary>
    /// <param name="value">The draft bytes.</param>
    public IReadOnlyList<string> Check(byte[] value)
    {
        ArgumentNullException.ThrowIfNull(value);
        if (_validator is not { } validator)
        {
            return [];
        }

        List<string>? reasons = null;
        for (var index = 0; index < validator.RuleCount; index++)
        {
            if (validator.ValidateRule(index, value) is { } reason)
            {
                (reasons ??= []).Add(reason);
            }
        }

        return reasons is null ? [] : reasons;
    }
}
