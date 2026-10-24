namespace Orleans.Lattice;

/// <summary>
/// Identifies an expected, pre-commit value-validation refusal without requiring
/// transports to reference the optional package that performs validation.
/// Implementations must expose policy diagnostics only, never submitted value contents.
/// </summary>
public interface ILatticeValueValidationFault : ILatticeDomainFault
{
    /// <summary>A stable machine-readable validation category.</summary>
    string Code { get; }

    /// <summary>The tree targeted by the rejected write.</summary>
    string TreeId { get; }

    /// <summary>The key targeted by the rejected write.</summary>
    string Key { get; }

    /// <summary>The policy-derived failure reason, without submitted value contents.</summary>
    string Reason { get; }
}
