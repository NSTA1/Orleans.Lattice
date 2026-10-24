namespace Orleans.Lattice.Explorer.UI.Transport;

/// <summary>A payload-free, expected schema rejection received over the data transport.</summary>
internal sealed class ShellValueValidationException(
    string message, string treeId, string key, string reason, Exception innerException)
    : ArgumentException(message, innerException), ILatticeValueValidationFault
{
    /// <inheritdoc />
    public string Code => "schema-policy";

    /// <inheritdoc />
    public string TreeId { get; } = treeId;

    /// <inheritdoc />
    public string Key { get; } = key;

    /// <inheritdoc />
    public string Reason { get; } = reason;
}
