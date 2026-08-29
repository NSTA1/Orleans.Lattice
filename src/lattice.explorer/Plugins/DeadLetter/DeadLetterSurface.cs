using Orleans.Lattice.Explorer.Core.DeadLetter;

namespace Orleans.Lattice.Explorer.Plugins.DeadLetter;

/// <summary>
/// The one place in this package that touches an Explorer service. It adapts the
/// shared dead-letter reader onto <see cref="IDeadLetterSurface"/>, so the view
/// depends on the narrow contract rather than on the Explorer core.
/// </summary>
/// <param name="reader">The shared dead-letter reader.</param>
internal sealed class DeadLetterSurface(IDeadLetterReader reader) : IDeadLetterSurface
{
    private readonly IDeadLetterReader _reader = reader ?? throw new ArgumentNullException(nameof(reader));

    /// <inheritdoc />
    public Task<int> CountAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return _reader.CountAsync(treeId, cancellationToken);
    }

    /// <inheritdoc />
    public Task<DeadLetterPage> ListAsync(
        string treeId,
        int pageSize,
        string? continuationToken = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return _reader.ListAsync(treeId, pageSize, continuationToken, cancellationToken);
    }
}
