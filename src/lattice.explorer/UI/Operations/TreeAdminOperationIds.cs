using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Orleans.Lattice.Api.TreeAdmin;

namespace Orleans.Lattice.Explorer.UI.Operations;

/// <summary>
/// The operation ids the Explorer gives the tree-administration operations it starts
/// (#4124), which say what each operation is for so a page opened later - in another
/// tab or after a reload - finds the one still running for the view, index, tree or
/// partition it shows. An operation's status names its trees but not its view, index
/// or partition, so the id carries that instead: <c>&lt;kind&gt;-&lt;target&gt;-&lt;nonce&gt;</c>,
/// where the target is the first 16 hex digits of the SHA-256 of the target's name
/// (an id may hold only letters, digits, <c>-</c>, <c>_</c> and <c>.</c>) and the
/// nonce makes every start a new operation.
/// </summary>
internal static class TreeAdminOperationIds
{
    /// <summary>A fresh id for an operation of <paramref name="kind"/> over <paramref name="target"/>.</summary>
    /// <param name="kind">The operation kind, one of <see cref="TreeAdminOperationKinds"/>.</param>
    /// <param name="target">What the operation is for, as <see cref="Target(string, int)"/> or a view, index or tree name.</param>
    /// <returns>The id.</returns>
    public static string New(string kind, string target) =>
        PrefixOf(kind, target) + Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture);

    /// <summary>Whether <paramref name="operationId"/> is one the Explorer gave an operation of <paramref name="kind"/> over <paramref name="target"/>.</summary>
    /// <param name="operationId">The id.</param>
    /// <param name="kind">The operation kind.</param>
    /// <param name="target">The target.</param>
    /// <returns><see langword="true"/> when it is.</returns>
    public static bool Matches(string operationId, string kind, string target)
    {
        ArgumentNullException.ThrowIfNull(operationId);
        return operationId.StartsWith(PrefixOf(kind, target), StringComparison.Ordinal);
    }

    /// <summary>The target of a WAL partition: its tree and partition together.</summary>
    /// <param name="treeId">The tree.</param>
    /// <param name="partition">The partition.</param>
    /// <returns>The target.</returns>
    public static string Target(string treeId, int partition)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return treeId + "\n" + partition.ToString(CultureInfo.InvariantCulture);
    }

    /// <summary>The leading part every id of <paramref name="kind"/> over <paramref name="target"/> shares, ending in <c>-</c>.</summary>
    /// <param name="kind">The operation kind.</param>
    /// <param name="target">The target.</param>
    /// <returns>The prefix.</returns>
    public static string PrefixOf(string kind, string target)
    {
        ArgumentException.ThrowIfNullOrEmpty(kind);
        ArgumentNullException.ThrowIfNull(target);
        var name = kind.StartsWith(TreeAdminOperationKinds.Prefix, StringComparison.Ordinal)
            ? kind[TreeAdminOperationKinds.Prefix.Length..]
            : kind;
        Span<byte> hash = stackalloc byte[SHA256.HashSizeInBytes];
        SHA256.HashData(Encoding.UTF8.GetBytes(target), hash);
        return name + "-" + Convert.ToHexStringLower(hash[..8]) + "-";
    }
}
