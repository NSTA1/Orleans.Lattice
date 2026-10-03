using System.IO.Hashing;
using System.Text;

namespace Orleans.Lattice;

/// <summary>
/// Builds the <c>ProjectionVersion</c> fingerprint every built-in view projection
/// derives from its definition. A projection appends its own descriptor fields to a
/// <see cref="StringBuilder"/>, calls <see cref="AppendFilter"/> for its optional
/// predicate, and hashes the result with <see cref="Hash"/>, so two definitions that
/// differ in any field - including any node of the filter tree - carry different
/// versions and a redefined view rebuilds rather than reusing stale rows.
/// </summary>
internal static class ProjectionVersionFingerprint
{
    /// <summary>
    /// Appends the canonical encoding of <paramref name="filter"/>, or the literal
    /// <c>none</c> when the projection has no filter.
    /// </summary>
    /// <param name="builder">The fingerprint being built.</param>
    /// <param name="filter">The projection's filter tree, or <see langword="null"/>.</param>
    internal static void AppendFilter(StringBuilder builder, LatticePredicateNode? filter)
    {
        if (filter is { } node)
        {
            AppendNode(builder, node);
        }
        else
        {
            builder.Append("none");
        }
    }

    /// <summary>
    /// Hashes the fingerprint text to the upper-case hexadecimal XxHash128 digest
    /// used as the projection version.
    /// </summary>
    /// <param name="builder">The completed fingerprint.</param>
    internal static string Hash(StringBuilder builder)
    {
        var hash = XxHash128.Hash(Encoding.UTF8.GetBytes(builder.ToString()));
        return Convert.ToHexString(hash);
    }

    private static void AppendNode(StringBuilder builder, in LatticePredicateNode node)
    {
        // Length-prefix the two variable-length, caller-controlled string fields
        // (member path and constant) so a value containing the ':' field delimiter
        // cannot shift a field boundary and make a structurally different node
        // serialize identically - which would let a redefined view reuse a stale
        // ProjectionVersion and skip the rebuild the change requires.
        var memberPath = node.MemberPath ?? string.Empty;
        var constant = node.Constant.ToString() ?? string.Empty;
        builder.Append('(')
            .Append((int)node.Kind).Append(':')
            .Append(memberPath.Length).Append(':').Append(memberPath).Append(':')
            .Append((int)node.ComparisonOperator).Append(':')
            .Append((int)node.BooleanOperator).Append(':')
            .Append((int)node.StringMethod).Append(':')
            .Append(constant.Length).Append(':').Append(constant);

        // Only a type test reads ValueKind, so only it contributes one: every tree
        // built before TypeOf existed keeps the version it always had.
        if (node.Kind == LatticePredicateNodeKind.TypeOf)
        {
            builder.Append(':').Append((int)node.ValueKind);
        }

        if (node.Children is { } children)
        {
            builder.Append(":[");
            foreach (var child in children)
            {
                AppendNode(builder, child);
            }

            builder.Append(']');
        }

        builder.Append(')');
    }
}
