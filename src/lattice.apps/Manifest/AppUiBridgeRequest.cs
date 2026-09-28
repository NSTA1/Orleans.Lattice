using System.Collections.Immutable;
using System.Runtime.InteropServices;

namespace Orleans.Lattice.Apps;

/// <summary>
/// A pure, normalised set of requested bridge grants, used to couple bridge consent to upgrades:
/// an upgrade that <em>adds</em> a grant (see <see cref="AddedRelativeTo(AppUiBridgeRequest)"/>)
/// must be re-consented before activation, exactly as a widened capability ceiling must, while
/// removing a grant never requires consent. Grants are sorted ordinally and deduplicated, and a
/// data operation granted for every declared tree absorbs its per-tree grants, so two requests
/// that grant the same thing are equal.
/// </summary>
[GenerateSerializer, Alias(AppsTypeAliases.AppUiBridgeRequest), Immutable]
public sealed class AppUiBridgeRequest : IEquatable<AppUiBridgeRequest>
{
    private static readonly IComparer<AppUiBridgeGrant> Order = Comparer<AppUiBridgeGrant>.Create(static (x, y) =>
    {
        var byOperation = string.CompareOrdinal(x.Operation, y.Operation);
        return byOperation != 0 ? byOperation : string.CompareOrdinal(x.Tree, y.Tree);
    });

    private AppUiBridgeRequest(ImmutableArray<AppUiBridgeGrant> grants) => Grants = grants;

    /// <summary>The request that grants nothing.</summary>
    public static AppUiBridgeRequest Empty { get; } = new([]);

    /// <summary>The normalised grants, sorted ordinally by operation and then tree (null first).</summary>
    [Id(0)] public ImmutableArray<AppUiBridgeGrant> Grants { get; private init; }

    /// <summary>Whether the request grants nothing.</summary>
    public bool IsEmpty => Grants.IsDefaultOrEmpty;

    /// <summary>Creates a normalised request from arbitrary grants.</summary>
    /// <param name="grants">The grants; duplicates and grants subsumed by an every-tree grant are folded.</param>
    /// <exception cref="ArgumentNullException"><paramref name="grants"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// A grant names an unknown operation, names a tree for a non-data operation, or names a tree
    /// that is not a valid local tree name.
    /// </exception>
    public static AppUiBridgeRequest Create(IEnumerable<AppUiBridgeGrant> grants)
    {
        ArgumentNullException.ThrowIfNull(grants);
        HashSet<AppUiBridgeGrant> set = [];
        foreach (var grant in grants)
        {
            if (!AppUiBridgeOperations.IsKnown(grant.Operation))
                throw new ArgumentException($"Unknown bridge operation '{grant.Operation}'.", nameof(grants));
            if (grant.Tree is not null)
            {
                if (!AppUiBridgeOperations.IsDataOperation(grant.Operation))
                    throw new ArgumentException($"Bridge operation '{grant.Operation}' cannot name a tree.", nameof(grants));
                if (!AppManifestValidator.IsName(grant.Tree))
                    throw new ArgumentException($"'{grant.Tree}' is not a local tree name.", nameof(grants));
            }
            set.Add(grant);
        }
        if (set.Count == 0)
            return Empty;
        set.RemoveWhere(grant => grant.Tree is not null && set.Contains(new(grant.Operation)));
        var sorted = set.ToArray();
        Array.Sort(sorted, Order);
        return new(ImmutableCollectionsMarshal.AsImmutableArray(sorted));
    }

    /// <summary>
    /// Creates the request a manifest's <c>ui.bridge</c> section makes; a manifest without a UI or
    /// without bridge operations requests nothing. The manifest is expected to be valid.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="manifest"/> is null.</exception>
    /// <exception cref="ArgumentException">The bridge section holds a declaration that validation would reject.</exception>
    public static AppUiBridgeRequest FromManifest(AppManifest manifest)
    {
        ArgumentNullException.ThrowIfNull(manifest);
        if (manifest.Ui?.Bridge is not { Length: > 0 } bridge)
            return Empty;
        List<AppUiBridgeGrant> grants = [];
        foreach (var declaration in bridge)
        {
            if (declaration is null)
                throw new ArgumentException("A bridge declaration cannot be null.", nameof(manifest));
            if (declaration.Trees is null)
                grants.Add(new(declaration.Operation));
            else
                foreach (var tree in declaration.Trees)
                    grants.Add(new(declaration.Operation, tree));
        }
        return Create(grants);
    }

    /// <summary>
    /// Returns whether this request covers <paramref name="grant"/>: it holds the grant itself or,
    /// for a single-tree data grant, the same operation over every declared tree.
    /// </summary>
    public bool Covers(AppUiBridgeGrant grant)
    {
        if (IsEmpty)
            return false;
        return Grants.BinarySearch(grant, Order) >= 0 ||
               (grant.Tree is not null && Grants.BinarySearch(new(grant.Operation), Order) >= 0);
    }

    /// <summary>
    /// Returns the grants this request makes that <paramref name="consented"/> does not cover. A
    /// non-empty result means consent must be given again; widening a data operation from specific
    /// trees to every declared tree counts as an addition, and a removal never appears.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="consented"/> is null.</exception>
    public AppUiBridgeRequest AddedRelativeTo(AppUiBridgeRequest consented)
    {
        ArgumentNullException.ThrowIfNull(consented);
        if (IsEmpty)
            return Empty;
        ImmutableArray<AppUiBridgeGrant>.Builder? added = null;
        foreach (var grant in Grants)
            if (!consented.Covers(grant))
                (added ??= ImmutableArray.CreateBuilder<AppUiBridgeGrant>()).Add(grant);
        // A subset of a normalised, sorted request is itself normalised and sorted.
        return added is null ? Empty : new(added.ToImmutable());
    }

    /// <summary>Returns whether both requests hold exactly the same normalised grants.</summary>
    public bool Equals(AppUiBridgeRequest? other) =>
        other is not null && (ReferenceEquals(this, other) || Grants.AsSpan().SequenceEqual(other.Grants.AsSpan()));

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as AppUiBridgeRequest);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        HashCode hash = new();
        foreach (var grant in Grants.AsSpan())
            hash.Add(grant);
        return hash.ToHashCode();
    }
}
