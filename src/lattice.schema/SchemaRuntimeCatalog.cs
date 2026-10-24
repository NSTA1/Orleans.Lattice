using System.Text.Json;

namespace Orleans.Lattice.Schema;

internal static class SchemaRuntimeCatalog
{
    internal static LatticeSchemaRuntimeRegistration Merge(
        LatticeSchemaRuntimeRegistration current,
        LatticeSchemaRuntimeRegistration addition,
        ILatticeValueTransformRegistry transforms,
        bool enforceMergedLimit = true)
    {
        ValidateSize(addition);

        var descriptors = current.Descriptors.ToDictionary(d => (d.SchemaId, d.Version));
        foreach (var descriptor in addition.Descriptors)
        {
            if (descriptor.SchemaId == 0 || descriptor.Version == 0
                || string.IsNullOrWhiteSpace(descriptor.Name) || descriptor.Name.Length > 256)
                throw new ArgumentException("Runtime descriptors require positive schema/version ids and a name of at most 256 characters.");
            var key = (descriptor.SchemaId, descriptor.Version);
            if (descriptors.TryGetValue(key, out var existing) && existing != descriptor)
                throw new ArgumentException($"Schema {key.SchemaId} v{key.Version} is already registered with a different definition.");
            descriptors[key] = descriptor;
        }

        var upcasters = current.Upcasters.ToDictionary(u => (u.SchemaId, u.FromVersion));
        foreach (var hop in addition.Upcasters)
        {
            if (hop is null || hop.FromVersion == 0 || hop.ToVersion <= hop.FromVersion
                || !descriptors.ContainsKey((hop.SchemaId, hop.FromVersion))
                || !descriptors.ContainsKey((hop.SchemaId, hop.ToVersion)))
                throw new ArgumentException("Each migration hop must advance between two registered versions of the same schema.");
            if (hop.Transform.HasValue == (hop.TransformId is not null))
                throw new ArgumentException("Each migration hop requires exactly one declarative transform or stable host transform id.");
            if (hop.TransformId is { } id && (id.Length > 256 || !transforms.TryGet(id, out _)))
                throw new ArgumentException($"Transform id '{id}' is not installed on this host. Provision it on every serving silo first.");
            if (hop.Transform is { } transform)
            {
                if (transform.Kind != LatticeValueTransformKind.Passthrough)
                    throw new ArgumentException("A migration transform must have a Passthrough document root.");
                var nodes = 0;
                ValidateTransform(transform, 0, ref nodes);
            }
            var key = (hop.SchemaId, hop.FromVersion);
            if (upcasters.TryGetValue(key, out var existing) && existing != hop)
                throw new ArgumentException($"Schema {key.SchemaId} already has a different migration from v{key.FromVersion}.");
            upcasters[key] = hop;
        }
        if (enforceMergedLimit && (descriptors.Count > 128 || upcasters.Count > 128))
            throw new ArgumentException("A tree's runtime catalog is limited to 128 versions and 128 migration hops.");

        return new()
        {
            Descriptors = descriptors.Values.OrderBy(d => d.SchemaId).ThenBy(d => d.Version).ToArray(),
            Upcasters = upcasters.Values.OrderBy(u => u.SchemaId).ThenBy(u => u.FromVersion).ToArray(),
        };
    }

    internal static void ValidateSize(LatticeSchemaRuntimeRegistration registration)
    {
        ArgumentNullException.ThrowIfNull(registration);
        if (registration.Descriptors is null || registration.Upcasters is null
            || registration.Descriptors.Length > 128 || registration.Upcasters.Length > 128
            || SerializedSize(registration) > 262144)
            throw new ArgumentException("A runtime catalog is limited to 128 versions, 128 hops and 256 KiB.");
    }

    internal static LatticeSchemaRuntimeRegistration Snapshot(LatticeSchemaRuntimeRegistration registration) => registration with
    {
        Descriptors = [.. registration.Descriptors],
        Upcasters = registration.Upcasters.Select(h => h with
        {
            Transform = h.Transform is { } transform ? SnapshotTransform(transform) : null,
        }).ToArray(),
        SupportedTransformIds = [.. registration.SupportedTransformIds],
    };

    private static LatticeValueTransform SnapshotTransform(LatticeValueTransform transform) => transform with
    {
        Children = transform.Children?.Select(SnapshotTransform).ToArray(),
        Condition = SnapshotPredicate(transform.Condition),
    };

    private static LatticePredicateNode SnapshotPredicate(LatticePredicateNode predicate) => predicate with
    {
        Children = predicate.Children?.Select(SnapshotPredicate).ToArray(),
    };

    private static int SerializedSize(LatticeSchemaRuntimeRegistration registration)
    {
        try { return JsonSerializer.SerializeToUtf8Bytes(registration).Length; }
        catch (JsonException ex) { throw new ArgumentException("The runtime catalog is too deeply nested or contains a cycle.", ex); }
    }

    private static void ValidateTransform(LatticeValueTransform node, int depth, ref int count)
    {
        if (depth > 32 || ++count > 1024 || !Enum.IsDefined(node.Kind))
            throw new ArgumentException("A transform must use supported nodes, at most 32 levels and 1024 nodes.");
        var children = node.Children ?? [];
        var expected = node.Kind switch
        {
            LatticeValueTransformKind.SetMember => 1,
            LatticeValueTransformKind.Conditional => 2,
            LatticeValueTransformKind.Compute or LatticeValueTransformKind.Passthrough => -1,
            _ => 0,
        };
        if (expected >= 0 && children.Length != expected)
            throw new ArgumentException($"Invalid child count for transform node {node.Kind}.");
        if (node.Kind is LatticeValueTransformKind.SetMember or LatticeValueTransformKind.DropMember
            or LatticeValueTransformKind.RenameMember or LatticeValueTransformKind.Member
            && string.IsNullOrWhiteSpace(node.MemberPath))
            throw new ArgumentException("Member transform nodes require a member path.");
        if (node.Kind == LatticeValueTransformKind.RenameMember && string.IsNullOrWhiteSpace(node.ToPath))
            throw new ArgumentException("A rename transform requires a destination path.");
        if (node.Kind == LatticeValueTransformKind.Compute && (!Enum.IsDefined(node.ComputeOperator) || children.Length == 0))
            throw new ArgumentException("A compute transform requires a supported operator and operands.");
        if (node.Kind == LatticeValueTransformKind.Constant && (!Enum.IsDefined(node.Constant.Kind)
            || node.Constant.Kind == LatticeConstantKind.Double && !double.IsFinite(node.Constant.DoubleValue)))
            throw new ArgumentException("A constant must use a supported finite JSON value.");
        if (node.Kind == LatticeValueTransformKind.Conditional)
            ValidatePredicate(node.Condition, depth + 1, ref count);
        foreach (var child in children)
        {
            if (node.Kind == LatticeValueTransformKind.Passthrough)
            {
                if (child.Kind is not (LatticeValueTransformKind.SetMember
                    or LatticeValueTransformKind.DropMember or LatticeValueTransformKind.RenameMember))
                    throw new ArgumentException("A document pipeline accepts only set, drop and rename operations.");
            }
            else if (child.Kind is not (LatticeValueTransformKind.Member or LatticeValueTransformKind.Constant
                or LatticeValueTransformKind.Conditional or LatticeValueTransformKind.Compute))
                throw new ArgumentException("Transform operands must be value expressions.");
            ValidateTransform(child, depth + 1, ref count);
        }
    }

    private static void ValidatePredicate(LatticePredicateNode node, int depth, ref int count)
    {
        if (depth > 32 || ++count > 1024 || !Enum.IsDefined(node.Kind)
            || !Enum.IsDefined(node.ComparisonOperator) || !Enum.IsDefined(node.BooleanOperator)
            || !Enum.IsDefined(node.StringMethod) || !Enum.IsDefined(node.ValueKind) || !Enum.IsDefined(node.Constant.Kind))
            throw new ArgumentException("A conditional predicate contains unsupported or excessive nodes.");
        var children = node.Children ?? [];
        var expected = node.Kind switch
        {
            LatticePredicateNodeKind.Compare or LatticePredicateNodeKind.StringMethod => 2,
            LatticePredicateNodeKind.Every => 1,
            LatticePredicateNodeKind.Boolean when node.BooleanOperator == LatticeBooleanOperator.Not => 1,
            LatticePredicateNodeKind.Boolean => -1,
            _ => 0,
        };
        if (expected >= 0 && expected != children.Length || expected == -1 && children.Length == 0)
            throw new ArgumentException("A conditional predicate has an invalid operand count.");
        foreach (var child in children)
            ValidatePredicate(child, depth + 1, ref count);
    }

    internal static ILatticeSchemaRegistry Build(
        LatticeSchemaRuntimeRegistration registration, ILatticeValueTransformRegistry transforms)
    {
        var builder = new LatticeSchemaRegistryBuilder();
        foreach (var descriptor in registration.Descriptors)
            builder.AddSchema(descriptor.SchemaId, descriptor.Version, descriptor.Name);
        foreach (var hop in registration.Upcasters)
        {
            if (hop.Transform is { } transform)
                builder.AddUpcaster(hop.SchemaId, hop.FromVersion, hop.ToVersion, transform);
            else
                builder.AddUpcaster(hop.SchemaId, hop.FromVersion, hop.ToVersion, hop.TransformId!);
        }
        return builder.Build(transforms);
    }
}
