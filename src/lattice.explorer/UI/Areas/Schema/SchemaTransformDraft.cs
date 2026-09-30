using System.Globalization;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>
/// The remediation editor's transform: an ordered list of member steps, and a
/// builder for the next one. The steps become one pass-through transform, so
/// every member a step does not name is kept as it is. Mutable, owned by one
/// editor.
/// </summary>
internal sealed class SchemaTransformDraft
{
    private readonly List<SchemaTransformStep> _steps = [];

    /// <summary>The steps, in the order they run.</summary>
    public IReadOnlyList<SchemaTransformStep> Steps => _steps;

    /// <summary>The kind of the step being written.</summary>
    public SchemaTransformStepKind Kind { get; set; } = SchemaTransformStepKind.Set;

    /// <summary>The member the step being written acts on.</summary>
    public string Path { get; set; } = string.Empty;

    /// <summary>For a rename, the member's new name.</summary>
    public string ToPath { get; set; } = string.Empty;

    /// <summary>For a set, the kind of constant.</summary>
    public SchemaConstantKind ValueKind { get; set; } = SchemaConstantKind.Text;

    /// <summary>For a set, the constant as typed.</summary>
    public string Value { get; set; } = string.Empty;

    /// <summary>Adds the step being written, or explains why it cannot be added.</summary>
    /// <param name="error">Why it cannot, as a plain sentence.</param>
    /// <returns><see langword="true"/> when the step was added.</returns>
    public bool TryAdd(out string? error)
    {
        error = null;
        var path = Path.Trim();
        if (path.Length == 0)
        {
            error = "Enter the member the step acts on.";
            return false;
        }

        switch (Kind)
        {
            case SchemaTransformStepKind.Rename:
                var to = ToPath.Trim();
                if (to.Length == 0)
                {
                    error = "Enter the member's new name.";
                    return false;
                }

                if (string.Equals(to, path, StringComparison.Ordinal))
                {
                    error = "A member cannot be renamed to its own name.";
                    return false;
                }

                _steps.Add(new SchemaTransformStep(Kind, path, to, SchemaConstantKind.Text, null));
                break;

            case SchemaTransformStepKind.Remove:
                _steps.Add(new SchemaTransformStep(Kind, path, null, SchemaConstantKind.Text, null));
                break;

            default:
                if (!TryConstant(ValueKind, Value, out _, out error))
                {
                    return false;
                }

                var stored = ValueKind switch
                {
                    SchemaConstantKind.Null => null,
                    SchemaConstantKind.Text => Value,
                    _ => Value.Trim(),
                };
                _steps.Add(new SchemaTransformStep(Kind, path, null, ValueKind, stored));
                break;
        }

        Path = string.Empty;
        ToPath = string.Empty;
        Value = string.Empty;
        return true;
    }

    /// <summary>Removes the step at <paramref name="index"/>; an index out of range is ignored.</summary>
    /// <param name="index">The zero-based step index.</param>
    public void RemoveAt(int index)
    {
        if (index >= 0 && index < _steps.Count)
        {
            _steps.RemoveAt(index);
        }
    }

    /// <summary>Removes every step and clears the builder.</summary>
    public void Clear()
    {
        _steps.Clear();
        Kind = SchemaTransformStepKind.Set;
        Path = string.Empty;
        ToPath = string.Empty;
        ValueKind = SchemaConstantKind.Text;
        Value = string.Empty;
    }

    /// <summary>Builds the transform the steps describe, or explains why there is none.</summary>
    /// <param name="transform">The transform, when there is one.</param>
    /// <param name="error">Why there is none, as a plain sentence.</param>
    /// <returns><see langword="true"/> when the transform was built.</returns>
    public bool TryBuild(out LatticeValueTransform transform, out string? error)
    {
        transform = default;
        error = null;
        if (_steps.Count == 0)
        {
            error = "Add at least one step.";
            return false;
        }

        var operations = new LatticeValueTransform[_steps.Count];
        for (var i = 0; i < _steps.Count; i++)
        {
            var step = _steps[i];
            switch (step.Kind)
            {
                case SchemaTransformStepKind.Remove:
                    operations[i] = LatticeValueTransform.DropMember(step.Path);
                    break;

                case SchemaTransformStepKind.Rename:
                    operations[i] = LatticeValueTransform.RenameMember(step.Path, step.ToPath!);
                    break;

                default:
                    if (!TryConstant(step.ValueKind, step.Value ?? string.Empty, out var constant, out error))
                    {
                        return false;
                    }

                    operations[i] = LatticeValueTransform.SetMember(step.Path, LatticeValueTransform.Const(constant));
                    break;
            }
        }

        transform = LatticeValueTransform.Passthrough(operations);
        return true;
    }

    /// <summary>Parses a constant as a set step writes it.</summary>
    /// <param name="kind">The kind of constant.</param>
    /// <param name="text">The constant as typed.</param>
    /// <param name="constant">The constant, when it parses.</param>
    /// <param name="error">Why it does not, as a plain sentence.</param>
    /// <returns><see langword="true"/> when it parses.</returns>
    internal static bool TryConstant(SchemaConstantKind kind, string text, out LatticeConstant constant, out string? error)
    {
        error = null;
        constant = default;
        var value = text.Trim();
        switch (kind)
        {
            case SchemaConstantKind.Null:
                constant = LatticeConstant.Null();
                return true;

            case SchemaConstantKind.Boolean:
                if (bool.TryParse(value, out var flag))
                {
                    constant = LatticeConstant.Bool(flag);
                    return true;
                }

                error = "Enter true or false.";
                return false;

            case SchemaConstantKind.Number:
                if (long.TryParse(value, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var whole))
                {
                    constant = LatticeConstant.Integer(whole);
                    return true;
                }

                if (double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var real) && double.IsFinite(real))
                {
                    constant = LatticeConstant.Real(real);
                    return true;
                }

                error = "Enter a number, such as 42 or 2.5.";
                return false;

            default:
                constant = LatticeConstant.Text(text);
                return true;
        }
    }
}
