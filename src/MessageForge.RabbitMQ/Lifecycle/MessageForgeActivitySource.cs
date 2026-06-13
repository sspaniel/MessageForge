using System.Diagnostics;

namespace MessageForge.RabbitMQ.Lifecycle;

/// <summary>
/// OpenTelemetry activity source for MessageForge lifecycle operations.
/// </summary>
public static class MessageForgeActivitySource
{
    /// <summary>
    /// The name of the <see cref="ActivitySource"/> used by MessageForge.
    /// </summary>
    public const string Name = "MessageForge";

    internal static readonly ActivitySource Instance = new(Name);
}
