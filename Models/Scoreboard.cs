using System;
using System.ComponentModel.DataAnnotations;
using Cassandra.Mapping.Attributes;

namespace Coflnet.Leaderboard.Models;

public class BoardScore
{
    [PartitionKey]
    [MaxLength(32)]
    public string Slug { get; set; }
    /// <summary>
    /// What bucket this score belongs to
    /// </summary>
    [ClusteringKey(0)]
    public long BucketId { get; set; }
    /// <summary>
    /// The value of the score
    /// </summary>
    [ClusteringKey(1)]
    public long Score { get; set; }
    /// <summary>
    /// The user who owns this score
    /// </summary>
    [MaxLength(32)]
    [SecondaryIndex]
    [ClusteringKey(2)]
    public string UserId { get; set; }
    /// <summary>
    /// How certain the inserter was that this is a legitmate score
    /// </summary>
    public short Confidence { get; set; }
    /// <summary>
    /// The time this score was inserted
    /// </summary>
    public DateTime TimeStamp { get; set; } = DateTime.Now;
}
