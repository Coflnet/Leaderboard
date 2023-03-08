using System.ComponentModel.DataAnnotations;

namespace Coflnet.Scoreboard.Models;

/// <summary>
/// A bucket is a group of scores that are grouped together to optimize performance.
/// </summary>
public class Bucket
{
    /// <summary>
    /// The bucket id
    /// </summary>
    [Cassandra.Mapping.Attributes.PartitionKey]
    [MaxLength(32)]
    public string Slug { get; set; }
    /// <summary>
    /// The How manyth bucket this is for the given slug
    /// </summary>
    public long BucketId { get; set; }
    /// <summary>
    /// How many scores are in this bucket
    /// </summary>
    //[Cassandra.Mapping.Attributes.Counter]
    //public short ScoreCount { get; set; }
    /// <summary>
    /// The minimum score of this bucket
    /// </summary>
    [Cassandra.Mapping.Attributes.ClusteringKey(0)]
    public long MinimumScore { get; set; }
    /// <summary>
    /// The offset of the first score in this bucket.
    /// Used to determine the range of scores in this bucket.
    /// (should)Equals the sum of all ScoreCount in buckets with lower BucketId.
    /// </summary>
  //  [Cassandra.Mapping.Attributes.Counter]
  //  public long First { get; set; }

    public Bucket(string slug, long bucketId)
    {
        Slug = slug;
        BucketId = bucketId;
    }

    public Bucket()
    {
    }
}
