using System;

namespace Coflnet.Sky.Search.Models;


public class SearchTrackElem
{
    /// <summary>
    /// The input from the user
    /// </summary>
    public string Input { get; set; }
    /// <summary>
    /// If any the element the user clicked on
    /// </summary>
    public string UrlClicked { get; set; }
    /// <summary>
    /// The session Id of the user (used to find earlier inputs)
    /// </summary>
    [Cassandra.Mapping.Attributes.PartitionKey]
    public string SessionUuid { get; set; }
    /// <summary>
    /// When the search occured
    /// </summary>
    [Cassandra.Mapping.Attributes.ClusteringKey(0)]
    public DateTime TimeStamp { get; set; } = DateTime.Now;
}

public class SearchWeight
{
    /// <summary>
    /// The input from the user
    /// </summary>
    [Cassandra.Mapping.Attributes.PartitionKey]
    public string Input { get; set; }
    /// <summary>
    /// The url this weight applies to
    /// </summary>
    public string Url { get; set; }
    /// <summary>
    /// The weight of the input
    /// </summary>
    [Cassandra.Mapping.Attributes.ClusteringKey]
    public int Weight { get; set; }

    /// <summary>
    /// Last weight update
    /// </summary>
    public DateTime TimeStamp { get; set; } = DateTime.Now;
}

public class SearchSuggestion
{
    /// <summary>
    /// The input from the user
    /// </summary>
    [Cassandra.Mapping.Attributes.PartitionKey]
    public string Input { get; set; }
    /// <summary>
    /// The result data
    /// </summary>
    public string ResultJson { get; set; }
    /// <summary>
    /// Encoded image previews to display in the search result
    /// </summary>
    public string Base64Images { get; set; }
    /// <summary>
    /// When the search occured
    /// </summary>
    [Cassandra.Mapping.Attributes.ClusteringKey(0)]
    public DateTime UpdatedAt { get; set; } = DateTime.Now;
}