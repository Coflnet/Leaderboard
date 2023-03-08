using Coflnet.Scoreboard.Models;
using Coflnet.Scoreboard.Services;
using Microsoft.AspNetCore.Mvc;

namespace Coflnet.Scoreboard;

[ApiController]
[Route("[controller]")]
public class ScoresController : ControllerBase
{
    private readonly ILogger<ScoresController> _logger;
    private readonly ScoreboardService service;

    public ScoresController(ILogger<ScoresController> logger, ScoreboardService service)
    {
        _logger = logger;
        this.service = service;
    }
    /// <summary>
    /// Add a score to the scoreboard
    /// </summary>
    /// <param name="scoreBoardSlug"></param>
    /// <param name="userId"></param>
    /// <param name="score"></param>
    /// <returns></returns>
    [Route("{scoreBoardSlug}")]
    [HttpPost]
    public async Task AddScore(string scoreBoardSlug, string userId, int score)
    {
        await service.AddScore(scoreBoardSlug, userId, score, 100);
    }

    [Route("{scoreBoardSlug}/mock")]
    [HttpPost]
    public async Task AddMockScore(string scoreBoardSlug, int start = 0)
    {
        for (int i = start; i < start + 2000; i++)
        {
            await service.AddScore(scoreBoardSlug, i.ToString(), i, 1);
        }
    }
    /// <summary>
    /// Get the scoreboard for a specific slug
    /// </summary>
    /// <param name="scoreBoardSlug"></param>
    /// <param name="userId"></param>
    /// <returns></returns>
    [Route("{scoreBoardSlug}")]
    [HttpGet]
    public async Task<IEnumerable<BoardScore>> GetScoreboard(string scoreBoardSlug, string userId)
    {
        return await service.GetScoresAround(scoreBoardSlug, userId);
    }

    /// <summary>
    /// GetOwnRank
    /// </summary>
    /// <param name="scoreBoardSlug"></param>
    /// <param name="userId"></param>
    /// <returns></returns>
    [Route("{scoreBoardSlug}/getOwnRank")]
    [HttpGet]
    public async Task<long> GetOwnRank(string scoreBoardSlug, string userId)
    {
        return await service.GetOwnRank(scoreBoardSlug, userId);
    }
}
