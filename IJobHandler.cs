﻿using System;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace HangfireJobHandler
{
    public interface IJobHandler
    {
        void CleanupHangingTasks();

        /// <summary>
        /// Deleting job id from queue
        /// </summary>
        /// <param name="jobId">Your custom job Id</param>
        Task DeleteJobFromQueueAsync(string jobId);

        /// <summary>
        /// Deleting job from queue
        /// </summary>
        /// <param name="jobId">Your custom job Id</param>
        /// <param name="jobRef">Hangfire job reference</param>
        Task DeleteJobFromQueueAsync(string jobId, string jobRef);

        /// <summary>
        /// Trying to enqueue job from expression with your custom ID if it's not being processed already.
        /// </summary>
        /// <param name="jobId">Your custom job Id</param>
        /// <param name="expression">Expression of the method to enqueue</param>
        /// <param name="delay">Delay in minutes</param>
        /// <returns></returns>
        Task<bool> TryEnqueueJobAsync(string jobId, Expression<Func<Task>> expression, int delay = 0);
    }
}
