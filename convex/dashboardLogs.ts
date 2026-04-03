import { v } from "convex/values";
import { mutation, query } from "./_generated/server";

export const log = mutation({
  args: {
    slackUserId: v.optional(v.string()),
    slackChannel: v.optional(v.string()),
    threadTs: v.optional(v.string()),
    prompt: v.string(),
    status: v.string(),
    dashboardId: v.optional(v.string()),
    dashboardUrl: v.optional(v.string()),
    model: v.string(),
    toolCalls: v.float64(),
    durationMs: v.float64(),
    errorSummary: v.optional(v.string()),
    retryCount: v.float64(),
    tilesCreated: v.float64(),
    tilesWithData: v.float64(),
  },
  handler: async (ctx, args) => {
    return await ctx.db.insert("dashboard_logs", {
      ...args,
      createdAt: Date.now(),
    });
  },
});

// Recent logs for analytics
export const listRecent = query({
  args: { limit: v.optional(v.float64()) },
  handler: async (ctx, args) => {
    const limit = args.limit ?? 20;
    return await ctx.db
      .query("dashboard_logs")
      .withIndex("by_created")
      .order("desc")
      .take(limit);
  },
});

// Success rate stats
export const stats = query({
  args: {},
  handler: async (ctx) => {
    const all = await ctx.db.query("dashboard_logs").collect();
    const total = all.length;
    const success = all.filter((l) => l.status === "success").length;
    const partial = all.filter((l) => l.status === "partial").length;
    const fail = all.filter((l) => l.status === "fail").length;
    const avgRetries =
      total > 0
        ? all.reduce((sum, l) => sum + l.retryCount, 0) / total
        : 0;
    const avgDuration =
      total > 0
        ? all.reduce((sum, l) => sum + l.durationMs, 0) / total
        : 0;
    return {
      total,
      success,
      partial,
      fail,
      successRate: total > 0 ? ((success + partial) / total) * 100 : 0,
      avgRetries: Math.round(avgRetries * 10) / 10,
      avgDurationMs: Math.round(avgDuration),
    };
  },
});
