import { v } from "convex/values";
import { mutation, query } from "./_generated/server";

export const add = mutation({
  args: {
    slackUserId: v.string(),
    slackChannel: v.optional(v.string()),
    threadTs: v.optional(v.string()),
    dashboardId: v.optional(v.string()),
    type: v.string(),
    message: v.string(),
  },
  handler: async (ctx, args) => {
    return await ctx.db.insert("feedback", {
      ...args,
      resolved: false,
      learningId: undefined,
      createdAt: Date.now(),
    });
  },
});

export const resolve = mutation({
  args: {
    id: v.id("feedback"),
    learningId: v.optional(v.id("learnings")),
  },
  handler: async (ctx, args) => {
    await ctx.db.patch(args.id, {
      resolved: true,
      learningId: args.learningId,
    });
  },
});

export const listUnresolved = query({
  args: { limit: v.optional(v.float64()) },
  handler: async (ctx, args) => {
    const limit = args.limit ?? 10;
    return await ctx.db
      .query("feedback")
      .withIndex("by_unresolved", (q) => q.eq("resolved", false))
      .order("desc")
      .take(limit);
  },
});
