import { v } from "convex/values";
import { mutation, query } from "./_generated/server";

export const upsert = mutation({
  args: {
    slackUserId: v.string(),
    slackUserName: v.string(),
    preferredChartTypes: v.optional(v.array(v.string())),
    preferredFolder: v.optional(v.string()),
    notes: v.optional(v.string()),
  },
  handler: async (ctx, args) => {
    const existing = await ctx.db
      .query("user_preferences")
      .withIndex("by_slack_user", (q) => q.eq("slackUserId", args.slackUserId))
      .first();

    if (existing) {
      const update: Record<string, unknown> = { lastSeenAt: Date.now() };
      if (args.slackUserName) update.slackUserName = args.slackUserName;
      if (args.preferredChartTypes !== undefined)
        update.preferredChartTypes = args.preferredChartTypes;
      if (args.preferredFolder !== undefined)
        update.preferredFolder = args.preferredFolder;
      if (args.notes !== undefined) update.notes = args.notes;
      await ctx.db.patch(existing._id, update);
      return existing._id;
    }

    return await ctx.db.insert("user_preferences", {
      ...args,
      lastSeenAt: Date.now(),
    });
  },
});

export const get = query({
  args: { slackUserId: v.string() },
  handler: async (ctx, args) => {
    return await ctx.db
      .query("user_preferences")
      .withIndex("by_slack_user", (q) => q.eq("slackUserId", args.slackUserId))
      .first();
  },
});
