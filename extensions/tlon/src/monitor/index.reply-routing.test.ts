import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { join } from "node:path";
import {
  createTestRegistry,
  getActivePluginRegistry,
  setActivePluginRegistry,
} from "openclaw/plugin-sdk/plugin-test-runtime";
import { buildReplyPayloads } from "openclaw/plugin-sdk/reply-payload-testing";
import { withTempHome } from "openclaw/plugin-sdk/test-env";
import { afterEach, describe, expect, it, vi } from "vitest";
import { tlonPlugin } from "../channel.js";
import { useTlonMonitorFixture } from "./monitor.test-harness.js";

const {
  withMonitor,
  getSubscription,
  authenticateMock,
  settingsManagerMock,
  ingressMock,
  sseClientMock,
  inboundRuntimeMock,
  realUrbitFixture,
} = useTlonMonitorFixture();
const runningServers: Server[] = [];
afterEach(async () => {
  await Promise.all(
    runningServers.splice(0).map(
      (server) =>
        new Promise<void>((resolve) => {
          server.close(() => resolve());
          server.closeAllConnections();
        }),
    ),
  );
});

describe("monitorTlonProvider durable group target", () => {
  it.each([
    { name: "channel post", threadReply: false, directive: "", direct: false },
    { name: "thread root", threadReply: true, directive: "", direct: false },
    { name: "explicit target", threadReply: true, directive: "[[reply_to:9999]] ", direct: false },
    {
      name: "explicit current message",
      threadReply: true,
      directive: "[[reply_to_current]] ",
      direct: false,
    },
    { name: "direct message", threadReply: false, directive: "", direct: true },
  ])("preserves the $name through durable delivery", async ({ threadReply, directive, direct }) => {
    await withTempHome(async (home) => {
      const actual = await vi.importActual<typeof import("openclaw/plugin-sdk/channel-inbound")>(
        "openclaw/plugin-sdk/channel-inbound",
      );
      const pokes: unknown[] = [];
      const server = createServer((req, res) => {
        let body = "";
        req.setEncoding("utf8");
        req.on("data", (part: string) => {
          body += part;
        });
        req.on("end", () => {
          if (req.method === "PUT") {
            pokes.push(...JSON.parse(body));
          }
          res.writeHead(204);
          res.end();
        });
      });
      runningServers.push(server);
      await new Promise<void>((resolve) => {
        server.listen(0, "127.0.0.1", resolve);
      });
      const address = server.address() as AddressInfo;
      const channelNest = "chat/~zod/general";
      const replyId = directive.includes("9999") ? "5679" : directive ? "5680" : "5678";
      realUrbitFixture.config = {
        session: { store: join(home, ".openclaw", "agents", "main", "sessions", "sessions.json") },
        messages: { visibleReplies: "automatic" },
        channels: {
          tlon: {
            code: "code",
            ship: "~zod",
            ownerShip: "~nec",
            url: `http://127.0.0.1:${address.port}`,
            network: { dangerouslyAllowPrivateNetwork: true },
            groupChannels: [channelNest],
            showModelSignature: true,
          },
        },
      };
      authenticateMock.mockResolvedValue("urbauth-~zod=proof");
      settingsManagerMock.load.mockResolvedValue({});
      ingressMock.receive.mockResolvedValue({ kind: "ignored" });
      sseClientMock.scry.mockResolvedValue({});
      inboundRuntimeMock.buildContext.mockImplementation(actual.buildChannelInboundEventContext);
      inboundRuntimeMock.dispatch.mockImplementation((params) =>
        actual.dispatchChannelInboundTurn({
          ...params,
          // Replace only model output; retain production payload normalization and durable delivery.
          replyResolver: async (ctx) => {
            const { replyPayloads } = await buildReplyPayloads({
              config: params.cfg,
              payloads: [{ text: directive + "automatic answer" }],
              blockStreamingEnabled: false,
              blockReplyPipeline: null,
              replyToMode: "all",
              replyToChannel: "tlon",
              currentMessageId: ctx.MessageSidFull ?? ctx.MessageSid,
              replyThreading: ctx.ReplyThreading,
              originatingChannel: ctx.OriginatingChannel,
              originatingChatType: ctx.ChatType,
              originatingTo: ctx.OriginatingTo,
              accountId: ctx.AccountId,
            });
            return replyPayloads;
          },
        }),
      );
      const priorRegistry = getActivePluginRegistry();
      setActivePluginRegistry(
        createTestRegistry([{ pluginId: "tlon", source: "test", plugin: tlonPlugin }]),
      );
      try {
        await withMonitor(async (runtime) => {
          const subscription = getSubscription(direct ? "chat" : "channels");
          const essay = {
            author: "~nec",
            content: [{ inline: ["~zod hello"] }],
            sent: 1700000000000,
          };
          await subscription.event(
            direct
              ? {
                  whom: "~nec",
                  id: "dm-1234",
                  response: { add: { essay } },
                }
              : {
                  nest: channelNest,
                  response: {
                    post: {
                      id: "1234",
                      "r-post": threadReply
                        ? {
                            reply: {
                              id: replyId,
                              "r-reply": { set: { memo: essay, seal: { "parent-id": "1234" } } },
                            },
                          }
                        : { set: { essay, seal: { id: "1234" } } },
                    },
                  },
                },
          );
          expect(inboundRuntimeMock.dispatch).toHaveBeenCalledOnce();
          expect(runtime.error).not.toHaveBeenCalled();
          const sends = pokes.filter(
            (poke) =>
              typeof poke === "object" &&
              poke !== null &&
              "mark" in poke &&
              poke.mark === (direct ? "chat-dm-action" : "channel-action-1"),
          );
          expect(sends).toHaveLength(1);
          if (direct) {
            expect(sends[0]).toMatchObject({ json: { ship: "~nec" } });
            return;
          }
          expect(sends[0]).toMatchObject({
            json: {
              channel: {
                nest: channelNest,
                action: {
                  post: threadReply
                    ? {
                        reply: {
                          id: directive.includes("9999") ? "9.999" : directive ? "5.680" : "1.234",
                          action: { add: { author: "~zod" } },
                        },
                      }
                    : { add: { author: "~zod", kind: "/chat" } },
                },
              },
            },
          });
        });
      } finally {
        setActivePluginRegistry(priorRegistry);
      }
    });
  });
});
