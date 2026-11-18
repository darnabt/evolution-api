// darnabt 2026-11-18 — Baileys 7.0.0-rc.9 answers every incoming message it IGNORES (shouldIgnoreJid
// returns true) with a NACK 500: handleMessage in lib/Socket/messages-recv.js calls
// sendMessageAck(node, NACK_REASONS.UnhandledError). The bridge ignores status@broadcast whenever
// readStatus is off, so every contact status (story) is nacked, and WhatsApp ends the stream about
// every 50 min ("Stream Errored (ack)" class:"status") -> the number reconnects.
// Fix: an ignored status@broadcast message gets the same plain ack Baileys sends for a message it has
// handled (sendMessageAck(node), as at the missing-keys and newsletter branches of handleMessage).
// It is still ignored: no decrypt, no event, no contact update, no webhook, no database statement.
// Every other ignored jid (groups when groupsIgnore, newsletters, other broadcast lists) keeps the nack.
const fs = require('fs');
const path = 'node_modules/baileys/lib/Socket/messages-recv.js';
let src = fs.readFileSync(path, 'utf8');
if (src.includes('darnabt: ignored status is acked')) {
  console.log('baileys status-ack patch: already applied');
  process.exit(0);
}
const anchor =
  "logger.debug({ key: node.attrs.key }, 'ignored message');\n" +
  '            await sendMessageAck(node, NACK_REASONS.UnhandledError);';
if (src.split(anchor).length !== 2) {
  console.error('baileys status-ack patch: anchor not found exactly once in ' + path + ' (Baileys changed?)');
  process.exit(1);
}
src = src.replace(
  anchor,
  "logger.debug({ key: node.attrs.key }, 'ignored message');\n" +
    '            // darnabt: ignored status is acked, not nacked (a nack makes WhatsApp close the stream)\n' +
    "            if (node.attrs.from === 'status@broadcast') {\n" +
    '                await sendMessageAck(node);\n' +
    '            }\n' +
    '            else {\n' +
    '                await sendMessageAck(node, NACK_REASONS.UnhandledError);\n' +
    '            }',
);
fs.writeFileSync(path, src);
console.log('baileys status-ack patch: applied');
