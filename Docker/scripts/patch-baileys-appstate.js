// darnabt 2026-10-01 — Baileys 7.0.0-rc.9 reports an app-state REMOVE mutation as if it were a
// change. When WhatsApp Web / the phone rewrites a record that was last written with another
// app-state key (index MAC differs per key), the patch carries SET <new value> + REMOVE <old
// record>; decodeSyncdMutations hands BOTH to onMutation, the mutation map is keyed by index,
// the REMOVE comes last, and Baileys emits the OLD value: "Mark as unread" on WhatsApp Web
// arrived as "read" (chats.update unreadCount 0) and the other way round. A REMOVE only retires
// a record — it is never the new state — so it must not be reported. LTHash handling is untouched.
const fs = require('fs');
const path = 'node_modules/baileys/lib/Utils/chat-utils.js';
let src = fs.readFileSync(path, 'utf8');
if (src.includes('darnabt: REMOVE is not a change')) {
  console.log('baileys app-state patch: already applied');
  process.exit(0);
}
const anchor = 'onMutation({ syncAction, index: JSON.parse(indexStr) });';
if (src.split(anchor).length !== 2) {
  console.error('baileys app-state patch: anchor not found exactly once in ' + path + ' (Baileys changed?)');
  process.exit(1);
}
src = src.replace(
  anchor,
  '// darnabt: REMOVE is not a change\n' +
    '        if (operation !== proto.SyncdMutation.SyncdOperation.REMOVE) {\n' +
    '            ' +
    anchor +
    '\n        }',
);
fs.writeFileSync(path, src);
console.log('baileys app-state patch: applied');
