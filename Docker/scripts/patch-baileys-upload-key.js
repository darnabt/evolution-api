// darnabt 2026-11-20 F4 — Baileys 7.0.0-rc.9 lib/Utils/process-message.js:178-192 makes the LAST
// app-state key it receives our upload key (newAppStateSyncKeyId = strKeyId → creds.update
// {myAppStateKeyId}). A key shared by another companion (e.g. :76) must be STORED so we can read
// its records, but must never sign our uploads (the phone may not hold it → our mark read/unread
// patches fail to verify). Rule: the upload key changes only when the share came from the primary
// (device 0) or when we have no upload key yet.
// Part 1 (decode-wa-message.js): keep the sender jid (with device) on the message, non-enumerable
//   so it is never stored or serialised. Same object reaches processMessage (chats.js:789 → :842).
// Part 2 (process-message.js): gate the creds.update on that device.
const fs = require('fs');
const MARK = 'darnabt: upload key only from primary';

function patch(path, anchor, replacement) {
  let src = fs.readFileSync(path, 'utf8');
  if (src.includes(MARK)) return console.log('baileys upload-key patch: already applied in ' + path);
  if (src.split(anchor).length !== 2) {
    console.error('baileys upload-key patch: anchor not found exactly once in ' + path + ' (Baileys changed?)');
    process.exit(1);
  }
  fs.writeFileSync(path, src.replace(anchor, replacement));
  console.log('baileys upload-key patch: applied to ' + path);
}

patch(
  'node_modules/baileys/lib/Utils/decode-wa-message.js',
  '    if (key.fromMe) {\n        fullMessage.status = proto.WebMessageInfo.Status.SERVER_ACK;',
  '    // ' + MARK + ' (sender device kept for process-message)\n' +
    "    Object.defineProperty(fullMessage, 'darnabtAuthor', { value: author, enumerable: false, writable: true });\n" +
    '    if (key.fromMe) {\n        fullMessage.status = proto.WebMessageInfo.Status.SERVER_ACK;',
);

patch(
  'node_modules/baileys/lib/Utils/process-message.js',
  "                    ev.emit('creds.update', { myAppStateKeyId: newAppStateSyncKeyId });",
  '                    // ' + MARK + '\n' +
    '                    const darnabtDev = message.darnabtAuthor ? jidDecode(message.darnabtAuthor)?.device || 0 : null;\n' +
    '                    if (creds.myAppStateKeyId && darnabtDev !== 0) {\n' +
    "                        console.log('app-state upload key kept (' + creds.myAppStateKeyId + ') — key from device ' + (darnabtDev === null ? 'unknown' : darnabtDev) + ' stored for reading only');\n" +
    '                    }\n' +
    '                    else {\n' +
    "                        ev.emit('creds.update', { myAppStateKeyId: newAppStateSyncKeyId });\n" +
    '                    }',
);
