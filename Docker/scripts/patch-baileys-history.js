// darnabt 2026-10-01 — Baileys 7.0.0-rc.9 decodes HistorySync.phoneNumberToLidMappings
// (the LID <-> phone pairs WhatsApp sends a newly linked device with its chat history)
// and then throws them away in processHistoryMessage. Without them every history chat the
// phone stores under a @lid arrives with no phone number. Keep them on the
// 'messaging-history.set' payload; whatsapp.baileys.service.ts stores and applies them.
const fs = require('fs');
const path = 'node_modules/baileys/lib/Utils/history.js';
let src = fs.readFileSync(path, 'utf8');
if (src.includes('phoneNumberToLidMappings')) {
  console.log('baileys history patch: already applied');
  process.exit(0);
}
const anchor = /syncType: item\.syncType,\s*progress: item\.progress\s*\n(\s*)\};/;
if (!anchor.test(src)) {
  console.error('baileys history patch: anchor not found in ' + path + ' (Baileys changed?)');
  process.exit(1);
}
src = src.replace(
  anchor,
  (_m, indent) =>
    'syncType: item.syncType,\n        progress: item.progress,\n        phoneNumberToLidMappings: item.phoneNumberToLidMappings || []\n' +
    indent +
    '};',
);
fs.writeFileSync(path, src);
console.log('baileys history patch: applied');
