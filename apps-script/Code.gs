/**
 * SpecialSits — Gmail intake
 *
 * Runs on the Gmail account you forward special situations alerts / newsletters to.
 * Finds unprocessed threads, posts the email payload to your SpecialSits API, and
 * labels the thread "SpecialSits/Processed" so it's not sent again.
 *
 * Setup:
 *   1. Create a new Apps Script project at script.google.com.
 *   2. Paste this file in.
 *   3. Project Settings → Script Properties:
 *        API_BASE       = https://<your-railway-url>
 *        INGEST_TOKEN   = (same value as Railway env var INGEST_TOKEN)
 *   4. Run `setupLabels` once and grant permissions.
 *   5. Triggers → Add Trigger → processInbox, Time-driven, Every 15 minutes.
 */

const LABEL_NAME = 'SpecialSits/Processed';
const ERROR_LABEL = 'SpecialSits/Error';
const MAX_THREADS_PER_RUN = 30;

function setupLabels() {
  [LABEL_NAME, ERROR_LABEL].forEach(n => {
    if (!GmailApp.getUserLabelByName(n)) GmailApp.createLabel(n);
  });
}

function processInbox() {
  const props = PropertiesService.getScriptProperties();
  const API_BASE = props.getProperty('API_BASE');
  const TOKEN = props.getProperty('INGEST_TOKEN');
  if (!API_BASE || !TOKEN) throw new Error('Set API_BASE and INGEST_TOKEN script properties');

  setupLabels();
  const processedLabel = GmailApp.getUserLabelByName(LABEL_NAME);
  const errorLabel = GmailApp.getUserLabelByName(ERROR_LABEL);

  // Any inbox thread not already labeled Processed or Error.
  const query = `in:inbox -label:"${LABEL_NAME}" -label:"${ERROR_LABEL}"`;
  const threads = GmailApp.search(query, 0, MAX_THREADS_PER_RUN);

  for (const thread of threads) {
    const msg = thread.getMessages()[0];
    try {
      const body = msg.getPlainBody() || htmlToText(msg.getBody());
      const links = extractLinks(msg.getBody()).slice(0, 20);
      const payload = {
        fromAddress: msg.getFrom(),
        subject: msg.getSubject(),
        body: body.slice(0, 30000),
        receivedAt: msg.getDate().toISOString(),
        links: links,
      };
      const res = UrlFetchApp.fetch(API_BASE + '/api/ingest/email', {
        method: 'post',
        contentType: 'application/json',
        payload: JSON.stringify(payload),
        headers: { 'x-ingest-token': TOKEN },
        muteHttpExceptions: true,
      });
      if (res.getResponseCode() >= 200 && res.getResponseCode() < 300) {
        thread.addLabel(processedLabel);
      } else {
        Logger.log('Ingest error ' + res.getResponseCode() + ' ' + res.getContentText());
        thread.addLabel(errorLabel);
      }
    } catch (e) {
      Logger.log('Exception: ' + e.message);
      thread.addLabel(errorLabel);
    }
  }
}

function htmlToText(html) {
  return String(html || '').replace(/<style[\s\S]*?<\/style>/gi, '')
    .replace(/<script[\s\S]*?<\/script>/gi, '')
    .replace(/<[^>]+>/g, ' ').replace(/&nbsp;/g, ' ').replace(/\s+/g, ' ').trim();
}

function extractLinks(html) {
  const urls = new Set();
  const re = /href\s*=\s*"([^"]+)"/gi;
  let m;
  while ((m = re.exec(html)) !== null) {
    const u = m[1];
    if (/^https?:\/\//.test(u) && !/unsubscribe|preferences|mailto:/i.test(u)) urls.add(u);
  }
  return Array.from(urls);
}
