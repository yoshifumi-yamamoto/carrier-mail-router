/**
 * DHL / FedEx のメールを検知して Chatwork に通知する（返信も通知 / 429対策）
 * - 重複通知防止: Gmail Message ID を ScriptProperties に保存（返信は別IDなので通知される）
 * - 同時実行ロック: LockService
 * - 件名で分類してタイトルを振り分け
 * - 429対策: 1回の実行で1投稿にまとめて送る（バッチ）
 */

// ====== ここだけ設定 ======
const CHATWORK_API_TOKEN = 'a5dea6686afa054aa28913cad677122c';
const CHATWORK_ROOM_ID = '421984269';
const TRACKING_ROOM_ID = '345267509';

const BASE_QUERY = [
  '(from:(@dhl.com OR @dhl.de OR @dhl.co.jp OR @dpdhl.com) OR from:(@fedex.com OR @fedex.co.jp))',
  'newer_than:14d',
].join(' ');

const SNIPPET_MAX = 160;

// ScriptProperties に保存するキー
const NOTIFIED_KEY = 'CW_NOTIFIED_MESSAGE_IDS_V2';

// 保存肥大化対策（保持上限）
const NOTIFIED_MAX_KEYS = 4000;

// 1回の実行で拾う上限（まとめるけど、拾いすぎ防止）
const MAX_COLLECT_PER_RUN = 30;

// スプレッドシート設定
// このスクリプトがスプレッドシートに紐づいている前提
const SETTINGS_SHEET_NAME = '担当者';
const ROLE_INVOICE_CONFIRMED = '請求確定';

// Supabase（ScriptProperties に設定）
const SUPABASE_URL_PROP = 'SUPABASE_URL';
const SUPABASE_SERVICE_ROLE_KEY_PROP = 'SUPABASE_SERVICE_ROLE_KEY';
const SUPABASE_ORDERS_TABLE = 'orders';


// ====== メイン ======
function notifyCarrierEmailsToChatwork() {
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(25000)) return;

  const runId = new Date().toISOString();
  console.log(`[run ${runId}] start`);

  try {
    const props = PropertiesService.getScriptProperties();
    const notified = loadNotifiedMap_(props); // { [messageId]: timestampNumber }
    console.log(`[run ${runId}] notified loaded: ${Object.keys(notified).length}`);

    const threads = GmailApp.search(BASE_QUERY, 0, 30);
    console.log(`[run ${runId}] threads: ${threads.length}`);
    if (!threads.length) return;

    const items = [];
    let collected = 0;

    // 収集：新しい順に拾う
    for (const thread of threads) {
      const messages = thread.getMessages();

      for (let i = messages.length - 1; i >= 0; i--) {
        const msg = messages[i];
        const msgId = msg.getId();

        if (notified[msgId]) {
          console.log(`[run ${runId}] skip notified msgId=${msgId}`);
          continue;
        }

        const from = msg.getFrom() || '';
        const subject = msg.getSubject() || '';
        const date = msg.getDate();

        if (isOlderThanDays_(date, 14)) continue;
        if (
          subject.includes('DHL集荷確認のお知らせ') ||
          subject.includes('DHL集荷予約の確認') ||
          subject.includes('梱包資材のご注文について') ||
          subject.includes('注文の進捗')
        ) {
          console.log(`[run ${runId}] skip pickup-confirmation subject msgId=${msgId}`);
          continue;
        }

        let bodyText = '';
        try {
          bodyText = msg.getPlainBody() || '';
        } catch (e) {
          bodyText = '';
        }
        let fullBody = bodyText.replace(/\r\n/g, '\n').trim();
        const link = `https://mail.google.com/mail/u/0/#all/${msgId}`;

        const cls = classifyCarrierEmail_(from, subject);
        if (cls.carrier === 'DHL') {
          fullBody = trimDhlFooter_(fullBody);
        }
        const trackingNumbers = extractTrackingNumbers_(subject, bodyText, cls.carrier);

        items.push({
          msgId,
          carrier: cls.carrier,
          title: cls.title,
          bucket: cls.bucket,
          from,
          subject,
          date,
          fullBody,
          link,
          trackingNumbers,
        });

        // ここではまだ notified を確定しない（投稿成功後に確定）
        collected++;
        if (collected >= MAX_COLLECT_PER_RUN) break;
      }
      if (collected >= MAX_COLLECT_PER_RUN) break;
    }

    if (!items.length) return;
    console.log(`[run ${runId}] items collected: ${items.length}`);
    console.log(`[run ${runId}] item msgIds: ${items.map(it => it.msgId).join(',')}`);

    // 追跡番号から対象アカウントを引く（失敗しても通知は継続）
    let lookup = { usersByTracking: {}, ordersByTracking: {}, lookupFailed: false };
    try {
      lookup = fetchEbayUserIdsByTrackingNumbers_(items);
    } catch (e) {
      console.error('Supabase lookup failed:', e && e.message ? e.message : e);
      lookup = { usersByTracking: {}, ordersByTracking: {}, lookupFailed: true };
    }
    console.log(`[run ${runId}] trackingToUsers keys: ${Object.keys(lookup.usersByTracking).length}`);
    console.log(`[run ${runId}] trackingToOrders keys: ${Object.keys(lookup.ordersByTracking).length}`);

    // 1通にまとめる（429対策）
    const trackingItems = items.filter(it => (it.trackingNumbers || []).length > 0);
    const otherItems = items.filter(it => (it.trackingNumbers || []).length === 0);

    if (trackingItems.length) {
      const body = buildChatworkBody_(trackingItems, lookup, '追跡番号付き');
      console.log(`[run ${runId}] posting to chatwork (room=${TRACKING_ROOM_ID})`);
      postToChatworkWithRetry_(TRACKING_ROOM_ID, body);
      console.log(`[run ${runId}] chatwork post ok (tracking room)`);
    }

    if (otherItems.length) {
      const body = buildChatworkBody_(otherItems, lookup, 'その他');
      console.log(`[run ${runId}] posting to chatwork (room=${CHATWORK_ROOM_ID})`);
      postToChatworkWithRetry_(CHATWORK_ROOM_ID, body);
      console.log(`[run ${runId}] chatwork post ok (default room)`);
    }

    // ★請求確定はタスク化（失敗しても通知済みは保存）
    try {
      const created = createTasksForInvoiceConfirmed_(items);
      if (created) {
        console.log(`[run ${runId}] task creation ok`);
      } else {
        console.warn(`[run ${runId}] task creation skipped`);
      }
    } catch (e) {
      console.error('Chatwork task creation failed:', e && e.message ? e.message : e);
    }

    // ★eBayアカウントIDの担当者にタスク作成
    try {
      const created = createTasksForEbayUsers_(items, lookup.usersByTracking);
      if (created) {
        console.log(`[run ${runId}] user task creation ok`);
      } else {
        console.warn(`[run ${runId}] user task creation skipped`);
      }
    } catch (e) {
      console.error('Chatwork user task creation failed:', e && e.message ? e.message : e);
    }

    // ★送信成功後に通知済み確定
    const now = Date.now();
    for (const it of items) {
      notified[it.msgId] = now;
    }
    trimNotifiedMap_(notified, NOTIFIED_MAX_KEYS);
    const serialized = JSON.stringify(notified);
    console.log(`[run ${runId}] saving notified: ${Object.keys(notified).length}, bytes=${serialized.length}`);
    try {
      props.setProperty(NOTIFIED_KEY, serialized);
      console.log(`[run ${runId}] notified saved ok`);
    } catch (e) {
      console.error(`[run ${runId}] notified save failed:`, e && e.message ? e.message : e);
    }

  } finally {
    lock.releaseLock();
    console.log(`[run ${runId}] end`);
  }
}


// ====== 分類ロジック（そのまま） ======
function classifyCarrierEmail_(from, subject) {
  const f = (from || '').toLowerCase();
  const sj = subject || '';
  const s = sj.toLowerCase();

  const isFedEx = f.includes('fedex') || s.includes('fedex');
  const isDHL = f.includes('dhl') || f.includes('dpdhl') || s.includes('dhl');

  const includesAnyLower = (lowerText, arr) => arr.some(k => lowerText.includes(k));
  const includesAnyJP = (text, arr) => arr.some(k => text.indexOf(k) !== -1);

  if (isFedEx) {
    if (includesAnyJP(sj, ['トランザクションは失敗しました'])) {
      return { carrier: 'FedEx', title: '【支払い失敗】要対応', bucket: 'payment_failed' };
    }
    if (includesAnyJP(sj, ['フェデックス　ビリング　オンライン', 'フェデックス ビリング オンライン', '請求書発行のお知らせ'])) {
      return { carrier: 'FedEx', title: '【請求確定】CSV取込', bucket: 'invoice_confirmed' };
    }
    if (includesAnyLower(s, ['awb'])) {
      return { carrier: 'FedEx', title: '【要調査】運送状/AWB', bucket: 'awb_inquiry' };
    }
    return { carrier: 'FedEx', title: '【その他】確認のみ', bucket: 'other' };
  }

  if (isDHL) {
    if (
      (includesAnyJP(sj, ['DHL MyBill']) && includesAnyJP(sj, ['カード決済エラー'])) ||
      includesAnyLower(s, ['payment failed notification'])
    ) {
      return { carrier: 'DHL', title: '【支払い失敗】要対応', bucket: 'payment_failed' };
    }
    if (includesAnyLower(s, ['your latest dhl invoice:'])) {
      return { carrier: 'DHL', title: '【請求確定】CSV取込', bucket: 'invoice_confirmed' };
    }
    if (includesAnyJP(sj, ['請求書発行のお知らせ'])) {
      return { carrier: 'DHL', title: '【請求確定】CSV取込', bucket: 'invoice_confirmed' };
    }
    if (includesAnyLower(s, ['awb']) || includesAnyJP(sj, ['運送状番号', '送り状番号'])) {
      return { carrier: 'DHL', title: '【要調査】運送状/AWB', bucket: 'awb_inquiry' };
    }
    return { carrier: 'DHL', title: '【その他】確認のみ', bucket: 'other' };
  }

  return { carrier: 'Other', title: '【その他】確認のみ', bucket: 'other' };
}

// ====== 追跡番号抽出 ======
function extractTrackingNumbers_(subject, bodyText, carrier) {
  const rawText = [subject || '', bodyText || ''].join('\n');
  // URL 内の数値は請求リンク等の誤検知になりやすいため除外
  const text = rawText.replace(/https?:\/\/\S+/gi, ' ');
  const results = [];

  if (carrier === 'FedEx') {
    // FedEx: 12〜15桁（代表: 12/15桁）
    const fedex = text.match(/\b\d{12,15}\b/g);
    if (fedex) results.push(...fedex);
  }

  if (carrier === 'DHL') {
    // DHL 10桁は追跡系キーワードがある行のみ抽出（請求番号/口座番号の誤検知を抑制）
    const lines = text.split('\n');
    const dhlHint = /(tracking|track|awb|waybill|shipment|運送状|送り状|追跡|tracking number)/i;
    for (const line of lines) {
      if (!dhlHint.test(line)) continue;
      const matched = line.match(/\b\d{10}\b/g);
      if (matched) results.push(...matched);
    }
  }

  // 重複排除
  return Array.from(new Set(results));
}

// ====== Supabase 連携 ======
function fetchEbayUserIdsByTrackingNumbers_(items) {
  const cfg = getSupabaseConfig_();
  if (!cfg) return { usersByTracking: {}, ordersByTracking: {}, lookupFailed: true };

  const all = [];
  for (const it of items) {
    if (it.trackingNumbers && it.trackingNumbers.length) {
      all.push(...it.trackingNumbers);
    }
  }
  const uniqueTracking = Array.from(new Set(all));
  if (!uniqueTracking.length) return { usersByTracking: {}, ordersByTracking: {}, lookupFailed: false };

  const usersByTracking = {};
  const ordersByTracking = {};
  const batchSize = 100;
  for (let i = 0; i < uniqueTracking.length; i += batchSize) {
    const batch = uniqueTracking.slice(i, i + batchSize);
    const rows = querySupabaseOrdersByTracking_(cfg, batch);
    for (const row of rows) {
      const tn = String(row.shipping_tracking_number || '').trim();
      const user = String(row.ebay_user_id || '').trim();
      const orderNo = String(row.order_no || '').trim();
      if (tn && user) {
        if (!usersByTracking[tn]) usersByTracking[tn] = [];
        usersByTracking[tn].push(user);
      }
      if (tn && orderNo) {
        if (!ordersByTracking[tn]) ordersByTracking[tn] = [];
        ordersByTracking[tn].push(orderNo);
      }
    }
  }
  return { usersByTracking, ordersByTracking, lookupFailed: false };
}

function getSupabaseConfig_() {
  const props = PropertiesService.getScriptProperties();
  const url = (props.getProperty(SUPABASE_URL_PROP) || '').trim();
  const key = (props.getProperty(SUPABASE_SERVICE_ROLE_KEY_PROP) || '').trim();
  if (!url || !key) return null;
  return { url, key };
}

function querySupabaseOrdersByTracking_(cfg, trackingNumbers) {
  const list = trackingNumbers
    .map(t => String(t).trim())
    .filter(Boolean)
    .join(',');
  if (!list) return [];

  const filter = `in.(${list})`;
  const endpoint = `${cfg.url}/rest/v1/${SUPABASE_ORDERS_TABLE}`
    + `?select=shipping_tracking_number,ebay_user_id,order_no`
    + `&shipping_tracking_number=${encodeURIComponent(filter)}`;

  const res = UrlFetchApp.fetch(endpoint, {
    method: 'get',
    headers: {
      apikey: cfg.key,
      Authorization: `Bearer ${cfg.key}`,
    },
    muteHttpExceptions: true,
  });

  const code = res.getResponseCode();
  if (code < 200 || code >= 300) {
    throw new Error(`Supabase API error: ${code} ${res.getContentText()}`);
  }

  const text = res.getContentText();
  if (!text) return [];
  try {
    const data = JSON.parse(text);
    return Array.isArray(data) ? data : [];
  } catch (e) {
    return [];
  }
}


// ====== Chatwork送信（429リトライ） ======
function postToChatworkWithRetry_(roomId, message) {
  const maxRetries = 5;
  let waitMs = 1500;

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    const res = postToChatworkRaw_(roomId, message);
    const code = res.getResponseCode();

    if (code >= 200 && code < 300) return;

    // 429のみバックオフしてリトライ
    if (code === 429 && attempt < maxRetries) {
      Utilities.sleep(waitMs);
      waitMs *= 2;
      continue;
    }

    throw new Error(`Chatwork API error: ${code} ${res.getContentText()}`);
  }
}

function postToChatworkRaw_(roomId, message) {
  const url = `https://api.chatwork.com/v2/rooms/${roomId}/messages`;
  return UrlFetchApp.fetch(url, {
    method: 'post',
    headers: { 'X-ChatWorkToken': CHATWORK_API_TOKEN },
    payload: { body: message },
    muteHttpExceptions: true,
  });
}

// ====== Chatworkタスク作成 ======
function createTasksForInvoiceConfirmed_(items) {
  const targetItems = items.filter(it => it.bucket === 'invoice_confirmed');
  if (!targetItems.length) return false;

  const assignee = getAssigneeIdByRole_(ROLE_INVOICE_CONFIRMED);
  console.log(`[task] assignee role=${ROLE_INVOICE_CONFIRMED} id=${assignee}`);
  if (!assignee) return false;

  const limitTs = Math.floor((Date.now() + 3 * 24 * 60 * 60 * 1000) / 1000);

  for (const it of targetItems) {
    const taskBody = [
      `【請求確定】${it.carrier}`,
      `Subject: ${it.subject}`,
      `From: ${it.from}`,
      `Gmail: ${it.link}`,
    ].join('\n');
    const res = createChatworkTask_(CHATWORK_ROOM_ID, taskBody, assignee, limitTs);
    console.log(`[task] create response code=${res.getResponseCode()} body=${res.getContentText()}`);
  }
  return true;
}

function createChatworkTask_(roomId, body, toIds, limitTs) {
  const url = `https://api.chatwork.com/v2/rooms/${roomId}/tasks`;
  return UrlFetchApp.fetch(url, {
    method: 'post',
    headers: { 'X-ChatWorkToken': CHATWORK_API_TOKEN },
    payload: {
      body,
      to_ids: String(toIds),
      limit: String(limitTs),
    },
    muteHttpExceptions: true,
  });
}

function createTasksForEbayUsers_(items, usersByTracking) {
  if (!items || !items.length) return false;
  const limitTs = Math.floor((Date.now() + 3 * 24 * 60 * 60 * 1000) / 1000);
  let createdAny = false;

  for (const it of items) {
    const tns = it.trackingNumbers || [];
    const users = tns.flatMap(tn => usersByTracking[tn] || []);
    const uniqueUsers = Array.from(new Set(users)).filter(Boolean);
    if (!uniqueUsers.length) continue;

    for (const userId of uniqueUsers) {
      const assignee = getAssigneeIdByRole_(userId);
      console.log(`[user-task] ebay_user_id=${userId} assignee=${assignee}`);
      if (!assignee) continue;

      const taskBody = [
        `【キャリアメール】${it.carrier}`,
        `eBay: ${userId}`,
        `Subject: ${it.subject}`,
        `From: ${it.from}`,
        `Gmail: ${it.link}`,
      ].join('\n');
      const res = createChatworkTask_(CHATWORK_ROOM_ID, taskBody, assignee, limitTs);
      console.log(`[user-task] create response code=${res.getResponseCode()} body=${res.getContentText()}`);
      createdAny = true;
    }
  }

  return createdAny;
}

// ====== 設定シート ======
function getAssigneeIdByRole_(role) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  if (!ss) return '';
  const sheet = ss.getSheetByName(SETTINGS_SHEET_NAME);
  if (!sheet) return '';

  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return '';

  const values = sheet.getRange(2, 1, lastRow - 1, 2).getValues();
  for (const [r, id] of values) {
    if (String(r).trim() === role) return String(id).trim();
  }
  return '';
}


// ====== ScriptProperties（通知済みID） ======
function loadNotifiedMap_(props) {
  const raw = props.getProperty(NOTIFIED_KEY);
  if (!raw) return {};
  try {
    const obj = JSON.parse(raw);
    return (obj && typeof obj === 'object') ? obj : {};
  } catch (e) {
    return {};
  }
}

function trimNotifiedMap_(mapObj, maxKeys) {
  const keys = Object.keys(mapObj);
  if (keys.length <= maxKeys) return;

  keys.sort((a, b) => (mapObj[a] || 0) - (mapObj[b] || 0));
  const removeCount = keys.length - maxKeys;
  for (let i = 0; i < removeCount; i++) delete mapObj[keys[i]];
}

function isOlderThanDays_(dateObj, days) {
  const ms = days * 24 * 60 * 60 * 1000;
  return (Date.now() - new Date(dateObj).getTime()) > ms;
}


// ====== 表示崩れ防止 ======
function escapeForChatwork_(text) {
  return String(text).replace(/\[|\]/g, (m) => (m === '[' ? '［' : '］'));
}

function buildChatworkBody_(items, lookup, label) {
  const header = `[info][title]📦 キャリアメール新着 ${items.length}件（DHL/FedEx）｜${label}[/title]`;
  const blocks = items.map((it, idx) => {
    const tns = it.trackingNumbers || [];
    const users = tns.flatMap(tn => lookup.usersByTracking[tn] || []);
    const orders = tns.flatMap(tn => lookup.ordersByTracking[tn] || []);
    const uniqueUsers = Array.from(new Set(users)).filter(Boolean);
    const uniqueOrders = Array.from(new Set(orders)).filter(Boolean);

    return [
      `#${idx + 1} ${it.title}｜${it.carrier}`,
      `■ Subject: ${escapeForChatwork_(it.subject)}`,
      `■ From: ${escapeForChatwork_(it.from)}`,
      `■ Date: ${it.date}`,
      `■ Category: ${it.bucket}`,
      tns.length ? `■ Tracking: ${tns.join(', ')}` : '',
      uniqueUsers.length ? `■ 対象アカウント: ${uniqueUsers.join(', ')}` : '',
      (tns.length && !uniqueUsers.length)
        ? (lookup.lookupFailed
            ? '■ 対象アカウント: 検索失敗（Supabase連携エラー）'
            : '■ 対象アカウント: 見つかりませんでした')
        : '',
      uniqueOrders.length ? `■ Order: ${uniqueOrders.join(', ')}` : '',
      it.fullBody ? `■ Body: ${escapeForChatwork_(it.fullBody)}` : '',
      `■ Gmail: ${it.link}`,
    ].filter(Boolean).join('\n');
  });

  return [header, blocks.join('\n\n' + '―'.repeat(10) + '\n\n'), '[/info]'].join('\n\n');
}

function trimDhlFooter_(text) {
  if (!text) return '';
  const marker = /DHL\s*Express\s*[–-]\s*Excellence\.\s*Simply\s*delivered\./i;
  const lines = String(text).split('\n');
  for (let i = 0; i < lines.length; i++) {
    if (marker.test(lines[i])) {
      return lines.slice(0, i).join('\n').trim();
    }
  }
  return text;
}


// ====== 初回だけ実行：5分おきトリガー ======
function setupTimeTrigger() {
  const triggers = ScriptApp.getProjectTriggers();
  for (const t of triggers) {
    if (t.getHandlerFunction() === 'notifyCarrierEmailsToChatwork') return;
  }
  ScriptApp.newTrigger('notifyCarrierEmailsToChatwork')
    .timeBased()
    .everyMinutes(5)
    .create();
}


// ====== Chatwork疎通テスト ======
function testSendToChatwork() {
  postToChatworkWithRetry_(CHATWORK_ROOM_ID,
    '[info][title]🧪 テスト通知[/title]GAS→Chatworkの疎通テストです。[/info]'
  );
}
