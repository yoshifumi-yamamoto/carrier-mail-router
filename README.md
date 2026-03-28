# Carrier Mail Router (GAS)

DHL / FedEx のメールを検知して Chatwork に通知し、請求確定メールはタスク化します。

## スプレッドシート
このスプレッドシートに GAS を配置しています。

```
https://docs.google.com/spreadsheets/d/1j-nruqSt78JxYeFYaf7jD7Y5uvhwrM6e-97ESCGgUl0/edit?gid=0#gid=0
```

## 設定シート
A列に役割、B列に担当者IDを設定します。

例:
- A2: `請求確定`
- B2: `1234`

## メモ
- `index.js` の `SETTINGS_SPREADSHEET_ID` を設定してください。
- Chatwork の API トークンとルームIDも `index.js` に設定します。
- Script Properties に以下を設定してください。
  - `SUPABASE_URL`
  - `SUPABASE_SERVICE_ROLE_KEY`
  - `CARRIER_INVOICE_API_URL`
  - `CARRIER_INVOICE_API_SECRET`

## Carrier invoice 連携
請求確定メールを検知すると、GAS から backend の `POST /api/csv/carrier-invoice-email` に通知し、
`carrier_invoices` に `pending` ステータスで請求書ヘッダを登録します。

CSV を `upload-carrier-invoices` で取り込むと、同じ `(carrier, invoice_number)` の行が `imported` に更新されます。
