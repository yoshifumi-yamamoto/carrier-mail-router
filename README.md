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
